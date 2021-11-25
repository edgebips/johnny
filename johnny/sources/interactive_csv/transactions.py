"""Interactive Brokers - Parse account statement CSV files.

  Login > Reports > Flex Report > (period) > Run

You need to select "Statement of Funds" to produce everything as a single table,
and enable all the columns, and it has to be done from a Flex query. This is the
way to do this properly; in fact, the only way I've found.

"Custom reports" from the first page are joint reports (includes many tables in
a single CSV file with a prefix column), they have an option to output the
statement of funds table, but it doesn't contain the stock detail. You want a
"Flex report", which has joins between the tables, e.g., the statement of funds
and the trades table.

Note that during the weekends you may not be able to download up to the day's
date (an error about the report/statement not being ready to download will be
shown). Simply select a Custom Date Range, and select the last valid market open
date / business date in order to produce a valid report.
"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from decimal import Decimal
from functools import partial
from itertools import chain
from os import path
from typing import Any, Dict, List, Optional, Tuple, Union, Iterable
import collections
import csv
import datetime
import hashlib
import itertools
import logging
import os
import pprint
import re
import typing

import click
from dateutil import parser

import mulmat
from mulmat import multipliers
from johnny.base import config as configlib
from johnny.base import discovery
from johnny.base import instrument
from johnny.base import inventories
from johnny.base import number
from johnny.base import transactions as txnlib
from johnny.base.etl import petl, Table, Record, WrapRecords
from johnny.sources.thinkorswim_csv import symbols
from johnny.sources.thinkorswim_csv import utils
from johnny.utils import csv_utils


Table = petl.Table
Record = petl.Record
Config = Any
ZERO = Decimal(0)




def CheckValues(values, value):
    assert value in values
    return value


def GetAssetClass(v: str) -> str:
    if v == 'STK':
        return 'Equity'
    elif v == 'OPT':
        return 'EquityOption'
    else:
        raise ValueError(f"Unknown value {v}")


def GetExpiration(v: str) -> Optional[datetime.date]:
    return (datetime.datetime.strptime(v, '%Y%m%d').date()
            if v
            else '')


def GetSymbol(rec: Record) -> instrument.Instrument:
    if rec.instype == 'Equity':
        return instrument.FromColumns(rec.Symbol,
                                      None, None, None, None, rec.multiplier)
    elif rec.instype == 'EquityOption':
        return instrument.FromColumns(rec.underlying,
                                      rec.expiration,
                                      rec.expcode,
                                      rec.putcall,
                                      rec.strike,
                                      rec.multiplier)
    else:
        raise ValueError(f"Unknown instrument type: {rec}")


def GetTransactions(filename: str) -> Tuple[Table, Table]:
    """Read and prepare all the tables to be joined."""

    table = (petl.fromcsv(filename)

             # See also:L 'Description'
             .cut('ClientAccountID', 'TransactionID', 'OrderID', 'TradeID',
                  'AssetClass', 'Symbol', 'UnderlyingSymbol', 'Multiplier', 'Strike', 'Expiry', 'Put/Call',
                  'Date', 'ActivityCode', 'ActivityDescription', 'Buy/Sell', 'TradeQuantity', 'TradePrice',
                  'TradeGross', 'TradeCommission', 'Debit', 'Credit', 'Amount', 'Balance')

             # Convert fields to data types.
             .convert('Date', lambda v: datetime.datetime.strptime(v, '%Y%m%d').date())
             .convert(['Multiplier', 'Strike', 'TradeQuantity', 'TradePrice',
                       'TradeGross', 'TradeCommission', 'Debit', 'Credit', 'Amount'],
                      lambda v: Decimal(v) if v else '')

             # Check Debit + Credit == Amount and remove.
             .addfield('Cr+Dr', lambda r: round((r.Debit or ZERO) + (r.Credit or ZERO) - r.Amount, 6))
             .cutout('Cr+Dr', 'Debit', 'Credit')

             # Check Multiplier * TradeQuantity * TradePrice == TradeGross
             .addfield('Gross', lambda r: (((r.Multiplier * r.TradeQuantity * r.TradePrice) + r.TradeGross)
                                           if r.Multiplier else ''))
             .cutout('Gross')

             #.sort(['Symbol', 'TransactionID'])
             )

    # Split table between trade and non-trade.
    trade, nontrade = table.biselect(lambda r: r.ActivityCode in {'BUY', 'SELL', 'DIV'})

    # Setup all instrument fields, derive symbol, and shrink the fields away.
    trade = (trade
             .convert('AssetClass', GetAssetClass)
             .rename('AssetClass', 'instype')

             .rename('UnderlyingSymbol', 'underlying')
             .rename('Put/Call', 'putcall')
             .rename('Strike', 'strike')
             .rename('Multiplier', 'multiplier')

             .convert('Expiry', GetExpiration)
             .rename('Expiry', 'expiration')

             # Note: we haven't traded futures in here, so don't know what their expcode looks like.
             .addfield('expcode', '')

             .addfield('symbol', GetSymbol)
             .convert('symbol', str)
             .applyfn(instrument.Shrink)
             .cutout('Symbol')
             )

    # Remove dividends, because we don't know what to do with them.
    # TODO(blais): Support dividends upstream.
    trade = (trade
             .selectne('ActivityCode', 'DIV')
             .cutout('ActivityCode'))

    # Normalize the trade table.
    trade = (trade
             .rename('ClientAccountID', 'account')
             .rename('TransactionID', 'transaction_id')
             .rename('OrderID', 'order_id')
             .cutout('TradeID')





             )

# account transaction_id datetime rowtype order_id symbol effect instruction
# quantity price cost commissions fees description



    # Check for empty cells and removed unused columns.
    nontrade = (
        nontrade

        .convert('OrderID', partial(CheckValues, {''}))
        .convert('TradeID', partial(CheckValues, {''}))
        .convert('UnderlyingSymbol', partial(CheckValues, {''}))
        .convert('Strike', partial(CheckValues, {''}))
        .convert('Multiplier', partial(CheckValues, {0, 1, ''}))
        .convert('Expiry', partial(CheckValues, {''}))
        .convert('Put/Call', partial(CheckValues, {''}))
        .convert('Buy/Sell', partial(CheckValues, {''}))
        .convert('TradeQuantity', partial(CheckValues, {0}))
        .convert('TradePrice', partial(CheckValues, {0}))
        .convert('TradeGross', partial(CheckValues, {0}))
        .convert('TradeCommission', partial(CheckValues, {0}))
        .cutout('OrderID', 'TradeID', 'AssetClass',
                'UnderlyingSymbol', 'Strike', 'Multiplier', 'Expiry', 'Put/Call',
                'Buy/Sell', 'TradeQuantity', 'TradePrice', 'TradeGross', 'TradeCommission')
        )

    print(trade.lookallstr())
    print(nontrade.lookallstr())

    return trade, nontrade


def Import(source: str, config: configlib.Config) -> Table:
    """Process the filename, normalize, and output as a table."""
    filename = discovery.GetLatestFile(source)
    table = GetTransactions(filename)
    return table


@click.command()
@click.argument('filename', type=click.Path(resolve_path=True, exists=True))
@click.option('--cash', is_flag=True, help="Print out cash transactions.")
def main(filename: str, cash):
    """Simple local runner for this translator."""
    trades_table, other_table = GetTransactions(filename)
    table = trades_table if not cash else other_table


if __name__ == '__main__':
    main()
