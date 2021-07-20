"""Tastyworks - Parse positions CSV file.
"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import collections
from decimal import Decimal
from os import path
from typing import Any, Optional, Tuple, Mapping
import datetime
import hashlib
import logging
import pprint
import re
import os
import decimal

import click
from dateutil import parser

from johnny.base import config as configlib
from johnny.base import discovery
from johnny.base import positions as poslib
from johnny.base.etl import petl, Table, Record, WrapRecords
from johnny.base.number import ToDecimal
from johnny.sources.tastyworks_csv import symbols


_INSTYPES = {
    'STOCK': 'Equity',
    'EQUITY': 'Equity',
    'OPTION': 'EquityOption',
    'FUTURES': 'Future',
    'FUTURES_OPTION': 'FutureOption',
}


def NormalizeAccountName(account: str) -> str:
    """Normalize to match that from the transactions log."""
    return "x{}".format(account[-4:]) if len(account) == 8 else account


def ConvertPoP(pop_str: str) -> Decimal:
    """Convert POP to an integer."""
    try:
        if pop_str == '< 1%':
            return Decimal(1)
        elif pop_str == '> 99.5%':
            return Decimal(99.5)
        elif pop_str == '--':
            return Decimal(0)
        else:
            return Decimal(pop_str.rstrip('%'))
    except Exception as exc:
        raise ValueError("Decimal error: {} on '{}'".format(exc, pop_str))


def GetPositions(filename: str) -> Table:
    """Process the filename, normalize, and produce tables."""
    table = petl.fromcsv(filename)
    table = (table

             # Clean up account name to match that from the transactions log.
             .convert('Account', NormalizeAccountName)
             .rename('Account', 'account')

             # Make instrument type match that from the transactiosn log.
             .convert('Type', _INSTYPES.__getitem__)
             .rename('Type', 'instype')

             # Parse symbol and add instrument fields.
             .addfield('instrument', lambda r: symbols.ParseSymbol(
                 r['Symbol'], r['instype']))
             .cutout('Symbol')
             .addfield('symbol', lambda r: str(r.instrument), index=2)
             # TODO(blais): Cross-check these fields against the symbol, just to be sure.
             .cutout('Exp Date', 'DTE', 'Strike Price', 'Call/Put')
             .cutout('instrument')

             # Convert fields to Decimal values.
             .convert(['Trade Price',
                       'Cost',
                       'Mark',
                       'Net Liq',
                       'P/L Open',
                       'P/L Day',
                       'β Delta',
                       '/ Delta',
                       'Delta',
                       'Theta',
                       'Vega',
                       'IV Rank'], ToDecimal)

             # Convert POP to a fraction.
             .convert('PoP', ConvertPoP)

             # Rename some fields for normalization.
             .rename('Quantity', 'quantity')
             .convert('quantity', ToDecimal)
             .rename('Trade Price', 'price')
             .rename('Cost', 'cost')
             .rename('Mark', 'mark')
             .rename('Net Liq', 'net_liq')
             .rename('P/L Open', 'pnl_open')
             .rename('P/L Day', 'pnl_day')

             # Add a group field, though there aren't any groupings yet.
             .addfield('group', None)

             #.addfield('DELTA_DIFFS', lambda r: r['Delta'] / r['/ Delta'] if r['/ Delta'] else '')
             # 'instype'
             .cut('account', 'group', 'symbol',
                  'quantity', 'price', 'mark',
                  'cost', 'net_liq',
                  'pnl_open', 'pnl_day')
             )

    return table


def Import(source: str, config: configlib.Config) -> Table:
    """Process the filename, normalize, and output as a table."""
    filename = discovery.GetLatestFile(source)
    return GetPositions(filename)


def ReadPricesFromPositionsFile(filename: str) -> Mapping[str, Decimal]:
    """Read the CSV file, normalize the symbols and return the prices."""
    if not filename or not path.exists(filename):
        return {}
    return (petl.fromcsv(filename)
            .convert('Type', _INSTYPES.__getitem__)
            .rename('Type', 'instype')

            .addfield('symbol', lambda r: str(symbols.ParseSymbol(
                r['Symbol'], r['instype'])))

            .rename('Mark', 'mark')
            .convert('mark', ToDecimal)

            .cut('symbol', 'mark')
            .lookupone('symbol', 'mark'))


@click.command()
@click.argument('filename', type=click.Path(resolve_path=True, exists=True))
def main(filename: str):
    """Simple local runner for this translator."""
    print(GetPositions(filename).lookallstr())


if __name__ == '__main__':
    main()
