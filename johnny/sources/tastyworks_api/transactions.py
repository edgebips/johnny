"""Tastyworks - Convert local database of updated transactions from the API.

You can use tastyworks-update to maintain a local database of unprocessed
transactions. This program can then read that database and convert it to our
desired normalized format.
"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import collections
import shelve
import decimal
from decimal import Decimal
from os import path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union
import datetime
import hashlib
import logging
import pprint
import re
import os

import click
from dateutil import parser
import pytz
import tzlocal

from johnny.base import config as configlib
from johnny.base import match
from johnny.base import transactions as txnlib
from johnny.base.etl import petl, Table, Record, WrapRecords
from johnny.base.number import ToDecimal
from johnny.sources.tastyworks_csv import symbols


ZERO = Decimal(0)
ONE = Decimal(1)
Q2 = Decimal('0.01')
Json = Union[Dict[str, 'Json'], List['Json'], str, int, float]


# Numerical fields appear as strings, and we convert them to Decimal instances.
NUMERICAL_FIELDS = ['clearing-fees',
                    'proprietary-index-option-fees',
                    'regulatory-fees',
                    'commission',
                    'value',
                    'net-value']


def PreprocessTransactions(items: Iterator[Tuple[str, Json]]) -> Iterator[Json]:
    """Proprocess the list of transactions for numerical values."""

    # This is a little easier than processing using petl.
    for key, txn in items:
        if key.startswith('__'):
            continue  # Skip special utility keys, like __latest__.

        for field in NUMERICAL_FIELDS:
            if field in txn:
                value = txn[field]
                effect = txn.pop(f'{field}-effect')
                if effect == 'None':
                    assert value == '0.0'
                sign = -1 if effect == 'Debit' else +1
                txn[field] = Decimal(value) * sign
            else:
                txn[field] = ZERO
        yield txn


# A list of (transaction-type, transaction-sub-type) to process.
# The other types are ignored.
ALLOW_TYPES = {
    ('Trade', 'Buy'): 'Trade',
    ('Trade', 'Buy to Close'): 'Trade',
    ('Trade', 'Buy to Open'): 'Trade',
    ('Trade', 'Sell'): 'Trade',
    ('Trade', 'Sell to Close'): 'Trade',
    ('Trade', 'Sell to Open'): 'Trade',
    ('Receive Deliver', 'Expiration'): 'Expire',
    ('Receive Deliver', 'Symbol Change'): 'Trade',
    ('Receive Deliver', 'Symbol Change'): 'Trade',
    ('Receive Deliver', 'ACAT'): 'Trade',
    ('Receive Deliver', 'ACAT'): 'Trade',
    ('Receive Deliver', 'Assignment'): 'Assign',
    ('Receive Deliver', 'Cash Settled Assignment'): 'Assign',
    ('Receive Deliver', 'Cash Settled Exercise'): 'Trade',
    ('Receive Deliver', 'Exercise'): 'Trade',
    ('Receive Deliver', 'Forward Split'): 'Trade',
}

OTHER_TYPES = {
    ('Money Movement', 'Balance Adjustment'),
    ('Money Movement', 'Credit Interest'),
    ('Money Movement', 'Mark to Market'),
    ('Money Movement', 'Transfer'),
    ('Money Movement', 'Withdrawal'),
    ('Money Movement', 'Deposit'),
    ('Money Movement', 'Dividend'),
    ('Money Movement', 'Fee'),
}

def GetRowType(rec: Record) -> bool:
    """Predicate to filter out row types we're not interested in."""
    typekey = (rec['transaction-type'], rec['transaction-sub-type'])
    assert typekey in ALLOW_TYPES or typekey in OTHER_TYPES, (
        rec)
    return ALLOW_TYPES.get(typekey, None)


def MapAccountNumber(number: str) -> str:
    """Map the account number to the configured value."""
    return f'x{number[-4:]}' # TODO(blais): Implement the translation to
                             # nickname from the configuration.


LOCAL_ZONE = tzlocal.get_localzone()

def ParseTime(row: Record):
    """Parse datetime and convert to local time."""
    utctime = parser.parse(row['executed-at']).replace(microsecond=0)
    localtime = utctime.astimezone(LOCAL_ZONE)
    #assert utctime == localtime, (utctime, localtime)
    return localtime.replace(tzinfo=None)


def GetPosEffect(rec: Record) -> Optional[str]:
    """Get position effect."""
    if rec.rowtype == 'Expire':
        return 'CLOSING'
    action = rec['action']
    if action.endswith('to Open'):
        return 'OPENING'
    elif action.endswith('to Close'):
        return 'CLOSING'
    else:
        return ''


def GetInstruction(rec: Record) -> Optional[str]:
    """Get instruction."""
    action = rec['action']
    if action is None:
        return ''
    elif action.startswith('Buy'):
        return 'BUY'
    elif action.startswith('Sell'):
        return 'SELL'
    elif rec.rowtype == 'Expire':
        # The signs aren't set. We're going to use this value temporarily, and
        # once the stream is done, we compute and map the signs {e80fcd889943}.
        return ''
    else:
        raise NotImplementedError("Unknown instruction: '{}'".format(rec.Action))


def ConvertSafeInteger(value_str: str) -> int:
    """Convert an integer safely, ensuring no truncation of fraction occurred."""
    value_str = re.sub(r'\.0$', '', value_str)
    value = Decimal(value_str)
    int_value = int(value)
    assert value == int_value
    return value  # int_value ?


def CalculateFees(rec: Record) -> Decimal:
    """Add up the fees."""
    return (rec['clearing-fees'] +
            rec['proprietary-index-option-fees'] +
            rec['regulatory-fees'])


def CalculateCost(rec: Record) -> Decimal:
    """Calculate the raw cost."""
    derived_net_value = rec['value'] + rec['commissions'] + rec['fees']
    assert rec['net-value'] == derived_net_value, (
        rec['net-value'], derived_net_value)

    sign = +1 if 'BUY' else -1
    return sign * rec['value']


def CalculatePrice(value: str, rec: Record) -> Decimal:
    """Clean up prices and calculate them where missing."""

    # TODO(blais): Contemplate adding a new type.
    if rec.description.startswith('Forward split'):
        assert value is None
        return abs(rec.cost / rec.quantity)
    if value is None:
        return ZERO
    return Decimal(value)


def GetTransactions(filename: str) -> Tuple[Table, Table]:
    """Open a local database of Tastyworks API transactions and normalize it."""

    # Convert numerical fields to decimals.
    db = shelve.open(filename, 'r')
    items = PreprocessTransactions(db.items())

    table = (petl.fromdicts(items)

             # Add row type and filter out the row types we're not interested
             # in.
             .addfield('rowtype', GetRowType)
             .select(lambda r: r.rowtype is not None)

             # Map account number.
             .convert('account-number', MapAccountNumber)
             .rename('account-number', 'account')

             # Rename transaction and convert to string.
             .convert('id', str)
             .rename('id', 'transaction_id')

             # Parse datetime and convert to local time.
             .addfield('datetime', ParseTime)
             .cutout('executed-at')

             # Reuse the original order ids.
             .rename('order-id', 'order_id')
             .convert('order_id', str)

             # Parse the symbol.
             .rename('symbol', 'symbol-orig')
             .addfield('symbol', lambda r: str(symbols.ParseSymbol(r['symbol-orig'],
                                                                   r['instrument-type'])))

             # Split 'action' field.
             .addfield('effect', GetPosEffect)
             .addfield('instruction', GetInstruction)

             # Safely convert quantity field to a numerical value.
             .convert('quantity', ConvertSafeInteger)

             # Rename commissions.
             .rename('commission', 'commissions')

             # Compute total fees.
             .addfield('fees', CalculateFees)

             # Compute cost and verify the totals.
             .addfield('cost', CalculateCost)

             # Convert price to decimal.
             .convert('price', CalculatePrice, pass_row=True)

             #.cut(txnlib.FIELDS) TODO(blais): Restore this.
             .cut('account',
                  'transaction_id',
                  'datetime',
                  'rowtype',
                  'order_id',
                  'symbol',
                  'effect',
                  'instruction',
                  'quantity',
                  'price',
                  'cost',
                  'commissions',
                  'fees',
                  'description')

             .sort(('account', 'datetime', 'description', 'quantity'))
             )

    return table


def Import(source: str, config: configlib.Config) -> Table:
    """Process the filename, normalize, and output as a table."""
    return GetTransactions(source)


@click.command()
@click.argument('database', type=click.Path(resolve_path=True, exists=True))
def main(database: str):
    """Normalizer for database of unprocessed transactions to normalized form."""
    table = GetTransactions(database)
    print(table.lookallstr())


if __name__ == '__main__':
    main()
