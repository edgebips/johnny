"""Tastyworks - Parse transactions history CSV file.

Click on "History" >> "Transactions" >> [period] >> [CSV]

This produces a standardized transactions history log and a separate
non-transaction log.
"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import collections
import decimal
from decimal import Decimal
from os import path
from typing import Any, Dict, List, Optional, Tuple
import datetime
import hashlib
import logging
import pprint
import re
import os

import click
from dateutil import parser

from johnny.base import discovery
from johnny.base import match
from johnny.base import transactions as txnlib
from johnny.base.etl import petl, Table, Record, WrapRecords
from johnny.base.number import ToDecimal
from johnny.sources.tastyworks_csv import symbols


ZERO = Decimal(0)
ONE = Decimal(1)


def GetSequence(prv: Record, cur: Record, nxt: Record) -> int:
    """Make up a unique sequence id for orders."""
    return (prv.sequence + 1
            if prv is not None and prv.order_id == cur.order_id
            else 1)

def GetTransactionId(rec: Record) -> int:
    if rec.order_id is None:
        h = hashlib.blake2s(digest_size=6)
        # Note: For FutureOption we need the symbol name because they don't
        # insert the underlying's description in there.
        h.update(rec['Date'].encode('ascii'))
        h.update(rec['Symbol'].encode('ascii'))
        h.update(rec['Description'].encode('ascii'))
        return "^{}".format(h.hexdigest())
    else:
        assert rec.sequence
        return '^{}.{}'.format(rec.order_id, rec.sequence)


_ROW_TYPES = {
    'Trade': 'Trade',
     'Receive Deliver': 'Expire',
}

def GetRowType(rowtype: str, rec: Record) -> str:
    """Validate the row type."""
    if rowtype == 'Trade':
        return 'Trade'
    elif rowtype == 'Receive Deliver':
        if re.match(r"Removal of .* due to (expiration|exercise|assignment)",
                    rec.Description):
            return 'Expire'
        elif re.match(r"(Buy|Sell) to", rec.Description):
            return 'Trade'
        elif re.match(r"Symbol change", rec.Description):
            return 'Trade'
    return KeyError("Invalid rowtype '{}'; description: '{}'".format(
        rowtype, rec.Description))


def GetOrderId(value: str, rec: Record) -> str:
    """Get the order id, or create one where necessary."""
    # If the row is a name change, we convert that to a trade that links
    # together the in and out legs with a uniquely generated order id. The time
    # appears to be unique, and we use that as a hash for the id.
    if re.match(r"Symbol change", rec.Description):
        h = hashlib.blake2s(digest_size=3)
        h.update(rec['Date'].encode('ascii'))
        return "nam{}".format(h.hexdigest())
    else:
        # Normal case.
        return value or None


def GetPrice(rec: Record) -> Decimal:
    """Get the per-contract price."""
    if rec.rowtype == 'Expire':
        return ZERO
    match = re.search(r"@ ([0-9.]+)$", rec.Description)
    if match:
        return Decimal(match.group(1))
    if re.match(r"Symbol change", rec.Description):
        assert rec.instype == 'EquityOption'
        # Note: This will work for the equity option case.
        return abs(rec.Value) / (rec.Quantity * rec.Multiplier)
    raise ValueError("Could not infer price from description: {}".format(rec))


def GetMultiplier(rec: Record) -> Decimal:
    """Get the underlying contract multiplier."""

    # Use the multiplier from the instrument.
    multiplier = rec.instrument.multiplier

    # Check the multiplier for stocks (which is normally unset).
    if rec['instype'] == 'Equity':
        assert multiplier == 1

    # Sanity check: Verify that the approximate multiplier you can compute using
    # the (rounded) average price is close to the one we infer from our futures
    # library. This is a cross-check for the futures library code.
    if rec['instype'] != 'Future' and rec['Average Price'] != ZERO:
        approx_multiplier = abs(rec['Average Price']) / rec.price
        assert 0.9995 < (multiplier / approx_multiplier) < 1.0005, (
            multiplier, rec['Average Price'], rec.price)
    assert isinstance(multiplier, int)
    return multiplier


def GetExpiration(expi_str: str) -> Optional[datetime.date]:
    """Get the contract expiration date."""
    return (datetime.datetime.strptime(expi_str, "%m/%d/%y").date()
            if expi_str
            else None)


def GetStrike(rec: Record) -> Optional[Decimal]:
    """Process, clean up and validate the strike price."""
    strike = rec['Strike Price']
    if strike:
        assert rec.instrument.strike == strike, (
            rec.instrument.strike, strike)
        return strike
    return None


def GetInstruction(rec: Record) -> Optional[str]:
    """Get instruction."""
    if rec.Action.startswith('BUY'):
        return 'BUY'
    elif rec.Action.startswith('SELL'):
        return 'SELL'
    elif rec.rowtype == 'Expire':
        # The signs aren't set. We're going to use this value temporarily, and
        # once the stream is done, we compute and map the signs {e80fcd889943}.
        return 'EXPI'
    else:
        raise NotImplementedError("Unknown instruction: '{}'".format(rec.Action))


def GetPosEffect(rec: Record) -> Optional[str]:
    """Get position effect."""
    if rec.Action.endswith('TO_OPEN'):
        return 'OPENING'
    elif rec.Action.endswith('TO_CLOSE'):
        return 'CLOSING'
    elif rec.rowtype == 'Expire':
        return 'CLOSING'
    else:
        return '?'


def ParseStrikePrice(string: str) -> Decimal:
    """Parse and normalize the strike price."""
    cstring = re.sub(r"(.*)\.0$", r"\1", string)
    if not cstring:
        return Decimal(0)
    try:
        return Decimal(cstring)
    except decimal.InvalidOperation:
        raise ValueError("Could not parse: {}".format(string))


_INSTYPES = {
    'Equity'         : 'Equity',
    'Equity Option'  : 'EquityOption',
    'Future'         : 'Future',
    'Future Option'  : 'FutureOption',
    'Cryptocurrency' : 'Crypto',
}


def GetExpirationSigns(txns: Table) -> Dict[str, Decimal]:
    """Compute a mapping of (transaction-id, signed-quantity).

    The expirations from TW do not include the signs, which forces us to use
    stateful computations when processing the log. The normalized log format
    requires a valid `instruction` field which produces the sign. In this
    function, we process the log - hopefully enough of it is present - and
    compute what the instructions should be. The resulting mapping is later used
    to fix up the log. {e80fcd889943}
    """
    mapping = {}
    inventory = collections.defaultdict(Decimal)
    for rec in txns.records():
        key = (rec.account, rec.symbol)
        if rec.rowtype == 'Expire':
            # Ignore the sign and quantity here. Pick up the accumulated one.
            accumulated = inventory[key]
            assert accumulated != ZERO
            inventory[key] = ZERO
            mapping[rec.transaction_id] = -accumulated
        else:
            sign = 1 if rec.instruction == 'BUY' else -1
            inventory[key] += sign * rec.quantity
    return mapping


def FixExpirationSigns(table: Table) -> Table:
    """Find and set the expiration signs."""
    mapping = GetExpirationSigns(table)

    def FixExpirationInstruction(value: str, rec: Record) -> str:
        signed_quantity = mapping.get(rec.transaction_id, None)
        if signed_quantity is None:
            return value
        else:
            assert value == 'EXPI'
            return 'SELL' if signed_quantity < ZERO else 'BUY'

    return (table
            .convert('instruction', FixExpirationInstruction, pass_row=True))


def GetFuturesCost(rec: Record) -> Decimal:
    """Override the cost if the field is a Future instrument."""
    if rec.instype == 'Future':
        sign = -1 if rec.instruction == 'BUY' else 1
        return sign * rec.quantity * rec.multiplier * rec.price
    else:
        return rec.Value


def NormalizeTrades(table: petl.Table, account: str) -> petl.Table:
    """Prepare the table for processing."""
    table = (table
             # Convert fields to Decimal values.
             .convert(['Value',
                       # Warning: Don't use 'Average Price' for anything
                       # serious, it is a value rounded to dollar, not a precise
                       # value to the instrument's actual precision.
                       'Average Price',
                       'Quantity',
                       'Multiplier',
                       'Commissions',
                       'Fees'], ToDecimal)
             .convert('Strike Price', ParseStrikePrice)

             # Normalize the instrument type.
             .rename('Instrument Type', 'instype')
             .convert('instype', _INSTYPES.__getitem__)

             # Parse the instrument from the original row.
             .addfield('instrument', lambda r: symbols.ParseSymbol(
                 r['Symbol'], r['instype']))
             .addfield('symbol', lambda r: str(r.instrument))

             # Add underlying with the normalized futures contract month code.
             .addfield('underlying', lambda r: r.instrument.underlying)

             # Add the account id.
             .addfield('account', account)

             # Normalize the type.
             .rename('Type', 'rowtype')
             .convert('rowtype', GetRowType, pass_row=True)

             # Parse the date into datetime.
             .addfield('datetime', lambda r: parser.parse(r.Date).replace(tzinfo=None))

             # Convert the futures expiration date.
             .convert('Expiration Date', GetExpiration)
             .rename('Expiration Date', 'expiration')

             # Infer the per-contract price.
             .addfield('price', GetPrice)

             # Infer the per-contract multiplier.
             .addfield('multiplier', GetMultiplier)

             # We remove the original multiplier column because it only
             # represents the multiplier of the average price and is innacurate.
             # We want the multiplier of the quantity.
             .cutout('Multiplier')

             # Process, clean up and validate the strike price.
             .addfield('strike', GetStrike)
             .cutout('Strike Price')

             # Add expiration code.
             .addfield('expcode', lambda r: r.instrument.expcode)

             # Convert order id and create a sequenced order id that's unique
             # (for transaction ids).
             .rename('Order #', 'order_id')
             .convert('order_id', GetOrderId, pass_row=True)
             .addfieldusingcontext('sequence', GetSequence)
             .addfield('transaction_id', GetTransactionId)

             # Rename some of the columns to be passed through.
             .rename('Description', 'description')
             .rename('Call or Put', 'putcall')
             .rename('Quantity', 'quantity')
             .rename('Commissions', 'commissions')
             .rename('Fees', 'fees')

             # Split 'Action' field.
             .addfield('instruction', GetInstruction)
             .addfield('effect', GetPosEffect)

             # Set cost field to notional value for futures (for easy running
             # P/L calculations).
             .addfield('cost', GetFuturesCost)
             .cutout('Value')

             # Remove instrument we parsed early on.
             .cutout('instrument')

             # Removed remaining unnecessary columns.
             .cutout('Symbol')
             .cutout('Underlying Symbol')
             .cutout('Average Price')
             .cutout('Action')

             # See transactions.md.
             .cut(txnlib.FIELDS)
             )

    # Note: The sign of the expirations isn't provided by the input file. It
    # gets inferred here.
    return FixExpirationSigns(table
                              .sort(['datetime', 'order_id']))


def SplitTables(table: Table) -> Tuple[Table, Table]:
    """Split the table into transactions and others."""
    return table.biselect(lambda r: r.Type != 'Money Movement')


def GetAccount(filename: str) -> str:
    """Get the account id."""
    match = re.match(r'tastyworks_transactions_(.*)_'
                     r'(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2}).csv',
                     path.basename(filename))
    if not match:
        logging.error("Could not figure out the account name from the filename")
        account = None
    else:
        account = match.group(1)
    return account


def GetTransactions(filename: str) -> Tuple[Table, Table]:
    """Process the filename, normalize, and produce tables."""
    table = petl.fromcsv(filename)
    trades_table, other_table = SplitTables(table)

    # TODO(blais): Figure out Cryptocurrency awards. What are these? They are
    # recorded as 'Receive and Deliver' and not trades. Are these transfers in
    # kind? I'm not sure what to do with them yet.
    award_table, trades_table = (
        trades_table
        .biselect(lambda r: (r['Instrument Type'] == 'Cryptocurrency' and
                             re.search('Awarded', r.Description))))
    if award_table.nrows() > 0:
        logging.error("You have Cryptocurrency Awarded rows; skipping those rows:\n{}"
                      .format(award_table.lookallstr()))

    norm_trades_table = NormalizeTrades(trades_table, GetAccount(filename))
    return norm_trades_table, other_table


def MatchFile(filename: str) -> Optional[Tuple[str, str, callable]]:
    """Return true if this file is a matching transactions file."""
    _FILENAME_RE = (r"tastyworks_transactions_(.*)_"
                    r"(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2}).csv")
    match = re.match(_FILENAME_RE, path.basename(filename))
    if not match:
        return None
    account, date1, date2 = match.groups()
    return account, date2, txnlib.MakeParser(GetTransactions)


def Import(source: str) -> Table:
    """Process the filename, normalize, and output as a table."""
    filename = discovery.GetLatestFile(source)
    table, _ = GetTransactions(filename)
    return table


@click.command()
@click.argument('filename', type=click.Path(resolve_path=True, exists=True))
def main(filename: str):
    """Simple local runner for this translator."""
    trades_table, _ = GetTransactions(filename)
    print(trades_table.lookallstr())


if __name__ == '__main__':
    main()
