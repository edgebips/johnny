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
from johnny.base import transactions as txnlib
from johnny.base.config import Account
from johnny.base.etl import petl, Table, Record, WrapRecords
from johnny.base.number import ToDecimal
from johnny.sources.tastytrade import config_pb2
from johnny.sources.tastytrade import symbols
from johnny.sources.tastytrade import nontrades


ZERO = Decimal(0)
ONE = Decimal(1)
Q2 = Decimal("0.01")
Json = Union[Dict[str, "Json"], List["Json"], str, int, float]


# Numerical fields appear as strings, and we convert them to Decimal instances.
# These ones have '{name}-effect' fields providing the sign.
NUMERICAL_FIELDS = [
    "clearing-fees",
    "proprietary-index-option-fees",
    "regulatory-fees",
    "commission",
    "value",
    "net-value",
]

UNSIGNED_NUMERICAL_FIELDS = ["price"]


def PreprocessTransactions(items: Iterator[Tuple[str, Json]]) -> Iterator[Json]:
    """Proprocess the list of transactions for numerical values."""

    # This is a little easier than processing using petl.
    for key, txn in items:
        if key.startswith("__"):
            continue  # Skip special utility keys, like __latest__.

        for field in NUMERICAL_FIELDS:
            if field in txn:
                value = txn[field]
                effect = txn.pop(f"{field}-effect")
                if effect == "None":
                    assert value == "0.0"
                sign = -1 if effect == "Debit" else +1
                txn[field] = Decimal(value) * sign
            else:
                txn[field] = ZERO

        for field in UNSIGNED_NUMERICAL_FIELDS:
            if field in txn:
                value = txn[field]
                txn[field] = Decimal(value)
            else:
                txn[field] = ZERO

        yield txn


# A list of (transaction-type, transaction-sub-type) to process.
# The other types are ignored.
TRANSACTION_TYPES = {
    # Futures trades.
    ("Trade", "Buy"): txnlib.Type.Trade,
    ("Trade", "Sell"): txnlib.Type.Trade,
    # Equity trades.
    ("Trade", "Buy to Close"): txnlib.Type.Trade,
    ("Trade", "Buy to Open"): txnlib.Type.Trade,
    ("Trade", "Sell to Close"): txnlib.Type.Trade,
    ("Trade", "Sell to Open"): txnlib.Type.Trade,
    # Expirations.
    ("Receive Deliver", "Expiration"): txnlib.Type.Expire,
    # Stock actions.
    ("Receive Deliver", "Forward Split"): txnlib.Type.Trade,
    ("Receive Deliver", "Reverse Split"): txnlib.Type.Trade,
    ("Receive Deliver", "Symbol Change"): txnlib.Type.Trade,
    ("Receive Deliver", "Symbol Change"): txnlib.Type.Trade,
    # Assignment and exercise.
    ("Receive Deliver", "Assignment"): txnlib.Type.Assign,
    ("Receive Deliver", "Exercise"): txnlib.Type.Exercise,
    ("Receive Deliver", "Cash Settled Assignment"): txnlib.Type.Assign,
    ("Receive Deliver", "Cash Settled Exercise"): txnlib.Type.Exercise,
    ("Receive Deliver", "Buy to Open"): txnlib.Type.Trade,
    ("Receive Deliver", "Sell to Open"): txnlib.Type.Trade,
    ("Receive Deliver", "Buy to Close"): txnlib.Type.Trade,
    ("Receive Deliver", "Sell to Close"): txnlib.Type.Trade,
    # Transfers.
    ("Receive Deliver", "ACAT"): txnlib.Type.Trade,
    ("Receive Deliver", "ACAT"): txnlib.Type.Trade,
    # Dividends.
    ("Money Movement", "Dividend"): txnlib.Type.Dividend,
}

ALL_TYPES = {}
ALL_TYPES.update(TRANSACTION_TYPES)
ALL_TYPES.update(nontrades.NONTRADE_TYPES)


def GetRowType(rec: Record) -> bool:
    """Predicate to filter out row types we're not interested in."""
    typekey = (rec["transaction-type"], rec["transaction-sub-type"])
    return ALL_TYPES[typekey]


def MapAccountNumber(number: str) -> str:
    """Map the account number to the configured value."""
    return f"x{number[-4:]}"  # TODO(blais): Implement the translation to
    # nickname from the configuration.


LOCAL_ZONE = tzlocal.get_localzone()


def ParseTime(row: Record):
    """Parse datetime and convert to local time."""
    utctime = parser.parse(row["executed-at"]).replace(microsecond=0)
    localtime = utctime.astimezone(LOCAL_ZONE)
    # assert utctime == localtime, (utctime, localtime)
    return localtime.replace(tzinfo=None)


def GetPosEffect(rec: Record) -> Optional[str]:
    """Get position effect."""
    if rec.rowtype in {txnlib.Type.Expire, txnlib.Type.Exercise, txnlib.Type.Assign}:
        return "CLOSING"
    if rec.rowtype in {txnlib.Type.Dividend}:
        return ""
    action = rec["action"]
    if action.endswith("to Open"):
        return "OPENING"
    elif action.endswith("to Close"):
        return "CLOSING"
    else:
        return ""


def GetInstruction(rec: Record) -> Optional[str]:
    """Get instruction."""
    action = rec["action"]
    if action is None:
        return ""
    elif action.startswith("Buy"):
        return "BUY"
    elif action.startswith("Sell"):
        return "SELL"
    elif rec.rowtype == txnlib.Type.Expire:
        # The signs aren't set. We're going to use this value temporarily, and
        # once the stream is done, we compute and map the signs {e80fcd889943}.
        return ""
    else:
        raise NotImplementedError("Unknown instruction: '{}'".format(rec.Action))


def ConvertQuantity(value_str: Optional[str], rec: Record) -> Decimal:
    """Convert and round integer values to decimal and leave fractional alone.
    This is only used to trim unnecessary trailing ".0" suffixes.
    """
    if rec.rowtype == txnlib.Type.Dividend:
        return ZERO
    rounded_value_str = re.sub(r"\.0$", "", value_str)
    return Decimal(rounded_value_str)


def CalculateFees(rec: Record) -> Decimal:
    """Add up the fees."""
    return (
        rec["clearing-fees"]
        + rec["proprietary-index-option-fees"]
        + rec["regulatory-fees"]
    )


def CalculateCost(rec: Record) -> Decimal:
    """Calculate the raw cost."""
    if rec["rowtype"] == txnlib.Type.Dividend:
        return ZERO

    derived_net_value = rec["value"] + rec["commissions"] + rec["fees"]
    assert rec["net-value"] == derived_net_value, (rec["net-value"], derived_net_value)

    # We need to handle opening and closing cost on Future differently (but not
    # FutureOption types) because much of the value for those contracts is
    # transfered via mark-to-market transaction lines, but we choose to instead
    # ignore those and instead use the notional value (cash equivalent). The
    # thing is, using the mark-to-market values would require use to assign
    # little bits of daily transfers to a variable number of shares, and it's
    # not meaningful anyway. Cash equivalent notional is easier to think about,
    # and renders nicer logs.
    value = rec["value"]
    if rec["instrument-type"] == "Future":
        assert rec["action"] in {"Buy", "Sell"}
        sign = -1 if rec["action"] == "Buy" else +1
        value = sign * rec["quantity"] * rec.instrument.multiplier * rec["price"]

    return value


def CalculateCash(rec: Record) -> Decimal:
    """Calculate the cash portion, from dividends."""
    return rec["value"] if rec["rowtype"] == txnlib.Type.Dividend else ZERO


def CalculatePrice(value: str, rec: Record) -> Decimal:
    """Clean up prices and calculate them where missing."""
    if rec["transaction-sub-type"] in {"Forward Split", "Reverse Split"}:
        return abs(rec.cost / rec.quantity / rec.instrument.multiplier)
    if rec["rowtype"] == txnlib.Type.Dividend:
        return ZERO
    if value is None:
        return ZERO
    return Decimal(value)


def GetOrderId(order_id: Optional[int], rec: Record) -> str:
    """Make up a unique order id; include expirations."""
    if order_id and isinstance(order_id, int):
        return str(order_id)
    else:
        assert rec.transaction_id
        return "w{}".format(rec.transaction_id)


def GetTransactions(filename: str) -> Table:
    """Open a local database of Tastyworks API transactions and normalize it."""

    # Convert numerical fields to decimals.
    db = shelve.open(filename, "r")
    items = PreprocessTransactions(db.items())

    # Filter rows that we care about. Note that this removes mark-to-market
    # entries.
    table = (
        petl.fromdicts(items)
        # Add row type and filter out the row types we're not interested
        # in.
        .addfield("rowtype", GetRowType).selectin(
            "rowtype", set(TRANSACTION_TYPES.values())
        )
    )

    # Add missing columns (can happen if there weren't any transactions ever).
    add_cols = {"order-id", "symbol", "quantity"}
    for col in add_cols:
        if col not in table.fieldnames():
            table = table.addfield(col, None)

    table = (
        table
        # Map account number.
        .convert("account-number", MapAccountNumber)
        .rename("account-number", "account")
        # Rename transaction and convert to string.
        .convert("id", str)
        .rename("id", "transaction_id")
        # Parse datetime and convert to local time.
        .addfield("datetime", ParseTime)
        .cutout("executed-at")
        # Reuse the original order ids.
        .rename("order-id", "order_id")
        .convert("order_id", GetOrderId, pass_row=True)
        # Parse the symbol.
        .rename("symbol", "symbol-orig")
        .addfield(
            "instrument",
            lambda r: symbols.ParseSymbol(r["symbol-orig"], r["instrument-type"]),
        )
        .addfield("symbol", lambda r: str(r.instrument))
        # Split 'action' field.
        .addfield("effect", GetPosEffect)
        .addfield("instruction", GetInstruction)
        # Safely convert quantity field to a numerical value.
        .convert("quantity", ConvertQuantity, pass_row=True)
        # Rename commissions.
        .rename("commission", "commissions")
        # Compute total fees.
        .addfield("fees", CalculateFees)
        # Compute cost and verify the totals.
        .addfield("cost", CalculateCost)
        # Compute dividends and other cash.
        .addfield("cash", CalculateCash)
        # Convert price to decimal.
        .convert("price", CalculatePrice, pass_row=True)
        # .cut(txnlib.FIELDS) TODO(blais): Restore this.
        .cut(
            "account",
            "transaction_id",
            "datetime",
            "rowtype",
            "order_id",
            "symbol",
            "effect",
            "instruction",
            "quantity",
            "price",
            "cost",
            "cash",
            "commissions",
            "fees",
            "description",
        )
        .sort(("account", "datetime", "description", "quantity"))
        .addfield("init", None)
    )

    return table


def ImportTransactions(config: config_pb2.Config) -> petl.Table:
    return GetTransactions(path.expandvars(config.dbm_filename))


@click.command()
@click.argument("database", type=click.Path(resolve_path=True, exists=True))
def main(database: str):
    """Normalizer for database of unprocessed transactions to normalized form."""

    if 1:
        transactions = Import(database, None, Account.TRANSACTIONS)
        transactions = GetTransactions(database)
        # print(transactions.head(10).lookallstr())
        transactions.selecteq("rowtype", txnlib.Type.Dividend).tocsv()

    if 0:
        nontrades = Import(database, None, Account.OTHER)
        # print(nontrades.lookallstr())
        for rec in nontrades.aggregate(
            ["transaction-type", "transaction-sub-type"], WrapRecords
        ).records():
            print(rec.value.lookallstr())


if __name__ == "__main__":
    main()
