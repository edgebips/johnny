"""Code associated with the schema for non-trades."""

__copyright__ = "Copyright (C) 2022  Martin Blais"
__license__ = "GNU GPLv2"

from decimal import Decimal
import datetime
import enum
import collections

from johnny.base.etl import Record, Table
from johnny.base.nontrades_pb2 import NonTrade
from johnny.base import checks


Type = enum.StrEnum("Type", {k: k for k in NonTrade.RowType.keys()})


# Transaction table field names.
FIELDS = [
    "account",
    "rowtype",
    "nativetype",
    "transaction_id",
    "ref",
    "datetime",
    "description",
    "symbol",
    "amount",
    "balance",
]


class ValidationError(Exception):
    """Conformance for transactions table. Check your importer."""


def ValidateFieldNames(table: Table):
    """Validate the field names and their order."""
    if list(table.header())[: len(FIELDS)] != FIELDS:
        raise ValidationError("Invalid field names on table:\n{}".format(table))


def ValidateRecord(r: Record):
    checks.AssertEnum(NonTrade.RowType, r.rowtype)
    checks.AssertString(r.account)
    assert (
        (r.rowtype in {Type.Balance, Type.FuturesBalance})
        or checks.CheckString(r.transaction_id)
    ), r
    checks.AssertDateTime(r.datetime)
    checks.AssertString(r.description)
    checks.AssertOptionalString(r.symbol)
    checks.AssertOptionalString(r.ref)
    checks.AssertDecimal(r.amount)
    checks.AssertOptionalDecimal(r.balance)


# TODO(blais): Not used anymore. Keep in case we want to reinstate.
def CheckUnique(table: Table, field: str):
    mapping = collections.defaultdict(int)
    for rec in table.records():
        mapping[getattr(rec, field)] += 1
    mapping = {key: value for key, value in mapping.items() if value != 1}
    assert not mapping


def Validate(nontrades: Table):
    """Validate the table of non-tradeds."""
    for rec in nontrades.records():
        ValidateRecord(rec)


def ToParquet(nontrades: Table, filename: str):
    """Write a non-trades table to Parquet."""
    # We don't have a proper schema for non-trades. TODO: Define one nicely.
    # For now, use automated conversion from Pandas.
    nontrades.todataframe().to_parquet(filename, index=False)
