"""Code associated with the schema for non-trades."""

__copyright__ = "Copyright (C) 2022  Martin Blais"
__license__ = "GNU GPLv2"

from decimal import Decimal
import datetime

from johnny.base.etl import Record, Table
from johnny.base.nontrades_pb2 import NonTrade


# Transaction table field names.
FIELDS = [
    "rowtype",
    "nativetype",
    "account",
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


def IsZoneAware(d: datetime.datetime) -> bool:
    """Return true if the time is timezone aware."""
    return d.tzinfo is not None and d.tzinfo.utcoffset(d) is not None


def ValidateFieldNames(table: Table):
    """Validate the field names and their order."""
    if list(table.header())[: len(FIELDS)] != FIELDS:
        raise ValidationError("Invalid field names on table:\n{}".format(table))


def ValidateRecord(r: Record):
    assert r.rowtype in NonTrade.RowType.keys()
    assert r.account and isinstance(r.account, str)
    assert r.transaction_id and isinstance(r.transaction_id, str), r
    assert isinstance(r.datetime, datetime.datetime)
    assert not IsZoneAware(r.datetime)
    assert r.description and isinstance(r.description, str)
    assert r.type is None or isinstance(r.type, str)
    assert r.symbol is None or isinstance(r.symbol, str)
    assert r.ref is None or isinstance(r.ref, str)
    assert isinstance(r.amount, Decimal)
    assert r.balance is None or isinstance(r.balance, Decimal)


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

    # Check that the transaction id is unique.
    CheckUnique(nontrades, "transaction_id")


def ToParquet(nontrades: Table, filename: str):
    """Write a non-trades table to Parquet."""
    # We don't have a proper schema for non-trades. TODO: Define one nicely.
    # For now, use automated conversion from Pandas.
    nontrades.todataframe().to_parquet(filename, index=False)
