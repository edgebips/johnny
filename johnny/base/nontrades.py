"""Code associated with the schema for non-trades."""

__copyright__ = "Copyright (C) 2022  Martin Blais"
__license__ = "GNU GPLv2"

from decimal import Decimal
import datetime

from johnny.base.etl import Record, Table


# Transaction table field names.
FIELDS = [
    "rowtype",
    "account",
    "transaction_id",
    "datetime",
    "description",
    "symbol",
    "type",
    "ref",
    "amount",
    "balance",
]


# Valid row types.
ROW_TYPES = {
    "CashBalance",
    "FuturesBalance",
    "Adjustment",
    "FuturesMTM",
    "BalanceInterest",
    "MarginInterest",
    "Dividend",
    "Distribution",
    "TransferIn",
    "TransferOut",
    "TransferInternal",
    "MonthlyFee",
    "TransferFee",
    "HTBFee",
    "Sweep",
}


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
    assert r.rowtype in ROW_TYPES
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
