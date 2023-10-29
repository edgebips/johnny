"""Common code to process and validate position files."""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from decimal import Decimal
from typing import Callable, Tuple
import functools

from johnny.base.etl import Record, Table
from johnny.base import checks


GetFn = Callable[[str], Tuple[Table, Table]]
ParserFn = Callable[[str], Table]


# Transaction table field names.
FIELDS = [
    "account",
    "group",
    "symbol",
    "quantity",
    "price",
    "mark",
    "cost",
    "net_liq",
    "unit_delta",
    "beta",
    "index_price",
]


class ValidationError(Exception):
    """Conformance for positions table. Check your importer."""


def ValidateFieldNames(table: Table):
    """Validate the field names and their order."""
    if list(table.header())[: len(FIELDS)] != FIELDS:
        raise ValidationError("Invalid field names on table:\n{}".format(table))


def ValidateRecord(r: Record):
    """Validate the transactions log for datatypes and conformance.
    See `transactions.md` file for details on the specification and expectations
    from the converters."""

    checks.AssertString(r.account)
    checks.AssertOptionalString(r.group)
    checks.AssertString(r.symbol)
    checks.AssertValidSymbol(r.symbol)
    checks.AssertOptionalDecimal(r.quantity)  # TODO(blais): Why optional?

    checks.AssertDecimal(r.price)
    checks.AssertDecimal(r.mark)
    checks.AssertDecimal(r.cost)
    checks.AssertDecimal(r.net_liq)
    checks.AssertDecimal(r.unit_delta)
    checks.AssertDecimal(r.beta)
    checks.AssertDecimal(r.index_price)


def Validate(positions: Table):
    """Validate the table of non-tradeds."""
    for rec in positions.records():
        ValidateRecord(rec)


def ToParquet(positions: Table, filename: str):
    """Write a positions table to Parquet."""
    # We don't have a proper schema for positions. TODO: Define one nicely.
    # For now, use automated conversion from Pandas.
    positions.todataframe().to_parquet(filename, index=False)
