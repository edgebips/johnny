"""Common code to process and validate position files."""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from decimal import Decimal
from typing import Callable, Tuple
import functools

from johnny.base.etl import Record, Table


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


def ValidatePositionRecord(r: Record):
    """Validate the transactions log for datatypes and conformance.
    See `transactions.md` file for details on the specification and expectations
    from the converters."""

    assert r.account and isinstance(r.account, str)
    assert r.group is None or isinstance(r.group, str)
    # Check the normalized symbol.
    assert r.symbol and isinstance(r.symbol, str)
    # TODO(blais): Parse the symbol to ensure it's right.
    ## assert instrument.Parse(r.symbol)

    assert r.quantity is None or isinstance(r.quantity, Decimal)
    assert isinstance(r.price, Decimal)
    assert isinstance(r.mark, Decimal)
    assert isinstance(r.cost, Decimal)
    assert isinstance(r.net_liq, Decimal)
    assert isinstance(r.unit_delta, Decimal)
    assert isinstance(r.beta, Decimal)
    assert isinstance(r.index_price, Decimal)


def ToParquet(positions: Table, filename: str):
    """Write a transactions table to Parquet.

    This is used because we have to convert all the data types.
    """
    # We don't have a proper schema for positoins. TODO: Define one nicely.
    # For now, use automated conversion from Pandas.
    positions.todataframe().to_parquet(filename, index=False)
