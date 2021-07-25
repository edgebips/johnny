"""Common code to process and validate transactions logs."""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from decimal import Decimal
from typing import Callable, Tuple
import datetime
import functools

from johnny.base.etl import Record, Table


GetFn = Callable[[str], Tuple[Table, Table]]
ParserFn = Callable[[str], Table]
ZERO = Decimal(0)


# Transaction table field names.
FIELDS = [
    # Event info
    'account', 'transaction_id', 'datetime', 'rowtype', 'order_id',

    # Instrument info
    'symbol',

    # Balance info
    'effect', 'instruction', 'quantity', 'price', 'cost', 'commissions', 'fees',

    # Descriptive info
    'description',
]


class ValidationError(Exception):
    """Conformance for transactions table. Check your importer."""


def IsZoneAware(d: datetime.datetime) -> bool:
    """Return true if the time is timezone aware."""
    return (d.tzinfo is not None and
            d.tzinfo.utcoffset(d) is not None)


# Valid row types.
ROW_TYPES = {'Trade', 'Expire', 'Open', 'Mark'}

# Valid effect types. The empty string is used to indicate "unknown".
EFFECT = {'OPENING', 'CLOSING', ''}

# Valid instructions.
INSTRUCTION = {'BUY', 'SELL'}


def ValidateFieldNames(table: Table):
    """Validate the field names and their order."""
    if list(table.header())[:len(FIELDS)] != FIELDS:
        raise ValidationError("Invalid field names on table:\n{}".format(table))


def ValidateTransactionRecord(r: Record):
    """Validate the transactions log from a source for datatypes and conformance.
    See `transactions.md` file for details on the specification and expectations
    from the converters."""

    assert r.account and isinstance(r.account, str)

    assert r.transaction_id and isinstance(r.transaction_id, str)
    assert r.order_id and isinstance(r.order_id, str)
    assert r.transaction_id != r.order_id

    assert isinstance(r.datetime, datetime.datetime)
    assert not IsZoneAware(r.datetime)
    assert r.rowtype in ROW_TYPES
    assert r.effect in EFFECT or r.effect == '', r
    assert r.instruction in INSTRUCTION or r.instruction == '', r

    # Check the normalized symbol.
    assert r.symbol and isinstance(r.symbol, str)
    # TODO(blais): Parse the symbol to ensure it's right.
    ## assert instrument.Parse(r.symbol)

    # A quantity of 'None' is allowed if the logs don't include the expiration
    # quantity, and is filled in automatically by matching code.
    assert r.quantity is None or (isinstance(r.quantity, Decimal) and r.quantity >= ZERO)
    assert isinstance(r.price, Decimal)
    assert isinstance(r.cost, Decimal)
    assert isinstance(r.commissions, Decimal)
    assert isinstance(r.fees, Decimal)

    assert isinstance(r.description, str)
