"""Checks and assertions for field types.

We're mainly defining this as a result of using petl which, while being very
flexible, requires us to properly assert types in order to get some consistency.
"""

from decimal import Decimal
import datetime as dt

from johnny.base import instrument


ZERO = Decimal(0)


def CheckDecimal(value) -> bool:
    return isinstance(value, Decimal)


def AssertDecimal(value):
    assert CheckDecimal(value), f"Invalid decimal: {value!r}"


def CheckOptionalDecimal(value) -> bool:
    return value is None or isinstance(value, Decimal)


def AssertOptionalDecimal(value):
    assert CheckOptionalDecimal(value), f"Invalid optional decimal: {value!r}"


def CheckOptionalPositiveDecimal(value) -> bool:
    return value is None or (isinstance(value, Decimal) and value >= ZERO)


def AssertOptionalPositiveDecimal(value):
    assert CheckOptionalDecimal(value), f"Invalid optional positive decimal: {value!r}"


def CheckDateTime(value) -> bool:
    return isinstance(value, dt.datetime) and value.tzinfo is None


def AssertDateTime(value):
    assert CheckDateTime(value), f"Invalid datetime: {value}"


def CheckEnum(proto_enum, value) -> bool:
    return value in proto_enum.keys()


def AssertEnum(proto_enum, value):
    assert CheckEnum(proto_enum, value), f"Invalid required enum: {value}"


def CheckOptionalEnum(proto_enum, value) -> bool:
    # TODO(blais): Turn this to None
    return value == "" or value in proto_enum.keys()


def AssertOptionalEnum(proto_enum, value):
    assert CheckOptionalEnum(proto_enum, value), f"Invalid optional enum: {value}"


def CheckString(value) -> bool:
    return isinstance(value, str) and value


def AssertString(value):
    assert CheckString(value), f"Invalid required string: {value!r}"


def CheckOptionalString(value) -> bool:
    return value is None or (isinstance(value, str) and value)


def AssertOptionalString(value):
    assert CheckOptionalString(value), f"Invalid optional string: {value!r}"


def AssertValidSymbol(value):
    # Note: We don't provide a corresponding check because this may raise an
    # exception on failure.
    assert instrument.FromString(value), f"Invalid symbol: {value!r}"
