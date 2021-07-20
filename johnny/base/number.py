"""Common utilities for parsing numbers from brokers."""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import re
import decimal
from decimal import Decimal


# Fractions of the fraction. These are rendered as decimal but stand for binary
# fractions. /ZN has {0, 5} for 1/2 subfractions, /ZF has {0, 2, 5, 7} for 1/4
# subfractions, /ZT has {0, 1, 2, 3, 5, 6, 7, 8} for 1/8 subfractions.
_SUB_FRACTIONS = {
    '0': Decimal(0),
    '1': Decimal(1)/Decimal(8),
    '2': Decimal(2)/Decimal(8),
    '3': Decimal(3)/Decimal(8),
    '4': Decimal(3+4)/Decimal(8+8), # Approximation, for calculations.
    '5': Decimal(4)/Decimal(8),
    '6': Decimal(5)/Decimal(8),
    '7': Decimal(6)/Decimal(8),
    '8': Decimal(7)/Decimal(8),
    '9': Decimal(7+8)/Decimal(8+8), # Approximation, for calculations.
}


def ToDecimal(string: str) -> Decimal:
    """Convert number to Decimal. This also decimalizes bond fractions to decimals"""

    # Ignore empty or N/A values.
    ostring = string
    if string in {"", "--"} or string.startswith("N/A"):
        return Decimal(0)

    # Remove dollar signs and parentheses.
    string = string.replace('$', '').replace(',', '')

    # Parse parentheses as a negative number.
    sign = 1
    match = re.match(r"\((.*)\)", string)
    if match:
        sign = -1
        string = match.group(1)

    # 64'ths.
    match = re.match(r"([-+]?\d+)(?:\"|'')(\d+)", string)
    if match:
        integral, s64th = match.groups()
        if len(s64th) > 3:
            raise ValueError("Invalid 64ths price: '{}'".format(ostring))
        frac64ths = Decimal(s64th[:2])
        if len(s64th) == 3:
            try:
                frac64ths += _SUB_FRACTIONS[s64th[2]]
            except KeyError:
                raise ValueError("Invalid subfraction in '{}'".format(ostring))
        return sign * Decimal(integral) + frac64ths / Decimal(64)

    # 32'ths.
    match = re.match(r"([-+]?\d+)'(\d+)", string)
    if match:
        integral, s32th = match.groups()
        if len(s32th) > 3:
            raise ValueError("Invalid 32ths price: '{}'".format(ostring))
        frac32ths = Decimal(s32th[:2])
        if len(s32th) == 3:
            try:
                frac32ths += _SUB_FRACTIONS[s32th[2]]
            except KeyError:
                raise ValueError("Invalid subfraction in '{}'".format(ostring))
        return sign * Decimal(integral) + frac32ths / Decimal(32)

    # Decimal.
    try:
        return sign * Decimal(string or 0)
    except decimal.InvalidOperation as exc:
        raise ValueError(f"Invalid operation: Could not parse '{string}'")
