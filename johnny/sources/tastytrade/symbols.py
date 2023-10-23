"""Parsing symbols from Tastyworks to a common normalized symbology called `BeanSyms`."""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import datetime
import re
from typing import Optional
from decimal import Decimal

from mulmat import multipliers
from johnny.base import instrument

Instrument = instrument.Instrument


def ParseSymbol(symbol: str, instype: Optional[str] = None) -> Instrument:
    """Parse a symbol from the Tastyworks platforms."""
    # Note: The instrument type is a normalized one, not the original one.
    if not symbol:
        return None

    # Futures options always start with a period.
    inst = None
    if (
        instype in {"FutureOption", "Future Option"}
        or instype is None
        and symbol.startswith("./")
    ):
        inst = _ParseFuturesOptionSymbol(symbol)
    # Futures always start with a slash.
    elif instype == "Future" or instype is None and symbol.startswith("/"):
        inst = _ParseFuturesSymbol(symbol)
    # Then we have options, with a space.
    elif (
        instype in {"EquityOption", "Equity Option"}
        or instype is None
        and " " in symbol
    ):
        inst = _ParseEquityOptionSymbol(symbol)
    # And finally, just equities.
    elif instype == "Equity" or instype is None:
        inst = _ParseEquitySymbol(symbol)
    elif instype in {"Crypto", "Cryptocurrency"}:
        inst = _ParseCrypto(symbol)
    else:
        raise ValueError(f"Unknown instrument type for {symbol}")
    return inst


# Divisor for TW strike prices.
_STRIKE_PRICE_DIVISOR = Decimal("1000")


def _ParseEquitySymbol(symbol: str) -> Instrument:
    return Instrument(underlying=symbol, multiplier=Decimal(1))


def _ParseEquityOptionSymbol(symbol: str) -> Instrument:
    # e.g. 'TLRY  210416C00075000' for equity option;
    return Instrument(
        underlying=symbol[0:6].rstrip(),
        expiration=datetime.date(
            int(symbol[6:8]), int(symbol[8:10]), int(symbol[10:12])
        ),
        putcall=symbol[12],
        strike=_ParseStrikeAmount(symbol[13:21]),
        multiplier=multipliers.OPTION_CONTRACT_SIZE,
    )


def _ParseStrikeAmount(string: str) -> Decimal:
    """Parse a thousand multiplicand of a strike price."""
    value = Decimal(string[:-3])
    fraction = string[-3:].rstrip("0")
    if fraction:
        fraction = Decimal("0." + fraction)
        value += fraction
    return Decimal(value)


_FUTSYM = "([A-Z0-9]+)([FGHJKMNQUVXZ])2?([0-9])"
_DECADE = datetime.date.today().year % 100 // 10


def _ParseFuturesSymbol(symbol: str) -> Instrument:
    match = re.fullmatch(f"/{_FUTSYM}", symbol)
    assert match, "Invalid futures options symbol: {}".format(symbol)
    root, fmonth, fyear = match.groups()
    underlying = f"/{root}{fmonth}{_DECADE}{fyear}"
    multiplier = multipliers.MULTIPLIERS[underlying[:-3]]
    return Instrument(underlying=underlying, multiplier=multiplier)


def _ParseFuturesOptionSymbol(symbol: str) -> Instrument:
    # e.g., "./6JM1 JPUK1 210507P0.009" for futures option.
    assert symbol.startswith(r"./"), "Invalid futures options symbol: {}".format(symbol)
    underlying = symbol[1:7].rstrip()
    contractsym = symbol[7:12].rstrip()
    assert symbol[12] == " "

    # Parse the underlying futures contract.
    inst = _ParseFuturesSymbol(underlying)

    # Parse the corresponding options contract.
    match = re.match(f"{_FUTSYM}", contractsym)
    assert match, "Invalid futures options symbol: {}".format(symbol)
    root, optfmonth, optfyear = match.groups()
    expcode = f"{root}{optfmonth}{_DECADE}{optfyear}"

    # Parse the option itself.
    match = re.match(r"(\d{6})([CP])([0-9.]+)", symbol[13:])
    if not match:
        raise ValueError("Could not match future option: {}".format(symbol))

    expistr = match.group(1)
    expiration = datetime.date(
        2000 + int(expistr[0:2]), int(expistr[2:4]), int(expistr[4:6])
    )
    putcall = match.group(2)
    strike = Decimal(match.group(3))

    return inst._replace(
        expcode=expcode, expiration=expiration, putcall=putcall, strike=strike
    )


def _ParseCrypto(symbol: str) -> Instrument:
    match = re.match("([A-Z]{3})/([A-Z]{3})", symbol)
    assert match
    base, quote = match.groups()
    return Instrument(underlying="{}_{}".format(base, quote), multiplier=Decimal(1))
