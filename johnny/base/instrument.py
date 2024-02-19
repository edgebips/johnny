"""Normalized symbols."""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"


import datetime
import re
from decimal import Decimal
from typing import NamedTuple, Optional, List

from mulmat import multipliers
from johnny.base.etl import Table


# TODO(blais): Set the expiration datetime for future option instruments to the
# end of the corresponding calendar month. It's better than nothing, and you can
# use it to synthesize expirations where missing.

# TODO(blais): What about the subtype, e.g. (European) (Physical), etc.? That
# is currently lost.


# A representation of an instrument.
class Instrument(NamedTuple):
    """An instrument broken down by its component fields.
    See instrument.md for details.
    """

    # The name of the underlying instrument, stock or futures. For futures, this
    # includes the leading slash and the expiration month code (e.g., 'Z21').
    # Example '/CLZ21'. Note that the decade is included as well.
    underlying: str

    # For options, the expiration date for the options contract. For options on
    # futures, this is the expitation of the option, not of the underlying; this
    # should be compatible with the 'expcode' field.
    expiration: Optional[datetime.date] = None

    # For options on futures, the expiration code, including its calendar month,
    # e.g. this could be 'LOM21'. This excludes a leading slash. .
    expcode: Optional[str] = None

    # For options, the side is represented by the letter 'C' or 'P'.
    #
    # TODO(blais): Normalize to 'CALL' or 'PUT'
    putcall: Optional[str] = None

    # For options, the strike price.
    strike: Optional[Decimal] = None

    # The multiplier for the quantity of the instrument. Always set. Some
    # products require a fractional one unfortunately, e.g. `/BTC`, so we use a
    # decimal type.
    multiplier: Decimal = 1

    @property
    def instype(self) -> str:
        """Return the instrument type."""
        if self.underlying.startswith("/"):
            return "FutureOption" if self.putcall else "Future"
        elif self.underlying in multipliers.CBOE_MULTIPLIERS:
            return "IndexOption"
        elif self.putcall:
            return (
                "NonEquityOption"
                if self.underlying in NONEQUITY_UNDS
                else "EquityOption"
            )
        else:
            return "Collectibles" if self.underlying in COLLECTIBLES_UNDS else "Equity"

    def is_future(self) -> bool:
        return self.underlying.startswith("/")

    def is_option(self) -> bool:
        return bool(self.putcall) or self.instype.endswith("Option")

    def __str__(self):
        """Convert an instrument to a string code."""
        return ToString(self)

    @staticmethod
    def from_string(string: str) -> "Instrument":
        return FromString(string)


def FromColumns(
    underlying: str,
    expiration: Optional[datetime.date],
    expcode: Optional[str],
    putcall: Optional[str],
    strike: Optional[Decimal],
    multiplier: Decimal,
) -> Instrument:
    """Build an Instrument from column values."""

    assert not expcode or not expcode.startswith("/")

    # TODO(blais): Normalize to 'CALL' or 'PUT'
    putcall = putcall[0] if putcall else None

    # Infer the multiplier if it is not provided.
    if multiplier is None:
        match = re.match(r"(/.*)([FGHJKMNQUVXZ]2\d)", underlying)
        if match:
            _, calendar = match.groups()
        else:
            calendar = None

        if calendar is None:
            if expiration is not None:
                multiplier = multipliers.OPTION_CONTRACT_SIZE
            else:
                multiplier = 1
        else:
            multiplier = multipliers.MULTIPLIERS[underlying[:-3]]

    return Instrument(underlying, expiration, expcode, putcall, strike, multiplier)


def ParseUnderlying(symbol: str) -> str:
    """Parse only the underlying from the symbol."""
    match = re.match(r"(/?[A-Z0-9]+)(_.*)?", symbol)
    assert match
    return match.group(1)


def ParseProduct(underlying: str) -> str:
    """Return the product from an underlying."""
    match = re.fullmatch(r"(/?[A-Z0-9]+?)([FGHJKMNQUVXZ][23][0-9])", underlying)
    return match.group(1) if match else underlying


def FromString(symbol: str) -> Instrument:
    """Build an instrument object from the symbol string."""

    # Match options.
    match = re.match(r"(/?[A-Z0-9]+)_(?:(\d{6})|([A-Z0-9]+))_([CP])(.*)", symbol)
    if match:
        underlying, expi_str, expcode, putcall, strike_str = match.groups()
        expiration = (
            datetime.datetime.strptime(expi_str, "%y%m%d").date() if expi_str else None
        )
        strike = Decimal(strike_str)
    else:
        assert re.match("[A-Z]{3}_[A-Z]{3}", symbol) or ("_" not in symbol), symbol
        expiration, expcode, putcall, strike = None, None, None, None
        underlying = symbol

    return FromColumns(underlying, expiration, expcode, putcall, strike, None)


def ToString(inst: Instrument) -> str:
    """Convert an instrument to a string code."""

    instype = inst.instype
    if instype == "FutureOption":
        # Note: For options on futures, the correct expiration date isn't always
        # available (e.g. from TOS). We use it when it's available; ignore it
        # otherwise for that reason, the date is implicit in the option code.
        # Since the expiration date gets reduced and encoded into the symbol
        # name, this means that for platforms that do not provide the expiration
        # date (e.g. TOS), we may not be able to run some algorithms. One must
        # always assume that the 'expiration' is not present (in which case the
        # 'expcode' will always be).
        expiration_str = (
            inst.expiration.strftime("%y%m%d") if inst.expiration else inst.expcode
        )
        return "{}_{}_{}{}".format(
            inst.underlying, expiration_str, inst.putcall, inst.strike
        )

    elif instype == "Future":
        return inst.underlying

    elif instype in {"EquityOption", "IndexOption", "NonEquityOption"}:
        return "{}_{:%y%m%d}_{}{}".format(
            inst.underlying, inst.expiration, inst.putcall, inst.strike
        )

    elif instype in {"Equity", "Collectibles"}:
        return inst.underlying

    raise ValueError("Invalid instrument type: {}".format(instype))


def GetContractName(symbol: str) -> str:
    """Return the underlying root without the futures calendar expiration, e.g. '/CL'."""
    underlying = symbol.split("_")[0]
    if underlying.startswith("/"):
        match = re.match(r"(.*)([FGHJKMNQUVXZ]2\d)", underlying)
        assert match, string
        return match.group(1)
    else:
        return underlying


INSTATTR = {
    "instype": lambda r: r._instrument.instype,
    "underlying": lambda r: r._instrument.underlying,
    "expiration": lambda r: r._instrument.expiration,
    "expcode": lambda r: r._instrument.expcode,
    "putcall": lambda r: r._instrument.putcall,
    "strike": lambda r: r._instrument.strike,
    "multiplier": lambda r: r._instrument.multiplier,
}


def ExpandInstrument(table: Table, only: List[str]) -> Table:
    """Expand the symbol name into its component fields."""
    if only:
        for fieldname in only:
            table = table.addfield(fieldname, INSTATTR[fieldname])
    else:
        for fieldname, getter in INSTATTR.items():
            table = table.addfield(fieldname, getter)
    return table


def Expand(table: Table, fieldname: str, *only: List[str]) -> Table:
    """Expand the symbol name into its component fields."""
    return (
        table.addfield("_instrument", lambda r: FromString(getattr(r, fieldname)))
        .applyfn(ExpandInstrument, only=only)
        .cutout("_instrument")
    )


FIELDNAMES = [
    "instype",
    "underlying",
    "expiration",
    "expcode",
    "putcall",
    "strike",
    "multiplier",
]


def Shrink(table: Table, *exceptions: List[str]) -> Table:
    """Remove the component fields of the instrument."""
    if exceptions:
        fieldnames = FIELDNAMES.copy()
        for exception in exceptions:
            fieldnames.remove(exception)
    else:
        fieldnames = FIELDNAMES
    return table.cutout(*fieldnames)


# Underlyings that are treated as collectibles. Options on these are non-equity
# options. Note: But not COPX.
COLLECTIBLES_UNDS = {"GLD", "OUNZ", "SLV", "IAU", "CPER"}


# https://greentradertax.com/how-to-apply-lower-tax-rates-to-volatility-options/
# – iPath S&P 500 VIX ST Futures ETN (VXX)
# – iPath S&P 500 VIX Mid-Term Futures ETN (VXZ)
# – UBS VelocityShares 1X Daily Inverse VSTOXX Futures ETN (EXIV)
# – UBS VelocityShares VIX Variable Long/Short ETN (LSVX)
# – UBS VelocityShares VIX Tail Risk ETN (BSWN)
# – UBS VelocityShares 1X Long VSTOXX Futures ETN (EVIX)
# – iPath S&P 500 Dynamic VIX ETN (XVZ)
# – Credit Suisse VelocityShares Daily Long VIX Short-Term ETN (VIIX)
VOL_ETNS = {"VXX", "VXZ", "EXIV", "LSVX", "BSWN", "EVIX", "XVZ", "VIIX"}


# Unds for non-equity options.
NONEQUITY_UNDS = COLLECTIBLES_UNDS | VOL_ETNS
