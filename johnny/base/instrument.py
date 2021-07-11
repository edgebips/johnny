"""Normalized symbols."""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"


import datetime
import re
from decimal import Decimal
from typing import List, NamedTuple, Optional

from johnny.base import futures
from johnny.base.etl import Table


# TODO(blais): Set the expiration datetime for future option instruments to the
# end of the corresponding calendar month. It's better than nothing, and you can
# use it to synthesize expirations where missing.

# TODO(blais): What about the subtype, e.g. (European) (Physical), etc.? That
# is currently lost.


# A representation of an option.
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

    # The multiplier for the quantity of the instrument. Always set.
    multiplier: int = 1


    @property
    def instype(self) -> str:
        """Return the instrument type."""
        if self.underlying.startswith('/'):
            return 'FutureOption' if self.putcall else 'Future'
        else:
            return 'EquityOption' if self.putcall else 'Equity'

    def is_future(self) -> bool:
        return self.underlying.startswith('/')

    def is_option(self) -> bool:
        return bool(self.putcall)

    def __str__(self):
        """Convert an instrument to a string code."""
        return ToString(self)

    @staticmethod
    def from_string(self, string: str) -> 'Instrument':
        return FromString(string)


def FromColumns(underlying: str,
                expiration: Optional[datetime.date],
                expcode: Optional[str],
                putcall: Optional[str],
                strike: Optional[Decimal],
                multiplier: int) -> Instrument:
    """Build an Instrument from column values."""

    assert not expcode or not expcode.startswith('/')

    # TODO(blais): Normalize to 'CALL' or 'PUT'
    putcall = putcall[0] if putcall else None

    # Infer the multiplier if it is not provided.
    if multiplier is None:
        match = re.match('(/.*)([FGHJKMNQUVXZ]2\d)', underlying)
        if match:
            _, calendar = match.groups()
        else:
            calendar = None

        if calendar is None:
            if expiration is not None:
                multiplier = futures.OPTION_CONTRACT_SIZE
            else:
                multiplier = 1
        else:
            multiplier = futures.MULTIPLIERS[underlying[:-3]]

    return Instrument(underlying, expiration, expcode, putcall, strike, multiplier)


def ParseUnderlying(symbol: str) -> str:
    """Parse only the underlying from the symbol."""
    match = re.match(r'(/?[A-Z0-9]+)(_.*)?', symbol)
    assert match
    return match.group(1)


def ParseProduct(underlying: str) -> str:
    """Return the product from an underlying."""
    match = re.fullmatch(r'(/?[A-Z0-9]+?)([FGHJKMNQUVXZ][23][0-9])', underlying)
    return match.group(1) if match else underlying


def FromString(symbol: str) -> Instrument:
    """Build an instrument object from the symbol string."""

    # Match options.
    match = re.match(r'(/?[A-Z0-9]+)_(?:(\d{6})|([A-Z0-9]+))_([CP])(.*)', symbol)
    if match:
        underlying, expi_str, expcode, putcall, strike_str = match.groups()
        expiration = (datetime.datetime.strptime(expi_str, '%y%m%d').date()
                      if expi_str
                      else None)
        strike = Decimal(strike_str)
    else:
        assert re.match('[A-Z]{3}_[A-Z]{3}', symbol) or ('_' not in symbol), symbol
        expiration, expcode, putcall, strike = None, None, None, None
        underlying = symbol

    return FromColumns(underlying, expiration, expcode, putcall, strike, None)


def ToString(inst: Instrument) -> str:
    """Convert an instrument to a string code."""

    instype = inst.instype
    if instype == 'FutureOption':
        # Note: For options on futures, the correct expiration date isn't always
        # available (e.g. from TOS). We ignore it for that reason, the date is
        # implicit in the option code. It's not very precise, but better to be
        # consistent.
        return "{}_{}_{}{}".format(
            inst.underlying, inst.expcode, inst.putcall, inst.strike)

    elif instype == 'Future':
        return inst.underlying

    elif instype == 'EquityOption':
        return "{}_{:%y%m%d}_{}{}".format(
            inst.underlying, inst.expiration, inst.putcall, inst.strike)

    elif instype == 'Equity':
        return inst.underlying

    raise ValueError('Invalid instrument type: {}'.format(instype))


def GetContractName(symbol: str) -> str:
    """Return the underlying root without the futures calendar expiration, e.g. '/CL'."""
    underlying = symbol.split('_')[0]
    if underlying.startswith('/'):
        match = re.match('(.*)([FGHJKMNQUVXZ]2\d)', underlying)
        assert match, string
        return match.group(1)
    else:
        return underlying


def Expand(table: Table, fieldname: str) -> Table:
    """Expand the symbol name into its component fields."""
    return (table
            .addfield('_instrument', lambda r: FromString(getattr(r, fieldname)))
            .addfield('instype', lambda r: r._instrument.instype)
            .addfield('underlying', lambda r: r._instrument.underlying)
            .addfield('expiration', lambda r: r._instrument.expiration)
            .addfield('expcode', lambda r: r._instrument.expcode)
            .addfield('putcall', lambda r: r._instrument.putcall)
            .addfield('strike', lambda r: r._instrument.strike)
            .addfield('multiplier', lambda r: r._instrument.multiplier)
            .cutout('_instrument'))


def Shrink(table: Table) -> Table:
    """Remove the component fields of the instrument."""
    return (table
            .cutout('instype', 'underlying', 'expiration', 'expcode',
                    'putcall', 'strike', 'multiplier'))
