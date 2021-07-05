"""Load information about futures contracts, in particular, the multipliers.
"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from typing import Tuple


# Standard equity option contract size.
OPTION_CONTRACT_SIZE = 100

# NOTE(blais): If you need a CME or CBOE product that is not here, please send
# me a patch to add in the multiplier. These just happen to be the ones I've
# needed in the past.
MULTIPLIERS = {
    # Indices : S&P 500
    '/ES'     : 50,
    '/MES'    : 5,
    'SPX'     : 100,

    # Indices : Nasdaq 100
    '/NQ'     : 20,
    '/MNQ'    : 2,
    'NDX'     : 100,

    # Indices : Russell 2000
    '/RTY'    : 50,
    '/M2K'    : 5,
    'RUT'     : 100,

    # Indices : Dow Jones
    '/YM'     : 5,
    '/MYM'    : 0.5,
    'DJI'     : 100,

    # Volatility
    '/VX'     : 1000,
    'VIX'     : 100,
    'RVX'     : 1000,
    'VXN'     : 1000,
    'VXD'     : 1000,

    # FX
    '/6E'     : 125_000,
    '/6J'     : 12_500_000,
    '/6A'     : 100_000,
    '/6C'     : 100_000,
    '/6B'     : 62_500,
    '/SFX'    : 100,

    # Energy
    '/CL'     : 1000,
    '/NG'     : 10_000,
    '/SMO'    : 100,

    # Metals
    '/GC'     : 100,
    '/MGC'    : 10,
    '/SI'     : 5000,
    '/SIL'    : 1000,
    '/HG'     : 25000,
    #'/QC'    : 12500,
    '/PA'     : 100,
    '/PL'     : 50,
    '/SPRE'   : 100,

    # Rates
    '/ZQ'     : 4167,
    '/GE'     : 2500,
    '/ZT'     : 2000,
    '/ZF'     : 1000,
    '/ZN'     : 1000,
    '/ZB'     : 1000,
    '/S2Y'    : 100,
    '/S10Y'   : 100,
    '/S30Y'   : 100,

    # Agricultural
    '/ZC'     : 50,
    '/ZS'     : 50,
    '/ZW'     : 50,

    # Livestock
    '/HE'     : 400,
    '/LE'     : 400,

    # Small exchange unrelated
    '/SM75'   : 100,
    '/STIX'   : 100,
    '/S420'   : 100
}


# This is a mapping of (option-product-code, month-code) to
# (futures-product-code, month-code). Options are offered on a monthly basis,
# but the underlying futures contract isn't necessarily offered for every month
# (depends on seasonality sometimes), so the underlying is sometimes for the
# same month (and the options expire a few days ahead of the futures) or for the
# subsequent month (in which case multiple months are applicable to the same
# underlying).
#
# CME has definitions on this, like this: "/SI: Monthly contracts listed for 3
# consecutive months and any Jan, Mar, May, and Sep in the nearest 23 months and
# any Jul and Dec in the nearest 60 months."
# https://www.cmegroup.com/trading/metals/precious/silver_contractSpecs_options.html
#
# We need to eventually encode all those rules as logic, as some input files
# (notably, from TOS) sometimes only produce the options code and in order to
# produce a normalized symbol we need both.

# NOTE(blais): Temporary monster hack, based on my own file.
# Update as needed.

# TODO(blais): Remove leading slash in the option code key. It's just not
# expected to be there.
_TEMPORARY_MAPPING = {
    ('/SO', 'M'): ('/SI', 'N'),
    ('/SO', 'N'): ('/SI', 'N'),
    ('/OG', 'N'): ('/GC', 'Q'),
    ('/EUU', 'M'): ('/6E', 'M'),
    ('/EUU', 'Q'): ('/6E', 'U'),
    ('/OZC', 'N'): ('/ZC', 'N'),
    ('/OZS', 'N'): ('/ZS', 'N'),
    ('/OZN', 'N'): ('/ZN', 'U'),
    ('/QNE', 'G'): ('/NQ', 'H'),
    ('/LO', 'N'): ('/CL', 'N'),
    ('/LNE', 'N'): ('/NG', 'N'),
    ('/GBU', 'N'): ('/6B', 'U'),
    ('/CAU', 'N'): ('/6C', 'U'),
    ('/JPY', 'N'): ('/6J', 'U'),
    ('/EW', 'N'): ('/ES', 'U'),
    ('/EW3', 'N'): ('/ES', 'U'),
    ('/R3E', 'N'): ('/RTY', 'U'),
    ('/RTM', 'N'): ('/RTY', 'U'),
    ('/QNE', 'N'): ('/NQ', 'U'),
    ('/OZB', 'Q'): ('/ZB', 'U'),
    ('/OZS', 'U'): ('/ZS', 'U'),
}

def GetUnderlyingMonth(optcontract: str, optmonth: str) -> Tuple[str, str]:
    """Given the future option contract code and its month (e.g., '/SOM'), return
    the underlying future and its month ('/SIN'). The reason this function
    exists is that not all the months are available as underlyings. This depends
    on the particulars of each futures contract, and the details depend on
    cyclicality / availability / seasonality of the product.
    """
    return _TEMPORARY_MAPPING[(optcontract, optmonth)]
