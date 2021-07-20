"""Load information about futures contracts, in particular, the multipliers.
"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from typing import Tuple

from johnny.base import config as configlib


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


class FutOptMonthMapper:
    """A read-only dict mapping options month codes to futures month codes."""

    def __init__(self, mapping: configlib.FutOptMonthMapping):
        self.month_map = {
            (m.option_product, m.option_month): (m.future_product, m.future_month)
            for m in mapping.months}

    def get(self, optcontract: str, optmonth: str) -> Tuple[str, str]:
        return self.month_map[(optcontract, optmonth)]
