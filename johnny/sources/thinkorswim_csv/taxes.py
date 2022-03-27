"""Tax worksheets and form 8949 parsing for TD Ameritrade.
"""

from decimal import Decimal
import re

import petl
import dateutil.parser

from johnny.base import instrument
from johnny.base.etl import Table
from johnny.base.number import ToDecimal
from johnny.base import taxes


Q = Decimal("0.01")
ZERO = Decimal(0)


def read_worksheet(filename: str) -> Table:
    """Parse the Ameritrade worksheet ("without wash sales adjustments")."""

    table = petl.fromxlsx(filename)
    lowercase = {
        key: re.sub(r"[^A-Za-z_]", "", key.lower().replace(" ", "_"))
        for key in table.fieldnames()
    }
    worksheet = (
        table.rename(lowercase)
        .convert(
            ["proceeds", "cost", "gain_adj", "st_gain", "lt_gain", "or_gain"],
            lambda v: ZERO if v is None else Decimal(v).quantize(Q),
        )
        .addfield("gain_loss", lambda r: r.st_gain + r.lt_gain)
        .rename({"st_gain": "st_gain_loss", "lt_gain": "lt_gain_loss"})
        .selectne("security", "Total:")
        .addfield("60/40", lambda r: r.security.endswith("*"))
        .convert("security", lambda v: v.rstrip("*"))
        .addfield(
            "symbol",
            lambda r: _parse_security_description(r.security),
            index=0,
        )
        .applyfn(instrument.Expand, "symbol", "instype")
        .movefield("instype", 1)
    )
    taxes.validate_worksheet(worksheet)
    return worksheet


def read_form8949(filename: str) -> Table:
    """Parse the Ameritrade Form 8949 ("with wash sales adjustments")."""

    table = petl.fromxlsx(filename, sheet="Report for 8949")
    lowercase = {
        key: re.sub(r"[^A-Za-z_]", "", key.lower().replace(" ", "_"))
        for key in table.fieldnames()
    }
    form8949 = (
        table.rename(lowercase)
        .selectne("close_date", None)
        .convert("shares_sold", int)
        .convert(
            ["proceeds", "cost", "gainloss", "gainloss_adjustment"],
            lambda v: ZERO if v is None else Decimal(v).quantize(Q),
        )
        .rename(
            {
                "shares_sold": "quantity",
                "gainloss": "gain_loss",
                "gainloss_adjustment": "gain_adj",
                "stlt": "term",
            }
        )
        .addfield(
            "symbol",
            lambda r: _parse_security_description(r.security),
            index=0,
        )
        .applyfn(instrument.Expand, "symbol", "instype")
        .movefield("instype", 1)
        .convert("box", lambda v: v[-1])
    )
    taxes.validate_form8949(form8949)
    return form8949


_WRAPS = {
    "SPXW": "SPX",
    "RUTW": "RUT",
    "NDXP": "NDX",
}


def _parse_security_description(description: str) -> str:
    match = re.fullmatch(
        r"([A-Z0-9]+) ([A-Z][a-z][a-z] \d+ \d{4}) ([0-9.]+) (Call|Put)",
        description,
    )
    if match:
        und, expiration, strike, putcall = match.groups()
        und = _WRAPS.get(und, und)
        expiration = dateutil.parser.parse(expiration).date()
        if strike.endswith(".0"):
            strike = strike[:-2]
        strike = ToDecimal(strike)
        return f"{und}_{expiration:%y%m%d}_{putcall[0]}{strike}"

    match = re.fullmatch(r".* \((.*)\)", description)
    if match:
        return match.group(1)

    raise ValueError(f"Could not parse '{description}'")
