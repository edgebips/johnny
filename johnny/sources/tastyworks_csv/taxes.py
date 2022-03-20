"""Tax worksheets and form 8949 parsing for Tastyworks.
"""

import re

import petl
import dateutil.parser

from johnny.base import instrument
from johnny.base.etl import Table
from johnny.base.number import ToDecimal
from johnny.base import taxes


def read_worksheet(filename: str) -> Table:
    """Parse the Ameritrade worksheet ("without wash sales adjustments")."""

    table = petl.fromcsv(filename)
    lowercase = {key: key.lower() for key in table.fieldnames()}
    worksheet = (
        table.rename(lowercase)
        .selecteq("tax year", "2021")
        .convert(
            [
                "quantity",
                "cost",
                "proceeds",
                "gain_adj",
                "short_term_gain_loss",
                "long_term_gain_loss",
                "ordinary_gain_loss_amt",
            ],
            ToDecimal,
        )
        .addfield("gain_loss", lambda r: r.short_term_gain_loss + r.long_term_gain_loss)
        .rename(
            {
                "short_term_gain_loss": "st_gain_loss",
                "long_term_gain_loss": "lt_gain_loss",
            }
        )
        .addfield(
            "symbol",
            lambda r: _parse_security_description(
                r.security_description, r.underlying_symbol, r.opening_transaction
            ),
            index=0,
        )
        .applyfn(instrument.Expand, "symbol", "instype")
        .movefield("instype", 1)
    )
    taxes.validate_worksheet(worksheet)
    return worksheet


def read_form8949(filename: str) -> Table:
    """Parse the Ameritrade Form 8949 ("with wash sales adjustments")."""

    table = petl.fromcsv(filename)
    lowercase = {key: key.lower() for key in table.fieldnames()}
    form8949 = (
        table.rename(lowercase)
        .convert(
            [
                "quantity",
                "cost",
                "proceeds",
                "gain_loss",
                "gain_loss_adj",
            ],
            ToDecimal,
        )
        .rename({"8949_box": "box", "gain_loss_adj": "gain_adj"})
        .addfield(
            "symbol",
            lambda r: _parse_security_description(
                r.security_description, r.underlying_symbol, r.opening_transaction
            ),
            index=0,
        )
        .applyfn(instrument.Expand, "symbol", "instype")
        .movefield("instype", 1)
        .convert("box", str.strip)
        .convert("term", {"S": "ST", "L": "LT"})
    )
    taxes.validate_form8949(form8949)
    return form8949


def _parse_security_description(description: str, und: str, txntype: str) -> str:
    if txntype in {"BTO", "STO"}:
        match = re.match(
            r"(PUT|CALL)\s+([A-Z0-9]+)\s+(\d\d/\d\d/\d\d)\s+([0-9.]+)", description
        )
        if match:
            putcall, und, expiration, strike = match.groups()
            expiration = dateutil.parser.parse(expiration).date()
            strike = ToDecimal(strike.rstrip("0") if "." in strike else strike)
            return f"{und}_{expiration:%y%m%d}_{putcall[0]}{strike}"
        raise ValueError(f"Could not parse option '{description}'")

    return und.lstrip("*")
