"""Process the Ameritrade website transactions and output a table of
descriptions of treasuries to insert into the input.

Note that this is not information that's possible to find in TOS.
"""

from johnny.base.etl import petl, Record, Table
from johnny.base import transactions as txnlib

from typing import Optional
from decimal import Decimal
import argparse
import datetime as dt
import dateutil.parser
import logging
import re


def ParseNumber(value: str) -> Optional[Decimal]:
    return Decimal(value) if value else None


def GetSymbol(rec: Record) -> str:
    mo = re.search(r"\((.*)\)", rec.description)
    assert mo
    return mo.group(1)


def GetRowType(rec: Record) -> str:
    if re.match(r"BUY TRADE", rec.description):
        return txnlib.Type.Trade
    if re.match(r"BONDS - REDEMPTION", rec.description):
        return txnlib.Type.Trade
    if re.match(r"US TREASURY INTEREST", rec.description):
        return txnlib.Type.Dividend
    raise ValueError(f"Can't figure out rowtype for {rec}")


def GetInstruction(rec: Record) -> str:
    if re.match(r"BUY TRADE", rec.description):
        return "BUY"
    if re.match(r"BONDS - REDEMPTION", rec.description):
        return "SELL"
    if re.match(r"US TREASURY INTEREST", rec.description):
        return None
    raise ValueError(f"Can't figure out instruction for {rec}")


def GetPosEffect(rec: Record) -> str:
    if re.match(r"BUY TRADE", rec.description):
        return "OPENING"
    if re.match(r"BONDS - REDEMPTION", rec.description):
        return "CLOSING"
    if re.match(r"US TREASURY INTEREST", rec.description):
        return None
    raise ValueError(f"Can't figure out instruction for {rec}")


def GetRate(rec: Record) -> str:
    mo = re.search("(\d*\.\d+) %", rec.description)
    return Decimal(mo.group(1)) if mo else None


def GetMaturity(rec: Record) -> str:
    mo = re.search("due (\d\d/\d\d/\d\d\d\d)", rec.description)
    return dt.datetime.strptime(mo.group(1), "%m/%d/%Y").date()


def ImportTreasuries(filename: str) -> Table:
    table = petl.fromcsv(filename)
    return (
        table.rename({col: col.lower().strip() for col in table.fieldnames()})
        .selecttrue("description")
        .cutout(
            "reg fee",
            "short-term rdm fee",
            "fund redemption fee",
            "deferred sales charge",
            "commission",
        )
        .select(lambda r: re.search(r".*TREASURY", r.description))
        .convert("date", lambda v: dateutil.parser.parse(v).date())
        .cutout("symbol")
        .addfield("symbol", GetSymbol)
        .addfield("rowtype", GetRowType)
        .addfield("instruction", GetInstruction)
        .addfield("pos_effect", GetPosEffect)
        .addfield("rate", GetRate)
        .addfield("maturity", GetMaturity)
        .convert("price", ParseNumber)
        .convert("amount", ParseNumber)
        .convert("quantity", ParseNumber)
        .sort(["symbol", "date"])
    )


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)-8s: %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.strip())
    parser.add_argument(
        "filename", help="Filename of Ameritrade website transactions download"
    )
    args = parser.parse_args()

    table = ImportTreasuries(args.filename)
    print(table.replaceall(None, "").lookallstr())


if __name__ == "__main__":
    main()
