"""Process the Schwab website transactions and output a table of
descriptions of treasuries to insert into the input.
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
    return Decimal(value.replace(",", "").replace("$", "")) if value else None


def GetMissingSymbol(value: str, rec: Record) -> str:
    # For pre-migration TDA TRAN, the symbol is not always present in the
    # 'symbol' column. Fill in when we can.
    if value:
        return value
    mo = re.search(r"\(([A-Z0-9]+)\)", rec.description)
    return mo.group(1) if mo else ""


Q3 = Decimal("0.001")


def GetRate(rec: Record) -> str:
    mo = re.search(r"(\d*(\.\d+)?)%", rec.description)
    if mo:
        return Decimal(mo.group(1)).quantize(Q3) if mo else None

    mo = re.search(r"\bT BILL\b", rec.description)
    if mo:
        return Decimal("0.000")


def GetMaturity(rec: Record) -> str:
    mo = re.search(r"due (\d\d/\d\d/\d\d\d\d)", rec.description)
    if mo:
        return dt.datetime.strptime(mo.group(1), "%m/%d/%Y").date()

    mo = re.search(r"DUE (\d\d/\d\d/\d\d)", rec.description)
    if mo:
        return dt.datetime.strptime(mo.group(1), "%m/%d/%y").date()


def ReadSchwabTable(filename: str) -> Table:
    """Read and clean the Schwab table."""
    table = petl.fromcsv(filename)
    return (
        table.rename(
            {col: col.lower().strip().replace(" & ", "_") for col in table.fieldnames()}
        )
        .convert("date", lambda v: dateutil.parser.parse(v).date())
        .addfield("tda", lambda r: r["description"].startswith("TDA TRAN - "))
        .convert("quantity", ParseNumber)
        .convert("price", ParseNumber)
        .convert("fees_comm", ParseNumber)
        .convert("amount", ParseNumber)
        .convert("symbol", GetMissingSymbol, pass_row=True)
        # Remove TDA TRAN prefix.
        # .convert("description", lambda v: re.sub("^TDA TRAN - ", "", v))
        #
        # TODO: Add missing firleds for transactions.
    )


def ImportTreasuries(filename: str) -> Table:
    table = ReadSchwabTable(filename)
    table = (
        table.select(
            "description", lambda v: re.search("treasur", v, flags=re.IGNORECASE)
        )
        .addfield("rate", GetRate)
        .addfield("maturity", GetMaturity)
        .cut(["symbol", "rate", "maturity"])
        .distinct()
    )

    def reduce(symbol, grouper):
        rows = list(grouper)
        out = []
        for index in range(len(rows[0])):
            values = set([row[index] for row in rows])
            values.discard(None)
            assert len(values) in {0, 1}, values
            out.append(next(iter(values)) if values else None)
        return out

    return table.rowreduce(key="symbol", reducer=reduce)


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
