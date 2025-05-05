"""Process the Schwab website transactions and output a table of
descriptions of treasuries to insert into the input.
"""

from johnny.base.etl import petl, Record, Table
from johnny.base import transactions as txnlib
from johnny.sources.schwab import transactions as txnlib

from typing import Optional
from decimal import Decimal
import argparse
import datetime as dt
import dateutil.parser
import logging
import re


def ImportTreasuries(filename: str) -> Table:
    table = txnlib.ReadSchwabTable(filename)
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
        "filename", help="Filename of Schwarb website transactions download"
    )
    args = parser.parse_args()

    table = ImportTreasuries(args.filename)
    print(table.replaceall(None, "").lookallstr())


if __name__ == "__main__":
    main()
