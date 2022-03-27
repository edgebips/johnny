"""Compute a textual description for IRS purposes.

This code can be used to generate a description of instruments. For futures
contracts, it pulls the data from the Mulmat database of CME contracts.
"""

from functools import partial
from typing import Mapping, Tuple
import re
import datetime as dt
import pprint

import dateutil.parser
import click

import mulmat
from mulmat.lists import split_month
from mulmat import xdescriptions
from mulmat import months
from johnny.base.etl import Table, Record, Replace
from johnny.base.etl import petl
from johnny.base import instrument


def get_description(contract_getter, r: Record):
    if r.instype in {"Future", "FutureOption", "IndexOption"}:
        return contract_getter(r)
    elif r.instype == "Equity":
        return f"Share of {r.symbol} Stock"
    elif r.instype == "Collectibles":
        return f"Share of {r.symbol} Stock (Collectible)"
    elif r.instype == "EquityOption":
        term = options_term(r)
        return f"Option of {r.underlying} Stock - {term}"
    elif r.instype == "NonEquityOption":
        term = options_term(r)
        return f"Option of {r.underlying} Stock (Nonequity) - {term}"
    raise ValueError(f"Not supported: {r}")


def build_globex_description_map(db: Table) -> Mapping[str, str]:
    return (
        db.selectin("Contract Type", {"Future"})
        .cut("Globex", "Product")
        .distinct()
        .lookupone("Globex", "Product")
    )

def build_options_description_map(db: Table) -> Mapping[str, str]:
    # In the standardized symbology, we don't have the option wrap code, so we
    # look it up from CME using the expiration date.
    return (
        db.selectin("Contract Type", {"Option"})
        .cut("Underlying", "Expiration", "Product")
        .convert("Expiration", lambda v: dateutil.parser.parse(v).date())
        .distinct()
        #.lookupone(["Underlying", "Expiration"], "Product")
        .lookupone("Underlying", "Product")
    )


def futures_term(month: str, year: int):
    """Produce a description of the futures term."""
    mth_name = months.CODE_TO_NAME[month]
    return f"{mth_name} {year}"


def options_term(r: Record):
    """Produce a description of the options term."""
    putcall = 'Put' if r.putcall[0] == 'P' else 'Call' if r.putcall[0] == 'C' else ""
    return f"{r.expiration:%m/%d/%y} {putcall} {r.strike} "


def build_contract_getter(db: Table, settle_year: int):
    futures_map = build_globex_description_map(db)
    options_map = build_options_description_map(db)

    def getter(r: Record):
        if r.instype == 'Future':
            product, month, year = split_month(r.symbol, settle_year)
            fdescription = (futures_map.get(product[1:]) or
                            xdescriptions.CBOE_DESCRIPTIONS.get(product[1:]))
            term = futures_term(month, year)
            description = f"{fdescription} - {term}"

        elif r.instype == 'FutureOption':
            product, month, year = split_month(r.underlying, settle_year)
            fdescription = (futures_map.get(product[1:]) or
                            xdescriptions.CBOE_DESCRIPTIONS.get(product[1:]))
            term = options_term(r)
            description = f"{fdescription} Option - {term}"

        elif r.instype == 'IndexOption':
            fdescription = xdescriptions.CBOE_DESCRIPTIONS.get(r.underlying)
            term = options_term(r)
            description = f"{fdescription} Option - {term}"

        else:
            raise ValueError("Not supported")
        return description

    return getter


@click.command()
@click.argument("filename", type=click.Path(exists=True))
def main(filename: str):
    table = (
        petl.fromcsv(filename)
        .cut("symbol")
        .applyfn(instrument.Expand, "symbol")
        .distinct()
    )

    db = mulmat.read_cme_database()
    contract_getter = build_contract_getter(db, dt.date.today().year)

    table = (table
             .addfield("description", partial(get_description, contract_getter))
             .applyfn(instrument.Shrink))
    print(table.lookallstr())


if __name__ == "__main__":
    main()
