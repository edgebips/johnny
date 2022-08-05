#!/usr/bin/env python3
"""Compute a list of matches realized in a specific interval of time.

I'm doing this because when you move states, you have to attribute the realized
gains as per the time period you lived there.
"""
__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import logging
import os
import datetime as dt
import sys
import collections
from typing import List, Optional

import click

from johnny.base import chains as chainslib
from johnny.base import chains_pb2
from johnny.base import config as configlib
from johnny.base import discovery
from johnny.base import instrument
from johnny.base import mark
from johnny.base import match
from johnny.base.etl import petl, Table, Record


@click.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    help="Configuration filename. Default to $JOHNNY_CONFIG",
)
@click.option(
    "--from",
    "-f",
    "from_",
    default=str(dt.date(dt.date.today().year, 1, 1)),
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Start date",
)
@click.option(
    "--to",
    "-t",
    default=str(dt.date.today()),
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Ending date",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(exists=False),
    help="Output CSV filename.",
)
def main(
    config: Optional[str], from_: dt.datetime, to: dt.datetime, output: Optional[str]
):
    "Find, process and print transactions."

    filename = configlib.GetConfigFilenameWithDefaults(config)
    config = configlib.ParseFile(filename)
    output_rec = []
    transactions = petl.frompickle(config.output.transactions)
    chains = petl.frompickle(config.output.chains)
    for chain, txns in chainslib.GetChainsAndTransactions(chains, transactions):
        if (
            txns.selecteq("effect", "CLOSING")
            .select(lambda r: from_ <= r.datetime < to)
            .nrows()
            > 0
        ):
            matches = match.GetChainMatchesFromTransactions(
                txns, match.ShortBasisReportingMethod.INVERT
            )
            selected_matches = matches.select(
                lambda r: from_.date() <= r.date_max < to.date()
            )
            if 0:
                print("-" * 120)
                print(chain.lookallstr())
                print(txns.lookallstr())
                print(selected_matches.lookallstr())
            output_rec.extend(selected_matches.records())

    output_matches = petl.wrap([matches.fieldnames()] + output_rec).sort(
        ["account", "date_disposed"]
    )
    print(output_matches.lookallstr())

    if output:
        output_matches.tocsv(output)


if __name__ == "__main__":
    main(obj={})
