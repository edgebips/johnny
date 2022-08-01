#!/usr/bin/env python3
"""List missing vol infos on active trades for total vol stdev targets.
"""
__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from typing import List, Optional

import petl
import click

from johnny.base import chains_pb2
from johnny.base import chains as chainslib
from johnny.base import config as configlib

ChainStatus = chains_pb2.ChainStatus


@click.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    help="Configuration filename. Default to $JOHNNY_CONFIG",
)
def main(config: Optional[str]):
    "Find, process and print transactions."

    filename = configlib.GetConfigFilenameWithDefaults(config)
    config = configlib.ParseFile(filename)
    transactions = petl.frompickle(config.output.transactions)
    chains_db = configlib.ReadChains(config.input.chains_db)

    for chain in chains_db.chains:
        if chain.group == "Investment":
            chain.investment = True

    with open(config.output.chains_db, "w") as outfile:
        print(configlib.ToText(chains_db), file=outfile)


if __name__ == "__main__":
    main(obj={})
