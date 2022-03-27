#!/usr/bin/env python3
"""Remove finalization on futures.

Doing this as a one-off because I found a bug due to the way expiration is
partially specified in futures contracts, with chains calculations. Chains
should have been more split than they had been. Unfinalizing in order to
recompute.
"""
__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import logging
import csv
import re
from typing import List, Optional

import click

from johnny.base.etl import petl
from johnny.base import instrument
from johnny.base import chains_pb2
from johnny.base import chains as chainslib
from johnny.base import config as configlib

ChainStatus = chains_pb2.ChainStatus
Chain = chains_pb2.Chain


def Unfinalize(chain: Chain):
    """Mutate in-place unfinalizing the chain."""
    if chain.status == ChainStatus.FINAL and (
        chain.group in {"Error", "Scalp", "Experiments"}
        or chain.strategy in {"Short", "Pairs", "CallSpread", "PutSpread"}
    ):
        return
    print(chain.group, chain.strategy)

    chain.auto_ids.extend(chain.ids)
    chain.ClearField("ids")
    if chain.status == ChainStatus.FINAL:
        chain.status = ChainStatus.CLOSED


@click.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    help="Configuration filename. Default to $JOHNNY_CONFIG",
)
def main(config: Optional[str]):
    # Read chains DB.
    filename = configlib.GetConfigFilenameWithDefaults(config)
    config = configlib.ParseFile(filename)
    chains_db = configlib.ReadChains(config.input.chains_db)

    for chain in chains_db.chains:
        if re.match(
            r".*\.(6[ABCEJ]|NQ|ES|RTY|NG|CL|GC|SI|HG|PL|GE|Z[TFNB])[FGHJKMNQUVXZ]21$",
            chain.chain_id,
        ):
            logging.info(f"Unfinalizing chain '{chain.chain_id}'")
            Unfinalize(chain)

    with open(config.output.chains_db, "w") as outfile:
        outfile.write(configlib.ToText(chains_db))


if __name__ == "__main__":
    main(obj={})
