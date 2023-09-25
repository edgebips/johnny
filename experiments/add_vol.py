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
    transactions = petl.frompickle(config.output.transactions_pickle)
    chains_db = configlib.ReadChains(config.input.chains_db)
    chain_table, _ = chainslib.TransactionsTableToChainsTable(transactions, chains_db)
    chain_map = chain_table.recordlookupone("chain_id")

    rows = []
    for chain in chains_db.chains:
        # We only process ACTIVE chains.
        if chain.status != chains_pb2.ChainStatus.ACTIVE:
            continue

        # We try to find a corresponding calculated row.
        chain_row = chain_map.get(chain.chain_id)
        if chain_row is None:
            continue

        rows.append((chain.chain_id, chain_row.underlyings, chain_row.mindate))

    print(petl.wrap([("chain_id", "underlyings", "mindate")] + rows).lookallstr())


if __name__ == "__main__":
    main(obj={})
