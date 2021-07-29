#!/usr/bin/env python3
"""Mark overnight closed trades as earnings trades for review.
"""
__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import logging
import os
import sys
from typing import List, Optional

import click

from johnny.base import discovery
from johnny.base import mark
from johnny.base import chains as chainslib
from johnny.base import config as configlib
from johnny.base import instrument
from johnny.base.etl import petl, Table


@click.command()
@click.option('--config', '-c', type=click.Path(exists=True),
              help="Configuration filename. Default to $JOHNNY_CONFIG")
@click.option('--group', '-g', default="Earnings",
              help="Group to assign to closed one-day trades.")
def main(config: Optional[str], group: str):
    "Find, process and print transactions."

    filename = configlib.GetConfigFilenameWithDefaults(config)
    config = configlib.ParseFile(filename)
    transactions = petl.frompickle(config.output.imported_filename)
    chain_table = chainslib.TransactionsTableToChainsTable(transactions, config)
    chain_map = chain_table.recordlookupone('chain_id')

    for chain in config.chains:
        # We only process CLOSED chains.
        if chain.status != configlib.ChainStatus.CLOSED:
            continue

        # We try to find a corresponding calculated row.
        chain_row = chain_map.get(chain.chain_id)
        if chain_row is None:
            continue

        # We only tag one-day trades.
        if (chain_row.maxdate - chain_row.mindate).days != 1:
            continue

        chain.group = group

    print(configlib.ToText(config))


if __name__ == '__main__':
    main(obj={})
