#!/usr/bin/env python3
"""Mark overnight closed trades as earnings trades for review.

Note: this only works after the fact.
It doesn't work super well yet, selecting the names manually works better.
"""
__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import datetime
import logging
import os
import sys
import re
from typing import List, Optional

import click

from johnny.base import discovery
from johnny.base import mark
from johnny.base import chains as chainslib
from johnny.base import chains_pb2
from johnny.base import config as configlib
from johnny.base import instrument
from johnny.base.etl import petl, Table


@click.command()
@click.option('--config', '-c', type=click.Path(exists=True),
              help="Configuration filename. Default to $JOHNNY_CONFIG")
@click.option('--date', '-d', default=str(datetime.date.today()),
              type=click.DateTime(formats=["%Y-%m-%d"]),
              help="Ending date for the 1 day trade.")
@click.option('--group', '-g', default="Earnings",
              help="Group to assign to closed one-day trades.")
def main(config: Optional[str], group: str, date: datetime.date):
    "Find, process and print transactions."
    date = date.date()

    filename = configlib.GetConfigFilenameWithDefaults(config)
    config = configlib.ParseFile(filename)
    transactions = petl.frompickle(config.output.transactions)
    chains_db = configlib.ReadChains(config.input.chains_db)
    chain_table, _ = chainslib.TransactionsTableToChainsTable(transactions, chains_db)
    chain_map = chain_table.recordlookupone('chain_id')

    finalized = []
    for chain in chains_db.chains:
        # We only process CLOSED chains.
        if chain.status != chains_pb2.ChainStatus.CLOSED:
            continue

        # We try to find a corresponding calculated row.
        chain_row = chain_map.get(chain.chain_id)
        if chain_row is None:
            continue

        # We only tag one-day trades.
        num_days = (chain_row.maxdate - chain_row.mindate).days
        if not (num_days == 1 and chain_row.maxdate == date):
            continue

        chainslib.AcceptChain(chain, group, status=None)
        finalized.append(chain.chain_id)

    for chain_id in sorted(finalized, key=lambda s: re.search('.*\.([A-Z0-9.]+)$', s).group(1)):
        print(chain_id, file=sys.stderr)

    print(configlib.ToText(config))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)-8s: %(message)s')
    main(obj={})
