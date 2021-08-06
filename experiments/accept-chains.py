#!/usr/bin/env python3
"""Read a list of chain ids and set the group of them.
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
from johnny.base import chains_pb2
from johnny.base import chains as chainslib
from johnny.base import config as configlib
from johnny.base import instrument
from johnny.base.etl import petl, Table


@click.command()
@click.option('--config', '-c', type=click.Path(exists=True),
              help="Configuration filename. Default to $JOHNNY_CONFIG")
@click.option('--status', '-s', default=None, type=chains_pb2.ChainStatus.Value,
              help="Set the status on the given chains.")
@click.option('--group', '-g', default=None,
              help="Group to assign to chains.")
@click.argument('chain_ids', nargs=-1)  # Chain ids to set group
def main(config: Optional[str],
         status: Optional[int],
         group: Optional[str],
         chain_ids: list[str]):
    "Find, process and print transactions."

    if not chain_ids:
        chain_ids = map(str.strip, sys.stdin.readlines())
        sys.stdin.close()

    filename = configlib.GetConfigFilenameWithDefaults(config)
    config = configlib.ParseFile(filename)
    chains_db = configlib.ReadChains(config.input.chains_db)
    chain_map = {c.chain_id: c for c in chains_db.chains}

    chain_ids = set(chain_ids)
    for chain_id in chain_ids:
        # Find the chain.
        chain = chain_map.get(chain_id, None)
        if chain is None:
            logging.error(f"Chain id '{chain_id}' not found.")
            continue

        # Apply the modifications.
        chainslib.AcceptChain(chain, group, status)

    print(configlib.ToText(config))


if __name__ == '__main__':
    main(obj={})
