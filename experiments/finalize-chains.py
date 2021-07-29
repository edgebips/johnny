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
from johnny.base import chains as chainslib
from johnny.base import config as configlib
from johnny.base import instrument
from johnny.base.etl import petl, Table


@click.command()
@click.option('--config', '-c', type=click.Path(exists=True),
              help="Configuration filename. Default to $JOHNNY_CONFIG")
@click.option('--group', '-g', default=None,
              help="Group to assign to chains.")
@click.argument('chain_ids', nargs=-1)  # Chain ids to set group
def main(config: Optional[str], group: str, chain_ids: list[str]):
    "Find, process and print transactions."

    filename = configlib.GetConfigFilenameWithDefaults(config)
    config = configlib.ParseFile(filename)
    chain_map = {c.chain_id: c for c in config.chains}
    # transactions = petl.frompickle(config.output.imported_filename)
    # chain_table = chainslib.TransactionsTableToChainsTable(transactions, config)
    # chain_map = chain_table.recordlookupone('chain_id')

    chain_ids = set(chain_ids)
    for chain_id in chain_ids:
        # Find the chain.
        chain = chain_map.get(chain_id, None)
        if chain is None:
            logging.error(f"Chain id '{chain_id}' not found.")
            continue

        # Check status for errors or warnings.warn
        if chain.status == configlib.ChainStatus.FINAL:
            logging.error(f"Invalid attempt to finalize already finalized "
                          f"chain '{chain_id}'.")
            continue
        if chain.status != configlib.ChainStatus.CLOSED:
            logging.warning(f"Finalizing chain with state '{chain_id}'")

        # Apply the modifications.
        chain.status = configlib.ChainStatus.FINAL
        if group is not None:
            chain.group = group
        for iid in chain.auto_ids:
            chain.ids.append(iid)
        chain.ClearField('auto_ids')

    print(configlib.ToText(config))


if __name__ == '__main__':
    main(obj={})
