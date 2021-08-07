#!/usr/bin/env python3
"""Insert comments on latest chain with a given symbol name.

This script reads a CSV file with 'symbol' and 'comment' columns, finds the
latest chain with that symbol and sets the comment on that chain.
"""
__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import logging
import csv
import re
from typing import List,Optional

import click

from johnny.base.etl import petl
from johnny.base import instrument
from johnny.base import chains_pb2
from johnny.base import chains as chainslib
from johnny.base import config as configlib
ChainStatus = chains_pb2.ChainStatus


def ParseInputFile(comments_filename: str,
                   groups: List[str],
                   strategies: List[str]):
    """Parse a text file with the input columns as a flexible format."""
    dicts = []
    with open(comments_filename) as infile:
        for row in csv.reader(infile):
            linedict = {}
            for comp in row:
                comp = comp.strip()
                if re.fullmatch("([A-Z]+|/[A-Z0-9]+)", comp):
                    linedict['symbol'] = comp
                elif comp in groups:
                    linedict['group'] = comp
                elif comp in strategies:
                    linedict['strategy'] = comp
                else:
                    linedict['comment'] = comp
            if 'symbol' not in linedict:
                logging.error(f"No symbol found on line {row}; skipping")
                continue
            dicts.append(linedict)
    return (petl.fromdicts(dicts)
            .sort('symbol'))


@click.command()
@click.argument('comments_filename', type=click.Path(exists=True))
@click.option('--config', '-c', type=click.Path(exists=True),
              help="Configuration filename. Default to $JOHNNY_CONFIG")
@click.option('--force/--no-force',
              help="Overwrite comments if already set.")
def main(comments_filename: str, config: Optional[str], force: bool=False):
    # Read chains DB.
    filename = configlib.GetConfigFilenameWithDefaults(config)
    config = configlib.ParseFile(filename)
    chains_db = configlib.ReadChains(config.input.chains_db)
    chains_map = {c.chain_id: c for c in chains_db.chains}

    # Read the CSV file with comments.
    groups = [c.group for c in chains_db.chains if c.group]
    strategies = [c.strategy for c in chains_db.chains if c.strategy]
    comments = ParseInputFile(comments_filename, groups, strategies)

    # Read prior transactions.
    last_chain = (petl.frompickle(config.output.transactions)
                  .applyfn(instrument.Expand, 'symbol')
                  .sort(['datetime', 'chain_id'])
                  .cut('underlying', 'chain_id')
                  .groupselectlast('underlying')
                  .recordlookupone('underlying'))

    for crow in comments.records():
        srow = last_chain.get(crow.symbol, None)
        if srow is None:
            continue
        chain = chains_map.get(srow.chain_id, None)
        if chain is None:
            continue
        if force or not chain.comment:
            chain.comment = crow.comment

    with open(config.output.chains_db, 'w') as outfile:
        outfile.write(configlib.ToText(chains_db))


if __name__ == '__main__':
    main(obj={})
