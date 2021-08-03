#!/usr/bin/env python3
"""Modify attributes of chains from a CSV file.

This provides a quick way to make mass modification: create a spreadsheet,
filter the columns, and set overrides. This is useful when you have to
categorize past history, e.g., hundreds or more of chains.
"""
__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import logging
import csv
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
ChainStatus = chains_pb2.ChainStatus


@click.command()
@click.argument('csv_filename', default=None)
@click.option('--config', '-c', type=click.Path(exists=True),
              help="Configuration filename. Default to $JOHNNY_CONFIG")
def main(config: Optional[str],
         csv_filename: str):

    # Read the existing configuration.
    filename = configlib.GetConfigFilenameWithDefaults(config)
    config = configlib.ParseFile(filename)
    chain_map = {c.chain_id: c for c in config.chains}

    # Read the CSV file.
    table = petl.fromcsv(csv_filename)
    fieldnames = table.fieldnames()
    if 'chain_id' not in fieldnames:
        logging.error("No `chain_id` column is present.")
        return 1

    # Apply status if present.
    for field, converter in [('group', str),
                             ('status', ChainStatus.Value),
                             ('strategy', str)]:
        if field in fieldnames:
            for row in (table
                        .rename(field, 'value')
                        .selecttrue('value')
                        .convert('value', converter)
                        .cut('chain_id', 'value')).records():
                chain = chain_map[row.chain_id]
                setattr(chain, field, row.value)

    print(configlib.ToText(config))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)-8s: %(message)s')
    main(obj={})
