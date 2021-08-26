#!/usr/bin/env python3
"""Fetch a list of chains to be annotated, run an editor with a list, and then
integrate that data in the chains.
"""
__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import logging
import tempfile
import subprocess
import os
import datetime
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
from johnny.base import recap as recaplib
from johnny.base.etl import petl, Table


@click.command()
@click.option('--config', '-c', type=click.Path(exists=True),
              help="Configuration filename. Default to $JOHNNY_CONFIG")
def main(config: Optional[str]):
    # Load the database.
    filename = configlib.GetConfigFilenameWithDefaults(config)
    config = configlib.ParseFile(filename)
    transactions = petl.frompickle(config.output.transactions)
    positions = petl.frompickle(config.output.positions)

    print(positions.lookallstr())


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)-8s: %(message)s')
    main(obj={})
