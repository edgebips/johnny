#!/usr/bin/env python3
"""Import new transactions from sources into our local database.

This tool reads a configuration file with a specification for transactions and
positions input sources for each account, normalizes them, runs the chains
processing code and ingests them everything to its own local database of
normalized and matched transactions. The local database is the source of data
for various tools, such as Johnny's trade log and eventually monitoring tools as
well. This is intended to be runnable mid-day.
"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import contextlib
import os
import datetime
import functools
from os import path
import logging
import traceback
import time
import tempfile
from typing import List, Optional, Tuple

import click
import simplejson

from johnny.base import chains as chainslib
from johnny.base import config as configlib
from johnny.base import discovery
from johnny.base import instrument
from johnny.base import match
from johnny.base import mark
from johnny.base import transactions as txnlib
from johnny.base.etl import petl, Table
from johnny.utils import timing


@click.command()
@click.option('--config', '-c', type=click.Path(exists=True),
              help="Configuration filename. Default to $JOHNNY_CONFIG")
@click.option('--output', '-o', type=click.Path(),
              help="Output directory. Default to temp dir.")
def main(config: Optional[str], output: Optional[str]):
    """Parse the configuration, the sources, transform, and save."""

    # Read the input configuration.
    filename = configlib.GetConfigFilenameWithDefaults(config)
    config = configlib.ParseFile(filename)

    # Read and filter the chains.
    #print(transactions.head(40).lookallstr())
    chains = petl.frompickle(config.output.chains)
    earnings_chains = (
        chains

        # Select the earnings trades only.
        .selecteq('group', 'Earnings')

        # Start sharing from April.
        .selectge('mindate', datetime.datetime(2021, 4, 1).date())

        # Remove chains where I already had a position I held through earnings.
        # Only keep those explicitly placed for earnings purposes.
        .selectlt('days', 5)

        # Remove unnecessary columns.
        .cutout('account')
    )
    chains_map = (earnings_chains
                  .dictlookupone('chain_id'))

    # Read the past transactions and narrow them to those in the chains..
    earnings_transactions = (
        petl.frompickle(config.output.transactions)

        # Remove non-earnings trades.
        .selectin('chain_id', chains_map)

        # Order by chain.
        .sort(('chain_id', 'datetime', 'transaction_id'))

        # Remove private data columns.
        .cutout('account', 'transaction_id', 'order_id', 'match_id')

        # Move the chain id to the front.
        .movefield('chain_id', 0)

        # Expand the symbol details to their own columns.
        .applyfn(instrument.Expand, 'symbol')
    )

    num_chains = earnings_chains.nrows()
    num_transactions = earnings_transactions.nrows()
    logging.info(f"Num chains: {num_chains}")
    logging.info(f"Num transactions: {num_transactions}")

    tempdir = output or tempfile.mkdtemp(prefix='earnings.')
    if not path.exists(tempdir):
        os.makedirs(tempdir)
    logging.info(f"Output to {tempdir}")
    earnings_chains.tocsv(path.join(tempdir, 'earnings_chains.csv'))
    earnings_transactions.tocsv(path.join(tempdir, 'earnings_transactions.csv'))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)-8s: %(message)s')
    main()
