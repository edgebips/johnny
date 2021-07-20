#!/usr/bin/env python3
"""Translate transaction ids to order ids.

On these retail platforms, it's more reliable to find the order ids in order to
define custom chains. The transaction ids aren't always available, and differ
depending on the sources (e.g., TOS doesn't provide it in the CSV fiels, but
only from the API).
"""
__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import functools
import logging
from typing import List, Optional

import click

from johnny.base import chains
from johnny.base import config as configlib
from johnny.base import discovery
from johnny.base import instrument
from johnny.base import match
from johnny.base import match2
from johnny.base import opening
from johnny.base.etl import Table, WrapRecords


@click.command()
@click.argument('config_filename', type=click.Path(exists=True))
def main(config_filename: str):
    config = configlib.ParseFile(config_filename)
    logtables = discovery.ReadConfiguredInputs(config)
    transactions = logtables[configlib.Account.LogType.TRANSACTIONS]

    # Write out original transactions.
    transactions.tocsv("/tmp/transactions.csv")

    # Ensure that
    txn_ids = set(transactions.values('transaction_id'))
    order_ids = set(transactions.values('order_id'))
    missing_chains = []
    for chain in config.chains:
        missing = False
        for id in chain.order_ids:
            if id not in order_ids:
                print(f"Missing order: {id}")
                missing = True
        for id in chain.transaction_ids:
            if id not in txn_ids:
                print(f"Missing txn: {id}")
                missing = True
        if missing:
            missing_chains.append(chain)
            print(chain)

    with open(f"{config_filename}.new", "w") as outfile:
        print(config, file=outfile)



if __name__ == '__main__':
    main()
