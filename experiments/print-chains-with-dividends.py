#!/usr/bin/env python3
"""Print all chains associated with dividends.
"""

from typing import List, Optional
import argparse
import logging
import os
import itertools

import click

from johnny.base import chains as chainslib
from johnny.base import config as configlib
from johnny.base import discovery
from johnny.base import instrument
from johnny.base import mark
from johnny.base.etl import petl, Table


def GetLogTables(config: configlib.Config, logtype: int) -> Table:
    """Process the log tables."""
    logtables = discovery.ImportConfiguredInputs(config, {logtype})
    opt_table = logtables.get(logtype)
    return opt_table if opt_table is not None else petl.empty()


def PrintTable(table: Table, expand: bool):
    """Print the table, given the options."""
    if expand:
        table = instrument.Expand(table, "symbol")
    print(table.lookallstr())


def main():
    filename = configlib.GetConfigFilenameWithDefaults(None)
    config = configlib.ParseFile(filename)
    transactions = petl.frompickle(config.output.transactions)

    header = transactions.header()
    def agg(chain):
        table = petl.wrap(list(itertools.chain([header], chain)))
        dividends = table.selecteq("rowtype", "Dividend")
        if petl.nrows(dividends):
            print(table.lookallstr())

    table = transactions.aggregate("chain_id", agg)
    list(table)


if __name__ == "__main__":
    main()
