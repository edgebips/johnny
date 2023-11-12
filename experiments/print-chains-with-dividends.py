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
from johnny.base.etl import petl, Table, WrapRecords


def main():
    filename = configlib.GetConfigFilenameWithDefaults(None)
    config = configlib.ParseFile(filename)
    transactions = petl.frompickle(config.output.transactions_pickle)
    # print(transactions.lookallstr())

    for rec in transactions.aggregate("chain_id", WrapRecords).records():
        dividends = rec.value.selecteq("rowtype", "Cash")
        if petl.nrows(dividends):
            print(rec.value.lookallstr())


if __name__ == "__main__":
    main()
