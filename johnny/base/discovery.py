"""Find the files to form the database.

We rely on patterns and dates and sorting to sieve the latest files of every
type in a bungled directory of downloads. For now.
"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import collections
import glob
import importlib
import os
from os import path
from typing import Callable, Dict, List, Optional, Tuple
from more_itertools import last

from johnny.base.etl import petl, Table

from johnny.base import chains
from johnny.base import match
from johnny.base import config as configlib
from johnny.sources.thinkorswim_csv import positions as positions_tos
from johnny.sources.thinkorswim_csv import transactions as transactions_tos
from johnny.sources.tastyworks_csv import positions as positions_tw
from johnny.sources.tastyworks_csv import transactions as transactions_tw


def GetLatestFile(source: str) -> str:
    """Given a globbing pattern, find the most recent file matching the pattern,
    based on the timestamp."""
    filenames = sorted((path.getctime(filename), filename)
                       for filename in glob.glob(source, recursive=True))
    _, filename = last(filenames)
    return filename


def ReadConfiguredInputs(
        config: configlib.Config) -> Dict[int, Table]:
    """Read the explicitly configured inputs in the config file.
    Returns tables for the transactions and positions."""

    # Parse and accumulate by log type.
    tablemap = collections.defaultdict(list)
    for account in config.input.accounts:
        module = importlib.import_module(account.module)
        table = module.Import(account.source)
        if table is None:
            continue
        tablemap[account.logtype].append(table)

    # Concatenate tables for each logtype.
    bytype = {}
    for logtype, tables in tablemap.items():
        bytype[logtype] = petl.cat(*tables)

    return bytype
