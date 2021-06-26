"""Find the files to form the database.

We rely on patterns and dates and sorting to sieve the latest files of every
type in a bungled directory of downloads. For now.
"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import collections
import os
from os import path
from typing import Callable, Dict, List, Optional, Tuple

from johnny.base.etl import petl, Table

from johnny.base import chains
from johnny.base import match
from johnny.broker.ameritrade import positions as positions_tos
from johnny.broker.ameritrade import transactions as transactions_tos
from johnny.broker.tastyworks import positions as positions_tw
from johnny.broker.tastyworks import transactions as transactions_tw


# Args:
#   filename: str
# Returns:
#   account: str
#   sortkey: str
#   module: ModuleType
MatchFn = Callable[[str], Optional[Tuple[str, str, callable]]]


def FindFiles(fileordirs: List[str],
              matchers: List[MatchFn]) -> Dict[str, callable]:
    """Read in the transactions log files from given directory and filenames."""

    # If input is empty, use the CWD.
    if not fileordirs:
        fileordirs = [os.getcwd()]
    elif isinstance(fileordirs, str):
        fileordirs = [fileordirs]

    # Find all the files for each account.
    byaccount = collections.defaultdict(list)

    def MatchStore(filename: str):
        for matcher in matchers:
            r = matcher(filename)
            if r:
                account, sortkey, parser = r
                byaccount[account].append((sortkey, filename, parser))

    for filename in fileordirs:
        if path.isdir(filename):
            for fn in os.listdir(filename):
                MatchStore(path.join(filename, fn))
        else:
            MatchStore(filename)

    # Select the latest matched file for each account.
    matchdict = {}
    for account, matchlist in byaccount.items():
        _, filename, parser = next(iter(sorted(matchlist, reverse=True)))
        matchdict[account] = (filename, parser)

    return matchdict


def GetTransactions(fileordirs: List[str]) -> Tuple[Table, List[str]]:
    """Find transactions files and parse and concatenate contents."""

    matches = FindFiles(
        fileordirs, [
            transactions_tw.MatchFile,
            transactions_tos.MatchFile,
        ])

    filenames = []
    tables = []
    for unused_account, (filename, parser) in sorted(matches.items()):
        transactions = parser(filename)
        if not transactions:
            continue
        tables.append(transactions)
        filenames.append(filename)

    table = petl.cat(*tables) if tables else petl.empty()
    return table, filenames


def GetPositions(fileordirs: List[str]) -> Tuple[Table, List[str]]:
    """Find positions files and parse and concatenate contents."""

    matches = FindFiles(
        fileordirs, [
            positions_tw.MatchFile,
            positions_tos.MatchFile,
        ])

    filenames = []
    tables = []
    for unused_account, (filename, parser) in sorted(matches.items()):
        positions = parser(filename)
        if not positions:
            continue
        filenames.append(filename)
        tables.append(positions)

    table = petl.cat(*tables) if tables else petl.empty()
    return table, filenames
