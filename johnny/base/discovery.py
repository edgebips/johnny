"""Find the files to form the database.

We rely on patterns and dates and sorting to sieve the latest files of every
type in a bungled directory of downloads. For now.
"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from decimal import Decimal
from os import path
from typing import Any, Dict, Optional, Set
import collections
import glob
import importlib
import logging
import re
import typing
import functools

from more_itertools import last
from dateutil import parser

from johnny.base import config as configlib
from johnny.base import instrument
from johnny.base import match
from johnny.base import transactions as txnlib
from johnny.base.config import Account, Config
from johnny.base.etl import petl, Table
from johnny.sources import Source
from johnny.utils import timing


ZERO = Decimal(0)


def GetLatestFile(source: str) -> Optional[str]:
    """Given a globbing pattern, find the most recent file matching the pattern,
    based on the timestamp."""
    filenames = sorted(
        (path.getctime(filename), filename)
        for filename in glob.glob(source, recursive=True)
    )
    if not filenames:
        return None
    _, filename = last(filenames)
    return filename


def GetLatestFilePerYear(source: str) -> Dict[int, str]:
    """Given a globbing pattern, find the most recent file matching the pattern
    for each year, based on the prefix (for the year) and timestamp (for latest)."""
    yeardict = collections.defaultdict(list)
    for filename in glob.glob(source, recursive=True):
        match = re.match(r"(\d{4})\b", path.basename(filename))
        if match:
            key = int(match.group(1))
            yeardict[key].append(filename)
    return {
        year: max(filenames, key=path.getctime) for year, filenames in yeardict.items()
    }


def ReadInitialPositions(filename: str) -> Table:
    """Read a table of initial positions."""

    table = (
        petl.fromcsv(filename)
        .cut(
            "transaction_id",
            "datetime",
            "symbol",
            "instruction",
            "quantity",
            "cost",
            "commissions",
        )
        .addfield("order_id", lambda r: "o{}".format(r.transaction_id))
        .addfield("account", "")
        .convert("datetime", lambda v: parser.parse(v))
        .addfield("rowtype", "Open")
        .addfield("effect", "OPENING")
        .convert("quantity", Decimal)
        .convert("cost", Decimal)
        .addfield("price", lambda r: abs(r.cost / r.quantity))
        .addfield("cash", ZERO)
        .convert("commissions", Decimal)
        .addfield("fees", ZERO)
        .addfield("description", lambda r: "Opening balance for {}".format(r.symbol))
        .addfield("init", None)
        .cut(txnlib.FIELDS)
    )

    # Verify that the signs are correctly set.
    for rec in instrument.Expand(table, "symbol").records():
        sign = -1 if rec.instruction == "BUY" else +1
        cost = sign * rec.quantity * rec.multiplier * rec.price
        # Note: The cost is not expected to have commissions and fees included in it.
        if cost - rec.cost:
            raise ValueError(f"Invalid cost for {rec}: {cost} != {rec.cost}")

    # TODO(blais): Support multiplier in here. In the meantime, detect and fail
    # if present.
    if table.select(lambda r: r.symbol.startswith("/")).nrows() > 0:
        raise ValueError("Futures are not supported")
    return table


def _GetRegistry(source_name: str) -> Source:
    modules = {
        "ameritrade": "johnny.sources.thinkorswim_csv.source",
        "tastytrade": "johnny.sources.tastyworks_api.source",
        "interactive": "johnny.sources.interactive_csv.source",
    }
    module_name = modules[source_name]
    return importlib.import_module(module_name)


def _ImportAny(
    account: Account, method_name: str, read_initial_positions: bool
) -> Optional[Table]:
    # Incorporate initial positions.
    tables = []
    if read_initial_positions and account.initial_positions:
        table = ReadInitialPositions(account.initial_positions)
        if table is not None:
            tables.append(table.update("account", account.nickname))

    # Import module transactions.
    source_name = account.WhichOneof("source")
    config = getattr(account, source_name)
    module = _GetRegistry(source_name)
    func = getattr(module, method_name)
    table = func(config)
    if table is None:
        return None

    # Filter out instrument types.
    if account.exclude_instrument_types:
        exclude_instrument_types = set(
            configlib.InstrumentType.Name(instype)
            for instype in account.exclude_instrument_types
        )
        table = (
            table.applyfn(instrument.Expand, "symbol")
            .selectnotin("instype", exclude_instrument_types)
            .applyfn(instrument.Shrink)
        )

    if "account" in table.fieldnames():
        table = table.update("account", account.nickname)
    tables.append(table)

    return petl.cat(*tables)


def ImportTransactions(account: Account) -> Optional[Table]:
    """Import the transactions from an account definition."""
    table = _ImportAny(account, "ImportTransactions", True)
    return table.sort(["datetime", "account", "transaction_id"])


def ImportNonTrades(account: Account) -> Table:
    """Import the positions from an account definition."""
    return _ImportAny(account, "ImportNonTrades", False)


def ImportPositions(account: Account) -> Table:
    """Import the positions from an account definition."""
    table = _ImportAny(account, "ImportPositions", False)
    return table.sort(["account", "symbol"])


def _ValidateTransactions(transactions: Table):
    """Check that the imports are sound before we process them and ensure that
    the transaction ids are unique.
    """
    unique_ids = collections.defaultdict(int)
    num_txns = 0
    try:
        for rec in transactions.records():
            unique_ids[rec.transaction_id] += 1
            num_txns += 1
            txnlib.ValidateTransactionRecord(rec)
    except Exception as exc:
        if force:
            traceback.print_last()
        else:
            raise
    if num_txns != len(unique_ids):
        for key, value in unique_ids.items():
            if value > 1:
                print("Duplicate id '{}', {} times".format(key, value))
        raise AssertionError(
            "Transaction ids aren't unique: {} txns != {} txns".format(
                num_txns, len(unique_ids)
            )
        )


def ImportAllTransactions(config: Config, logger: Optional[logging.Logger]) -> Table:
    """Read all transactions, and do all necessary processing."""

    # Read the inputs.
    log = timing.create_logger(logger)
    tables = []
    for account in config.input.accounts:
        with log(f"ImportTransactions.read for {account.nickname}"):
            table = ImportTransactions(account)
            if table:
                tables.append(table)
    transactions = petl.cat(*tables)

    with log("ImportTransactions.validate"):
        _ValidateTransactions(transactions)

    # Match transactions to each other, synthesize opening balances, and mark
    # ending positions.
    with log("ImportTransactions.match"):
        return match.Process(transactions)


def ImportAllPositions(config: Config, logger: Optional[logging.Logger]) -> Table:
    """Read all positions, and do all necessary processing."""

    # Read the inputs.
    log = timing.create_logger(logger)
    tables = []
    for account in config.input.accounts:
        with log(f"ImportPositions.read for {account.nickname}"):
            table = ImportPositions(account)
            if table:
                tables.append(table)
    return petl.cat(*tables)


def ImportAllNonTrades(config: Config, logger: Optional[logging.Logger]) -> Table:
    """Read all non-trades, and do all necessary processing."""

    # Read the inputs.
    log = timing.create_logger(logger)
    tables = []
    for account in config.input.accounts:
        with log(f"ImportNonTrades.read for {account.nickname}"):
            table = ImportNonTrades(account)
            if table:
                tables.append(table)
    return petl.cat(*tables)
