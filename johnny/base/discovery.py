"""Find the files to form the database.

We rely on patterns and dates and sorting to sieve the latest files of every
type in a bungled directory of downloads. For now.
"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from decimal import Decimal
from os import path
from typing import Any, Callable, Dict, Optional, Set
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
from johnny.base import nontrades as nontradeslib
from johnny.base import positions as poslib
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
    chosen_yeardict = {
        year: max(filenames) for year, filenames in yeardict.items()
    }
    ## import pprint; pprint.pprint(chosen_yeardict)
    return chosen_yeardict


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
        .addfield("rowtype", txnlib.Type.Open)
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
        "ameritrade": "johnny.sources.ameritrade.source",
        "tastytrade": "johnny.sources.tastytrade.source",
        "interactive": "johnny.sources.interactive.source",
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
    table = table.sort(["datetime", "account", "transaction_id"])
    txnlib.Validate(table)
    return table


def ImportNonTrades(account: Account) -> Table:
    """Import the positions from an account definition."""
    table = _ImportAny(account, "ImportNonTrades", False)
    nontradeslib.Validate(table)
    return table


def ImportPositions(account: Account) -> Table:
    """Import the positions from an account definition."""
    table = _ImportAny(account, "ImportPositions", False)
    poslib.Validate(table)
    return table.sort(["account", "symbol"])


def _ImportAllAccounts(
    process_func: Callable[[Table], Table],
    config: Config,
    logger: Optional[logging.Logger],
) -> Table:
    """Import all accounts, concatenate and process with a given function."""
    # Read the inputs.
    log = timing.create_logger(logger)
    tables = []
    for account in config.input.accounts:
        with log(f"{process_func.__name__} for {account.nickname}"):
            table = process_func(account)
            if table:
                tables.append(table)
    return petl.cat(*tables)


def ImportAllTransactions(config: Config, logger: Optional[logging.Logger]) -> Table:
    """Read all transactions, and do all necessary processing."""
    table = _ImportAllAccounts(ImportTransactions, config, logger)

    # Match transactions to each other, synthesize opening balances, and mark
    # ending positions.
    log = timing.create_logger(logger)
    with log("match"):
        return match.Process(table)


def ImportAllPositions(config: Config, logger: Optional[logging.Logger]) -> Table:
    """Read all positions, and do all necessary processing."""
    return _ImportAllAccounts(ImportPositions, config, logger)



def ImportAllNonTrades(config: Config, logger: Optional[logging.Logger]) -> Table:
    """Read all non-trades, and do all necessary processing."""
    return _ImportAllAccounts(ImportNonTrades, config, logger)
