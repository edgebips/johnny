"""Find the files to form the database.

We rely on patterns and dates and sorting to sieve the latest files of every
type in a bungled directory of downloads. For now.
"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from decimal import Decimal
from os import path
from typing import Any, Dict, Optional
import collections
import glob
import importlib

from more_itertools import last
from dateutil import parser

from johnny.base.etl import petl, Table
from johnny.base import instrument
from johnny.base import transactions as txnlib
from johnny.base import config as configlib


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
        .convert("commissions", Decimal)
        .addfield("fees", ZERO)
        .addfield("description", lambda r: "Opening balance for {}".format(r.symbol))
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


def ReadConfiguredInputs(
    config: configlib.Config, logtype: Optional[Any] = None
) -> Dict[int, Table]:
    """Read the explicitly configured inputs in the config file.
    Returns tables for the transactions and positions."""

    # Parse and accumulate by log type.
    tablemap = collections.defaultdict(list)
    for account in config.input.accounts:
        if logtype is not None and logtype != account.logtype:
            continue
        output_tables = tablemap[account.logtype]

        # Incorporate initial positions.
        if account.initial:
            table = ReadInitialPositions(account.initial)
            if table is not None:
                output_tables.append(table.update("account", account.nickname))

        # Import module transactions.
        module = importlib.import_module(account.module)
        table = module.Import(account.source, config)
        if table is None:
            continue

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

        output_tables.append(table.update("account", account.nickname))

    # Concatenate tables for each logtype.
    bytype = {}
    for t_logtype, tables in tablemap.items():
        table = petl.cat(*tables)
        if t_logtype == configlib.Account.LogType.TRANSACTIONS:
            bytype[t_logtype] = table.sort(("account", "datetime"))
        elif t_logtype == configlib.Account.LogType.POSITIONS:
            bytype[t_logtype] = table.sort(("account", "symbol"))

    return bytype
