#!/usr/bin/env python3
"""Produce a report suitable for accountant/taxes.
"""

import contextlib
import collections
import functools
import logging
import datetime as dt
import re
import traceback
import time
import shutil
import subprocess
import os
from os import path
from typing import Any, List, Optional, Tuple, Mapping

import click
import simplejson
import petl

from johnny.base import chains as chainslib
from johnny.base import config as configlib
from johnny.base import discovery
from johnny.base import instrument
from johnny.base import match
from johnny.base import mark
from johnny.base import transactions as txnlib
from johnny.base.etl import Table
from johnny.utils import timing

from mulmat import multipliers


# TODO(blais): Move this to Mulmat.
def is_index_option(symbol: str) -> bool:
    """Return true if this is an index optin."""
    return symbol in multipliers.CBOE_MULTIPLIERS


# TODO(blais): Move this to Mulmat.
def is_section_1256(symbol: str) -> bool:
    """Return true if this is a section 1256 instrument."""
    inst = instrument.FromString(symbol)
    return inst.instype in {"Future", "FutureOption"} or is_index_option(
        inst.underlying
    )


@click.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    help="Configuration filename. Default to $JOHNNY_CONFIG",
)
@click.option(
    "--sheets-id",
    "-i",
    help="Id of sheets doc to write the final output to",
)
@click.option(
    "--output-dir",
    "-o",
    default="/tmp/taxes-trades",
    help="Id of sheets doc to write the final output to",
)
def main(config: Optional[str], sheets_id: Optional[str], output_dir: str):
    logging.basicConfig(level=logging.INFO, format="%(levelname)-8s: %(message)s")

    # Read the input configuration.
    filename = configlib.GetConfigFilenameWithDefaults(config)
    config = configlib.ParseFile(filename)

    # Read imported transactions and chains.
    txns = petl.frompickle(config.output.transactions)
    chains = petl.frompickle(config.output.chains)

    # Filter and categorize the chains.
    min_date = dt.date(2021, 1, 1)
    max_date = dt.date(2022, 1, 1)

    acc_map = {acc.nickname: acc.sheetname for acc in config.input.accounts}
    chains = chains.select(
        lambda c: not (c.maxdate < min_date or c.mindate > max_date)
    ).addfield("category", functools.partial(categorize_chain, acc_map))

    # Group each trade.
    group_map = prepare_groups(txns, chains, min_date, max_date)

    # Make sure the cost basis is right (!) for LT investments.
    # Make sure the start date is right for LT.
    # Split up LT from CC positions (account for them differently).
    # TODO

    # Remove open positions.
    # TODO

    # Identify wash sales (tightly, like TradeLog).

    # Write out to a spreadsheet.
    write_output(chains, txns, group_map, output_dir, sheets_id)


def write_output(
    chains: petl.Table,
    txns: petl.Table,
    group_map: Mapping[str, List[Any]],
    output_dir: str,
    sheets_id: Optional[str],
):
    """Write out CSV files."""
    shutil.rmtree(output_dir, ignore_errors=True)
    os.makedirs(output_dir)

    filenames = []

    def write_table(table: Table, shortname: str):
        fn = path.join(output_dir, shortname)
        logging.info(f"Producing {fn}")
        table.tocsv(fn)
        filenames.append(fn)

    write_table(chains, "chains.csv")
    write_table(txns, "transactions.csv")

    for group_name, rows in sorted(group_map.items()):
        write_table(petl.wrap(rows), f"{group_name}.csv")

    if isinstance(sheets_id, str):
        logging.info("Creating sheets doc")
        title = "Trades by Types"
        command = ["upload-to-sheets", "-v", f"--title={title}"]
        if sheets_id:
            command.append(f"--id={sheets_id}")
        subprocess.check_call(command + filenames)


def make_chain_filter(min_date, max_date):
    """Filter the trade chains."""

    # Remove trades entirely out of our interval.
    tim = dt.time()
    min_datetime = dt.datetime.combine(min_date, tim)
    max_datetime = dt.datetime.combine(max_date, tim)

    def filter_chain(chain):
        year_datetimes = [
            ctxn.datetime
            for ctxn in ctxns.records()
            if min_datetime <= ctxn.datetime < max_datetime
        ]
        if not year_datetimes:
            return False

        if chain.status == "ACTIVE":
            raise ValueError
        return True

    return filter_chain


def categorize_chain(acc_map, chain) -> str:
    """Create a unique reporting category for this chain."""
    # Segment out investments
    # TODO(blais): Maybe generalized this?
    if re.match("Invest.*", chain.group):
        category = "LT_Investments"
    else:
        category = acc_map[chain.account]

    # Segment out Sec1256.
    underlyings = chain.underlyings.split(",")
    if all(is_section_1256(u) for u in underlyings):
        suffix = "_sec1256"
    elif all(not is_section_1256(u) for u in underlyings):
        suffix = ""
    else:
        suffix = "_mixed"
    category += suffix
    # category = ",".join(sorted(set(ctxns.values("account"))))
    return category


def prepare_groups(
    txns, chains, min_date: dt.date, max_date: dt.date
) -> Mapping[str, List[Any]]:
    """Join chains and transactions and combine them."""

    # A mapping of category (sheet) to a list of prepared combined chains tables.
    txns_chain_map = petl.recordlookup(txns, "chain_id")
    group_map = collections.defaultdict(list)
    for chain in chains.records():
        # Pretty up chains row.
        cchains = petl.wrap([chains.fieldnames(), chain]).cut(
            "chain_id",
            "account",
            "mindate",
            "maxdate",
            "days",
            "group",
            "underlyings",
            "status",
            "init_legs",
            "init",
            "pnl_chain",
            "net_liq",
            "commissions",
            "fees",
            "strategy",
        )

        # Clean up transactions detail.
        ctxns = txns_chain_map.pop(chain.chain_id)
        ctxns = (
            petl.wrap([txns.fieldnames()] + ctxns)
            .movefield("chain_id", 0)
            .movefield("account", 1)
            .convert("chain_id", lambda _: "")
            .cutout("match_id")
        )

        # Append a combined table to a list.
        rows = list(cchains) + list(ctxns)
        accounts_list = group_map[chain.category]
        accounts_list.extend(rows)
        accounts_list.append([])

    return group_map


if __name__ == "__main__":
    main()
