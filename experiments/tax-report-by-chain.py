#!/usr/bin/env python3
"""Produce a report suitable for accountant/taxes.

TODO (extras):

- Find a way to check that the stocks reported haven't split an ensure correct
  cost basis (splits should be represented in the flow but still).

- Make sure ACTIVE chains don't have any closed bits to report.

- Detect mixed 1256 and normal trades.

"""

from decimal import Decimal
from functools import partial
from os import path
from typing import Any, List, Optional, Tuple, Mapping
import collections
import contextlib
import datetime as dt
import functools
import logging
import os
import pprint
import re
import shutil
import subprocess
import time
import traceback

from more_itertools import first
import click
import petl
import simplejson
import mulmat

from johnny.base import chains as chainslib
from johnny.base import config as configlib
from johnny.base import discovery
from johnny.base import instrument
from johnny.base import mark
from johnny.base import match
from johnny.base import transactions as txnlib
from johnny.base.etl import Table, Record, Replace
from johnny.utils import timing
from mulmat import multipliers

import tax_description


Q = Decimal("0.01")


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

    # if 0
    # final_verification(output_dir)
    # raise SystemExit

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
    chains = (
        chains.select(lambda c: not (c.maxdate < min_date or c.mindate > max_date))
        # Remove Roth IRA account.
        .selectne("account", "x20")
        # Remove open positions.
        .selectne("status", "ACTIVE").addfield(
            "category", functools.partial(categorize_chain, acc_map)
        )
    )

    # Filter the list of transactions from the chains.
    valid_chains = set(chains.values("chain_id"))
    txns = txns.selectin("chain_id", valid_chains)

    # Group each trade.
    detail_map, final_map = prepare_groups(txns, chains, min_date, max_date)

    # Identify wash sales (tightly).
    # TODO.

    # Write out to a spreadsheet.
    write_output(
        detail_map, path.join(output_dir, "detail"), None, chains=chains, txns=txns
    )
    write_output(final_map, path.join(output_dir, "final"), sheets_id)

    # Perform final checks on the ultimate files.
    final_verification(output_dir)


def rmtree_contents(directory: str):
    for root, dirs, files in os.walk(directory):
        for f in files:
            os.unlink(path.join(root, f))
        for d in dirs:
            shutil.rmtree(path.join(root, d), ignore_errors=True)
        break


def write_output(
    group_map: Mapping[str, List[Any]],
    output_dir: str,
    sheets_id: Optional[str],
    chains: Optional[petl.Table] = None,
    txns: Optional[petl.Table] = None,
):
    """Write out CSV files."""
    rmtree_contents(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    filenames = []

    def write_table(table: Table, shortname: str):
        fn = path.join(output_dir, shortname)
        logging.info(f"Producing {fn}")
        table.tocsv(fn)
        filenames.append(fn)

    if chains is not None:
        write_table(chains, "chains.csv")
    if txns is not None:
        write_table(txns, "transactions.csv")

    for group_name, rows in sorted(group_map.items()):
        if isinstance(rows, list):
            rows = petl.wrap(rows)
        assert isinstance(rows, Table)
        write_table(rows, f"{group_name}.csv")

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


TERM = {"TaxST": "ShortTerm", "TaxLT": "LongTerm"}


def categorize_chain(acc_map, chain) -> str:
    """Create a unique reporting category for this chain."""
    # Segment out investments
    # TODO(blais): Maybe generalized this?
    category = acc_map[chain.account]

    # Segment out Sec1256.
    underlyings = chain.underlyings.split(",")
    if all(instrument.IsSection1256(u) for u in underlyings):
        suffix = "_Sec1256"
    elif all(not instrument.IsSection1256(u) for u in underlyings):
        suffix = "_{}".format(TERM[chain.term])
    else:
        suffix = "_MIXED"
    category += suffix
    # category = ",".join(sorted(set(ctxns.values("account"))))
    return category


def prepare_groups(
    txns, chains, min_date: dt.date, max_date: dt.date
) -> Tuple[Mapping[str, List[Any]], Mapping[str, List[Any]]]:
    """Join chains and transactions and combine them."""

    db = mulmat.read_cme_database()
    contract_getter = tax_description.build_contract_getter(db, dt.date.today().year)

    all_matches = []

    # A mapping of category (sheet) to a list of prepared combined chains tables.
    txns_chain_map = petl.recordlookup(txns, "chain_id")
    detail_map = collections.defaultdict(list)
    final_map = collections.defaultdict(list)
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
            "term",
        )

        # Clean up transactions detail.
        ctxns = txns_chain_map.pop(chain.chain_id)
        ctxns = (
            petl.wrap([txns.fieldnames()] + ctxns)
            .movefield("chain_id", 0)
            .movefield("account", 1)
            .convert("chain_id", lambda _: "")
            .sort(["match_id", "datetime"])
        )

        # Append a list of aggregated matches for the purpose of reporting.
        funcs = {
            "date_opened": partial(date_sub, "OPENING"),
            "date_closed": partial(date_sub, "CLOSING"),
            "cost": cost_opened,
            "proceeds": cost_closed,
            "account": ("account", first),
            "symbol": ("symbol", lambda g: next(iter(set(g)))),
            "quantity": estimate_match_quantity,
        }
        mheaders = (
            "description",
            "date_opened",
            "date_closed",
            "cost",
            "proceeds",
            "pnl",
            # "chain_id",
            # "account",
            # "match_id",
            # "symbol",
        )
        cmatches = (
            ctxns.aggregate("match_id", funcs)
            .addfield("chain_id", chain.chain_id)
            .addfield("pnl", lambda r: (r.proceeds + r.cost).quantize(Q))
            .convert("proceeds", lambda v: v.quantize(Q))
            # Flip the signs on cost, so that pnl = proceeds - cost, not proceeds + cost.
            .convert("cost", lambda v: -v.quantize(Q))
            # Fetch a readable instrument description.
            .applyfn(instrument.Expand, "symbol")
            .addfield("description", partial(get_description, contract_getter))
            .applyfn(instrument.Shrink)
            .cut(*mheaders)
        )

        flds = cmatches.fieldnames()
        check_row = Record([""] * len(flds), flds)
        matches_cost = sum(cmatches.values("cost"))
        matches_proceeds = sum(cmatches.values("proceeds"))
        matches_pnl = matches_proceeds - matches_cost
        matches_pnl2 = sum(cmatches.values("pnl"))
        chain_pnl = chain.pnl_chain + chain.commissions + chain.fees
        check_row = (
            tuple(
                Replace(
                    check_row,
                    # chain_id="TOTAL",
                    cost=matches_cost,
                    proceeds=matches_proceeds,
                    pnl=matches_pnl,
                )
            )
            + (matches_pnl, chain_pnl)
        )
        for mpnl in [matches_pnl, matches_pnl2]:
            diff = abs(chain_pnl - mpnl)
            # Note: If you don't quantize these match perfectly at 1 penny
            if diff > Decimal("0.05"):
                raise ValueError(f"Invalid matches_pnl for {chain}: {diff}")

        # Form 8949
        # (a) Description of property (Example: 100 sh. XYZ Co.)
        # (b) Date acquired (Mo., day, yr.)
        # (c) Date sold or disposed of (Mo., day, yr.)
        # (d) Proceeds (sales price) (see instructions)
        # (e) Cost or other basis. See the Note below and see Column (e) in the separate instructions Adjustment, if any, to gain or loss. If you enter an amount in column (g), enter a code in column (f). See the separate instructions.
        # (f) Code(s) from instructions
        # (g) Amount of adjustment
        # (h) Gain or (loss). Subtract column (e) from column (d) and combine the result with column (g)

        # Form 6781
        # (a) Description of property
        # (b) Date entered into or acquired
        # (c) Date closed out or sold
        # (d) Gross sales price
        # (e) Cost or other basis plus expense of sale
        # (f) Loss. If column (e) is more than (d), enter difference. Otherwise, enter -0-.
        # (g) Unrecognized gain on offsetting positions
        # (h) Recognized loss. If column (f) is more than (g), enter difference. Otherwise, enter -0

        # Append a combined table to a list.
        rows = list(cchains) + list(ctxns) + list(cmatches) + [check_row, [], []]
        detail_map[chain.category].extend(rows)
        final_map[chain.category].append(cmatches)

    for key, tables in final_map.items():
        table = petl.cat(*tables)
        final_map[key] = petl.cat(*tables)

    return detail_map, final_map


def date_sub(effect: str, rows: List[Record]) -> str:
    dtimes = set([r.datetime.date() for r in rows if r.effect == effect])
    if len(dtimes) > 1:
        return "Various"
    else:
        return next(iter(dtimes)).isoformat()


def estimate_match_quantity(rows: List[Record]) -> int:
    return sum(r.quantity for r in rows if r.effect == "OPENING")


def get_description(contract_getter, r: Record) -> str:
    description = tax_description.get_description(contract_getter, r)
    match_id = r.match_id.strip("&")
    return f"{r.quantity} {description} (id:{match_id})"


def cost_opened(rows: List[Record]) -> str:
    return sum((r.cost + r.commissions + r.fees) for r in rows if r.effect == "OPENING")


def cost_closed(rows: List[Record]) -> str:
    return sum((r.cost + r.commissions + r.fees) for r in rows if r.effect == "CLOSING")


def final_verification(output_dir: str):
    """Final verifications off the generated files.
    Just to be extra sure."""

    txns = (
        petl.fromcsv(path.join(output_dir, "detail/transactions.csv"))
        .cut("cost", "commissions", "fees")
        .convertall(Decimal)
    )
    txns_pnl = (
        sum(txns.values("cost"))
        + sum(txns.values("commissions"))
        + sum(txns.values("fees"))
    )
    print("transactions_pnl: {}".format(txns_pnl))

    final_dir = path.join(output_dir, "final")
    pnl_map = {}
    for filename in os.listdir(final_dir):
        pnl_map[filename] = (
            petl.fromcsv(path.join(final_dir, filename))
            .convert("pnl", Decimal)
            .values("pnl")
            .sum()
        )
    pprint.pprint(pnl_map)
    print("breakdowns_pnl: {}".format(sum(pnl_map.values())))

    lt_pnl = sum(value for key, value in pnl_map.items() if re.match(".*LongTerm", key))
    st_pnl = sum(
        value for key, value in pnl_map.items() if not re.match(".*LongTerm", key)
    )
    print("LongTerm pnl: {}".format(lt_pnl))
    print("Trading pnl: {}".format(st_pnl))


if __name__ == "__main__":
    main()
