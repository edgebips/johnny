#!/usr/bin/env python3
"""Produce a report suitable for accountant/taxes.

    Form 8949
    (a) Description of property (Example: 100 sh. XYZ Co.)
    (b) Date acquired (Mo., day, yr.)
    (c) Date sold or disposed of (Mo., day, yr.)
    (d) Proceeds (sales price) (see instructions)
    (e) Cost or other basis. See the Note below and see Column (e) in the
        separate instructions Adjustment, if any, to gain or loss. If you
        enter an amount in column (g), enter a code in column (f). See the
        separate instructions.
    (f) Code(s) from instructions
    (g) Amount of adjustment
    (h) Gain or (loss). Subtract column (e) from column (d) and combine
        the result with column (g)

    Form 6781
    (a) Description of property
    (b) Date entered into or acquired
    (c) Date closed out or sold
    (d) Gross sales price
    (e) Cost or other basis plus expense of sale
    (f) Loss. If column (e) is more than (d), enter difference. Otherwise,
        enter -0-.
    (g) Unrecognized gain on offsetting positions
    (h) Recognized loss. If column (f) is more than (g), enter difference.
        Otherwise, enter -0


TODO (extras):

- Find a way to check that the stocks reported haven't split an ensure correct
  cost basis (splits should be represented in the flow but still).

- Make sure ACTIVE chains don't have any closed bits to report.

- Detect mixed 1256 and normal trades.

"""

from decimal import Decimal
from functools import partial
from os import path
from typing import Any, Dict, List, Optional, Tuple, Mapping
import itertools
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
import pandas as pd
import numpy as np

from johnny.base import chains as chainslib
from johnny.base import config as configlib
from johnny.base import discovery
from johnny.base import instrument
from johnny.base import mark
from johnny.base import match
from johnny.base import transactions as txnlib
from johnny.base.etl import Table, Record, Replace
from johnny.base.number import ToDecimal
from johnny.utils import timing
from mulmat import multipliers

import tax_description


ZERO = Decimal(0)
Q = Decimal("0.01")
Grouper = itertools._grouper


@click.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    help="Configuration filename. Default to $JOHNNY_CONFIG",
)
@click.option(
    "--output-dir",
    "-o",
    default="/tmp/taxes-trades",
    help="Output directory root",
)
@click.option(
    "--include-roth",
    is_flag=True,
    help="Include Roth and check for wash sales to it.",
)
@click.option(
    "--summaries-dir",
    default=path.join(path.dirname(os.getenv("L")), "trading/taxes"),
    help="Directory where summaries for verification can be found.",
)
def main(
    config: Optional[str],
    output_dir: str,
    include_roth: bool,
    summaries_dir: str,
):
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
    chains = (
        chains.select(lambda c: not (c.maxdate < min_date or c.mindate > max_date))
        # Remove open positions.
        .selectne("status", "ACTIVE")
    )
    if not include_roth:
        # Remove Roth IRA account.
        chains = chains.selectne("account", "x20")

    # Filter the list of transactions from the chains.
    valid_chains = set(chains.values("chain_id"))
    txns = txns.selectin("chain_id", valid_chains)

    # Group each trade.
    acc_map = {acc.nickname: acc.sheetname for acc in config.input.accounts}
    bychain_map, final_map, matches = prepare_groups(
        txns, chains, min_date, max_date, acc_map
    )

    if not include_roth:
        # Identify wash sales (tightly).
        identify_wash_sales(chains, matches)

    # Write out to a spreadsheet.
    write_outputs(
        bychain_map,
        path.join(output_dir, "detail"),
        chains=chains,
        transactions=txns,
        matches=matches,
    )

    # Compare our calculations against manually picked summaries.
    cat_summary = matches.aggregate(
        "category",
        {
            "proceeds": ("proceeds", sum),
            "cost": ("cost", sum),
            "gain_loss": ("gain_loss", sum),
        },
    )
    cat_instype_summary = matches.applyfn(instrument.Expand, "symbol").aggregate(
        ["category", "instype"],
        {
            "proceeds": ("proceeds", sum),
            "cost": ("cost", sum),
            "gain_loss": ("gain_loss", sum),
        },
    )
    validate_numbers_against_manual(
        matches, cat_summary, cat_instype_summary, summaries_dir
    )

    write_outputs(
        final_map,
        path.join(output_dir, "final"),
        summary=cat_summary,
        summary_instype=cat_instype_summary,
    )

    # Perform final checks on the ultimate files.
    final_verification(output_dir)


def rmtree_contents(directory: str):
    for root, dirs, files in os.walk(directory):
        for f in files:
            os.unlink(path.join(root, f))
        for d in dirs:
            shutil.rmtree(path.join(root, d), ignore_errors=True)
        break


def write_outputs(
    group_map: Mapping[str, List[Any]],
    output_dir: str,
    **extra_tables: Dict[str, petl.Table],
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

    for name, xtable in extra_tables.items():
        write_table(xtable, f"{name}.csv")

    for group_name, rows in sorted(group_map.items()):
        if isinstance(rows, list):
            rows = petl.wrap(rows)
        assert isinstance(rows, Table)
        write_table(rows, f"{group_name}.csv")

    # if isinstance(sheets_id, str):
    #     logging.info("Creating sheets doc")
    #     title = "Trades by Types"
    #     command = ["upload-to-sheets", "-v", f"--title={title}"]
    #     if sheets_id:
    #         command.append(f"--id={sheets_id}")
    #     subprocess.check_call(command + filenames)


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


def categorize_chain(acc_map, row: Record) -> str:
    """Create a unique reporting category for this chain."""
    # Segment out Sec1256.
    if instrument.IsSection1256(row.instype, row.underlying):
        suffix = "Sec1256"
    elif instrument.IsCollectible(row.instype, row.underlying):
        suffix = "Collectible"
    else:
        # Note: Right now the LT/ST nature of the trade is manual.
        suffix = TERM[row.term]
    category = "{}_{}".format(acc_map[row.account], suffix)
    return category


def prepare_groups(
    txns, chains, min_date: dt.date, max_date: dt.date, acc_map: Mapping[str, str]
) -> Tuple[Mapping[str, List[Any]], Mapping[str, List[Any]]]:
    """Join chains and transactions and combine them."""

    db = mulmat.read_cme_database()
    contract_getter = tax_description.build_contract_getter(db, dt.date.today().year)

    # Sort by (und, date).
    sorted_chains = chains.sort(["underlyings", "chain_id", "mindate"])

    # A mapping of category (sheet) to a list of prepared combined chains tables.
    txns_chain_map = petl.recordlookup(txns, "chain_id")
    bychain_map = collections.defaultdict(list)
    final_map = collections.defaultdict(list)
    for chain in sorted_chains.records():
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
        cmatches = get_chain_matches_from_transactions(
            petl.wrap([txns.fieldnames()] + ctxns)
        )
        cmatches = (
            cmatches
            # Fetch a readable instrument description.
            .applyfn(instrument.Expand, "symbol")
            .addfield(
                "sec1256", lambda r: instrument.IsSection1256(r.instype, r.underlying)
            )
            .addfield("description", partial(get_description, contract_getter), index=0)
            # Categorize the chain based on the instruments traded in it. In
            # particular, this detects long-term and short-term, and section
            # 1256 or not.
            .addfield("term", chain.term)
            .addfield("category", lambda r: categorize_chain(acc_map, r))
            .applyfn(instrument.Shrink, "instype", "underlying")
            .addfield("*", "  ", index=6)
            .cutout("quantity", "account", "sec1256")
            .movefield("underlying", 9)
        )

        # Find the single category for all the matches. The chains configuration
        # is assumed to split chains into subchains with a single category.
        # Eventually this could be automated, but we impose this constraint
        # here.
        categories = set(cmatches.values("category"))
        if len(categories) != 1:
            raise ValueError(
                f"Match in chain {chain.chain_id} has non-unique categories: {categories}"
            )
        category = next(iter(categories))

        # Check P/L against chain.
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
        cmatches = cmatches.rename({"pnl": "gain_loss"})

        # Append a combined table to a list.
        rows = list(cchains) + list(ctxns) + list(cmatches) + [check_row, [], []]
        bychain_map[category].extend(rows)
        final_map[category].append(cmatches)

    for key, tables in final_map.items():
        table = petl.cat(*tables)
        final_map[key] = petl.cat(*tables)

    all_matches = petl.cat(*final_map.values())

    return bychain_map, final_map, all_matches


def get_chain_matches_from_transactions(txns: Table) -> Table:
    chain_id = next(iter(txns.values("chain_id")))
    ctxns = (
        txns.movefield("chain_id", 0)
        .movefield("account", 1)
        .convert("chain_id", lambda _: "")
        .sort(["match_id", "datetime"])
    )

    # Append a list of aggregated matches for the purpose of reporting.
    funcs = {
        "date_acquire": partial(date_sub, "OPENING"),
        "date_disposed": partial(date_sub, "CLOSING"),
        "date_min": ("datetime", lambda g: min(g).date()),
        "date_max": ("datetime", lambda g: max(g).date()),
        "cost": cost_opened,
        "proceeds": cost_closed,
        "futures_notional_open": futures_notional_open,
        "account": ("account", first),
        "symbol": ("symbol", lambda g: next(iter(set(g)))),
        "quantity": estimate_match_quantity,
    }
    cmatches = ctxns.applyfn(instrument.Expand, "symbol").aggregate("match_id", funcs)

    # For futures contracts, remove notional value from cost and add the
    # corresponding notional to proceeds. Opening futures positions should
    # have 0 cost (excluding commissions and fees) and closing positions
    # should be the matched P/L. This should produce proceeds and cost
    # numbers much closer to those on the 1099s.
    denotionalize_futures = True
    if denotionalize_futures:
        cmatches = cmatches.convert(
            "cost",
            lambda _, r: r.cost - r.futures_notional_open,
            pass_row=True,
        ).convert(
            "proceeds",
            lambda _, r: r.proceeds + r.futures_notional_open,
            pass_row=True,
        )

    cmatches = (
        cmatches.addfield("chain_id", chain_id)
        .addfield("pnl", lambda r: (r.proceeds + r.cost).quantize(Q))
        .convert("proceeds", lambda v: v.quantize(Q))
        # Flip the signs on cost, so that pnl = proceeds - cost, not proceeds + cost.
        .convert("cost", lambda v: -v.quantize(Q))
    )

    # Handle P/L specially on short options.
    short_options_method = "none"
    if short_options_method == "invert":
        cmatches = short_options_invert(cmatches)
    elif short_options_method == "nullify":
        cmatches = short_options_nullify(cmatches)

    return cmatches.cut(
        # "description",
        "date_acquire",
        "date_disposed",
        "cost",
        "proceeds",
        "pnl",
        # "*",
        "match_id",
        "symbol",
        "quantity",
        "date_min",
        "date_max",
        # "instype",
        # "underlying",
        "account",
        # "category",
    )


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
    # return f"{r.quantity} {description} (id:{match_id})"
    return f"{r.quantity} {description}"


def cost_opened(rows: List[Record]) -> str:
    return sum((r.cost + r.commissions + r.fees) for r in rows if r.effect == "OPENING")


def cost_closed(rows: List[Record]) -> str:
    return sum((r.cost + r.commissions + r.fees) for r in rows if r.effect == "CLOSING")


def futures_notional_open(rows: List[Record]) -> Decimal:
    return sum(r.cost for r in rows if r.instype == "Future" and r.effect == "OPENING")


def identify_wash_sales(chains: Table, matches: Table):
    """Try to identify potential wash sales across accounts."""
    smatches = matches.aggregate("und", check_wash_und)
    # print(smatches.selectne("value", ZERO).lookallstr())
    # print(smatches.values("value").sum())


def short_options_nullify(matches: Table) -> Table:
    """
    On short options sales, nullify the cost basis as per the following rule:
    https://support.tastyworks.com/support/solutions/articles/43000615420--0-00-cost-basis-on-short-equity-options-
    """
    return (
        matches.addfield(
            "null",
            lambda r: (
                r.instype == "EquityOption" and r.cost <= ZERO and r.proceeds <= ZERO
            ),
        )
        .addfield("offset", lambda r: -r.cost)
        .addfield("cost_null", lambda r: r.cost + offset if r.null else r.cost)
        .addfield(
            "proceeds_null", lambda r: r.proceeds + offset if r.null else r.proceeds
        )
        .cutout("null", "cost", "proceeds")
        .rename({"cost_null": "cost", "proceeds_null": "proceeds"})
    )


def short_options_invert(matches: Table) -> Table:
    """
    Invert sell-to-open then buy-to-close in order to have positive
    numbers. We swap the dates, swap cost and proceeds and swap the signs
    on them. The P/L should be the same.
    """
    return (
        matches.addfield("inv", lambda r: r.cost <= ZERO and r.proceeds <= ZERO)
        .addfield("cost_inv", lambda r: -r.proceeds if r.inv else r.cost)
        .addfield("proceeds_inv", lambda r: -r.cost if r.inv else r.proceeds)
        .cutout("inv", "cost", "proceeds")
        .rename({"cost_inv": "cost", "proceeds_inv": "proceeds"})
    )


def invert_sales(matches: Table) -> Table:
    """
    Invert sell-to-open then buy-to-close in order to have positive
    numbers. We swap the dates, swap cost and proceeds and swap the signs
    on them. The P/L should be the same.
    """
    irows = []
    for row in matches.records():
        if row.cost <= ZERO and row.proceeds <= ZERO:
            row = Replace(
                row,
                cost=-row.proceeds,
                proceeds=-row.cost,
                date_acquire=row.date_disposed,
                date_disposed=row.date_acquire,
            )
        irows.append(row)
    matches = petl.wrap([matches.fieldnames()] + irows)


def check_wash_und(g: Grouper):
    """Check wash sales across a single und."""

    # Find contiguous regions with overlaps.
    intervals = []
    rows = list(g)
    for r in rows:
        non_overlapping = []
        dates = [r.date_min, r.date_max]
        for index, interval in enumerate(intervals):
            (int_min, int_max) = interval
            if int_max < r.date_min or r.date_max < int_min:
                non_overlapping.append(interval)
            else:
                dates.append(int_min)
                dates.append(int_max)
        intervals = non_overlapping
        intervals.append((min(dates), max(dates)))

    # Compress if <= 31 days.
    int_iter = iter(sorted(intervals, key=lambda i: i[0]))
    joined_intervals = []
    prev_min, prev_max = next(int_iter)
    threshold = dt.timedelta(days=31)
    for int_min, int_max in int_iter:
        if int_min - prev_max > threshold:
            joined_intervals.append((prev_min, prev_max))
            prev_min, prev_max = int_min, int_max
        else:
            prev_max = int_max
    joined_intervals.append((prev_min, prev_max))

    # Look for cross-account occurences.
    total_disallowed = ZERO
    for int_min, int_max in joined_intervals:
        int_rows = []
        categories = set()
        for r in rows:
            if re.search("Sec1256", r.category):
                continue
            overlapping = not (int_max < r.date_min or r.date_max < int_min)
            if overlapping:
                int_rows.append(r)
                categories.add(r.category)

        if len(categories) > 1 and any(re.search("Roth", c) for c in categories):
            # Sum up non-Roth losses as they might be disallowed?
            disallowed = ZERO
            for r in int_rows:
                if not re.search("Roth", r.category) and r.pnl < 0:
                    disallowed += r.pnl
            total_disallowed += disallowed
            if 0:
                print(int_min, int_max)
                for r in int_rows:
                    print("  {}".format(r))
                print(f"disallowed: {disallowed}")
                print()

    return total_disallowed


def validate_numbers_against_manual(
    matches: Table,
    cat_summary: Table,
    cat_instype_summary: Table,
    summaries_dir: str,
) -> Tuple[Table, Table]:

    df_man_summary = (
        petl.fromcsv(path.join(summaries_dir, "summary-from-1099.csv"))
        .cutout("wash_sales", "wash_gain_loss")
        .convert(["proceeds", "cost", "gain_loss"], ToDecimal)
        .select("category", lambda v: bool(v) and not re.match("Note:", v))
        .todataframe()
        .set_index("category")
        .sort_index()
    )
    df_man_summary_instype = (
        petl.fromcsv(path.join(summaries_dir, "summary-instype-from-1099.csv"))
        .convert(["proceeds", "cost", "gain_loss"], ToDecimal)
        .todataframe()
        .sort_index()
    )

    # Produce aggregate reports of proceeds and cost for each category for 1099
    # cross-checking.
    df_cat_summary = cat_summary.todataframe().set_index("category")
    print()
    print("From 1099's")
    print(df_man_summary)
    print()
    print("From 'Johnny', my own system")
    print(df_cat_summary)
    df_diff_summary = df_cat_summary - df_man_summary
    df_diff_summary[df_man_summary == 0] = "?"
    # df_pct_summary = df_diff_summary / df_man_summary.max(0.0001)
    print()
    print("Difference")
    print(df_diff_summary)
    # print(df_pct_summary)

    print()
    print("Total")
    df_man_sum = df_man_summary.sum()
    df_cat_sum = df_cat_summary.sum()
    df = pd.DataFrame([df_man_sum, df_cat_sum, df_man_sum - df_cat_sum])
    print(df)

    # print()
    df_cat_instype_summary = cat_instype_summary.todataframe()
    # # print(df_man_instype_summary)
    # print(df_cat_instype_summary)

    return df_cat_summary, df_cat_instype_summary


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
        cat_table = petl.fromcsv(path.join(final_dir, filename))
        pnl_map[filename] = (
            cat_table.rename({"gain_loss": "pnl"})
            .convert("pnl", Decimal)
            .values("pnl")
            .sum()
        )
    # pprint.pprint(pnl_map)
    # print("Breakdowns pnl: {}".format(sum(pnl_map.values())))

    lt_pnl = sum(value for key, value in pnl_map.items() if re.match(".*LongTerm", key))
    st_pnl = sum(
        value for key, value in pnl_map.items() if not re.match(".*LongTerm", key)
    )
    # print("LongTerm pnl: {}".format(lt_pnl))
    # print("Trading pnl: {}".format(st_pnl))


if __name__ == "__main__":
    main()
