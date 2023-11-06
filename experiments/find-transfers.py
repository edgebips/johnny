#!/usr/bin/env python3
"""Find Beancount transfers to particular accounts.

This can be used to create the table of transfer sources/destinations.

This script will look at all rows with InternalTransfer and ExternalTransfer
rowtypes and for each, try to find a matching transaction from the Beancount
ledger that could match it. You then manually insert the links to certify the
mapping of those transfers to your other accounts. Johnny will not itself issue
the transfer transactions (it would be impossible to do right, since there is
missing information about the source/destination; that information lives on the
Beancount side only).
"""

from functools import partial
from pprint import pprint
from typing import Optional
import argparse
import datetime as dt
import dateutil.parser
import os
import re
import sys

from beancount import loader
import beancount.api as bn

from johnny.base.etl import petl, Record, WrapRecords
from johnny.base import config as configlib


def FindTransfers(
    ledger: str,
    account_regex: str,
    start_date: Optional[dt.date],
    end_date: Optional[dt.date],
):
    entries, errors, options_map = loader.load_file(ledger)
    acctypes = bn.get_account_types(options_map)

    transfers = []
    for entry in bn.filter_txns(entries):
        if start_date and entry.date < start_date:
            continue
        if end_date and entry.date >= end_date:
            break

        # At least one matching account in USD and at least two different asset
        # accounts in USD.
        usd_postings = [
            posting for posting in entry.postings if posting.units.currency == "USD"
        ]
        matches = [
            posting
            for posting in usd_postings
            if (re.match(account_regex, posting.account))
        ]
        assets = set(
            [
                posting.account
                for posting in usd_postings
                if bn.is_account_type(acctypes.assets, posting.account)
            ]
        )
        is_already_mapped = any(
            re.match("johnny-(.*)", link) for link in sorted(entry.links)
        )
        if is_already_mapped or (matches and len(assets) >= 2):
            transfers.append(entry)

    if 0:
        for entry in transfers:
            print("-" * 200)
            print(f"{entry.meta['filename']}:{entry.meta['lineno']}:")
            print(bn.format_entry(entry))
            print()

    return transfers


def FindMatches(
    rec: Record, btransfers: list[bn.dtypes.Transaction]
) -> bn.dtypes.Transaction:
    """Find a matching Beancount transaction to a Johnny transfer."""

    max_days = 2
    jdate = rec.datetime.date()
    matches = [
        entry
        for entry in btransfers
        if (
            abs((entry.date - jdate).days) <= max_days
            and any(posting.units.number == rec.amount for posting in entry.postings)
        )
    ]
    return matches


def GetLocation(rec: Record, entry_map: dict[str, bn.dtypes.Transaction]) -> str:
    """Find and render the location of a matching entry."""

    entry = entry_map.get(rec.transaction_id)
    if entry:
        meta = entry.meta
        return "{}:{}:".format(meta["filename"], meta["lineno"])


def ModifyFiles():
    # TODO(blais): Just copied here, unused, probably delete this.

    # Insert the transaction-id as a special link in the original file.
    link_map = {}
    for date, number, account, entry in btransfers:
        meta = entry.meta
        assert meta["filename"] == args.ledger
        matches = (
            jtransfers.selecteq("amount", number)
            .addfield("datediff", lambda r: abs(r.datetime.date() - date))
            .selectle("datediff", dt.timedelta(days=5))
            .sort("datediff")
            .cutout("datediff")
        )
        nrows = matches.nrows()
        assert nrows in {0, 1}
        if nrows == 1:
            link_map[meta["lineno"]] = matches.head(1)
    lines = list(enumerate((open(args.ledger).readlines()), start=1))
    for index, line in lines:
        table = link_map.get(index)
        if table:
            rec = next(iter(table.records()))
            output = "\n".join(
                [f";; {line}" for line in str(table.lookallstr()).splitlines()]
            )
            line = (
                ";; Johnny transfer\n"
                + output
                + "\n"
                + line.rstrip()
                + f" ^johnny-{rec.transaction_id}\n"
            )
        sys.stdout.write(line)


def main():
    parser = argparse.ArgumentParser(description=__doc__.strip())
    parser.add_argument(
        "-f",
        "--ledger",
        default=os.environ["L"],
        help="Ledger",
    )
    parser.add_argument(
        "account",
        help="Account regex against which to test for transfers",
    )
    parser.add_argument(
        "--start-date",
        help="Mininmum date",
        type=lambda s: dateutil.parser.parse(s).date(),
    )
    parser.add_argument(
        "--end-date",
        help="Maximum date",
        type=lambda s: dateutil.parser.parse(s).date(),
    )
    args = parser.parse_args()

    # Find likely transfers from Ledger file. These will be used to match
    # against the transfers from the Johnny database.
    btransfers = FindTransfers(
        args.ledger, args.account, args.start_date, args.end_date
    )

    # Create a mapping by transaction id and a list of the remaining transactions.
    btransfers_map = {}
    btransfers_list = []
    for entry in btransfers:
        mapped = False
        if entry.links:
            for link in sorted(entry.links):
                mo = re.match("johnny-(.*)", link)
                if mo:
                    transaction_id = mo.group(1)
                    btransfers_map[transaction_id] = entry
                    mapped = True
        if not mapped:
            btransfers_list.append(entry)

    # Find transfers imported in Johnny.
    filename = configlib.GetConfigFilenameWithDefaults(None)
    config = configlib.ParseFile(filename)
    imported = petl.frompickle(config.output.nontrades_pickle)
    jtransfers = imported.selectin("rowtype", {"InternalTransfer", "ExternalTransfer"}).sort("datetime")
    if args.end_date:
        jtransfers = jtransfers.selectlt("datetime", args.end_date)

    # Add a location column for the already mapped rows.
    jtransfers = jtransfers.addfield(
        "location", partial(GetLocation, entry_map=btransfers_map), index=0
    )

    if 1:
        print(jtransfers.lookallstr())

    # Look for matches for each of the transfers in Johnny, internal or external.
    for rec in jtransfers.sort("datetime").records():
        # First check if it's already been mapped.
        entry = btransfers_map.get(rec.transaction_id)
        if entry:
            if 0:
                print("*** MATCHED ***")
                bn.print_entry(entry)
            continue

        print("-" * 200)
        print(WrapRecords([rec]).lookallstr())

        # Otherwise look for potential matches.
        print(f"Potential matches for: ^johnny-{rec.transaction_id}\n")
        matches = FindMatches(rec, btransfers_list)
        if matches:
            for entry in matches:
                print(f"{entry.meta['filename']}:{entry.meta['lineno']}:")
                bn.print_entry(entry)

    # Our goal should be for Beancount to provide the transfer destinations for
    # the Beancount exporter, by inserting suitable metadata on each of those
    # transactions. This tool is intended just to find those matches easily.
    # TODO(blais):


if __name__ == "__main__":
    main()
