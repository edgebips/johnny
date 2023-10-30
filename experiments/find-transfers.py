#!/usr/bin/env python3
"""Find Beancount transfers to particular accounts.

This can be used to create the table of transfer sources/destinations.
"""

import argparse
import os
import re
import datetime as dt
from typing import Optional
import dateutil.parser

from beancount import loader
import beancount.api as bn

from johnny.base.etl import petl
from johnny.base import config as configlib


def FindTransfers(ledger: str, account_regex: str, start_date: Optional[dt.date]):
    entries, errors, options_map = loader.load_file(ledger)
    acctypes = bn.get_account_types(options_map)

    cash_account = f"{account_regex}:Cash"

    transfers = []
    for entry in bn.filter_txns(entries):
        if start_date and entry.date < start_date:
            continue

        matches, other = [], []
        for posting in entry.postings:
            tlist = matches if re.match(cash_account, posting.account) else other
            tlist.append(posting)
        if matches:
            other_assets = [
                posting
                for posting in other
                if (
                    not re.match(account_regex, posting.account)
                    and bn.is_balance_sheet_account(posting.account, acctypes)
                )
            ]
            if other_assets and len(other_assets) == 1:
                assert len(matches) == 1, bn.format_entry(entry)
                assert matches[0].units.currency == "USD"
                posting = matches[0]
                transfers.append(
                    (entry.date, posting.units.number, posting.account, entry)
                )

    if 0:
        for date, number, account, entry in transfers:
            print((date, number, account))
            print(bn.format_entry(entry))
            print()

    return transfers


def main():
    parser = argparse.ArgumentParser(description=__doc__.strip())
    parser.add_argument("-f", "--ledger", default=os.environ["L"], help="Ledger")
    parser.add_argument(
        "account", help="Account regex against which to test for transfers"
    )
    parser.add_argument(
        "-d",
        "--start-date",
        help="Mininmum start date",
        type=lambda s: dateutil.parser.parse(s).date(),
    )
    args = parser.parse_args()

    # Find likely transfers from Ledger file.
    btransfers = FindTransfers(args.ledger, args.account, args.start_date)

    # Find transfers imported in Johnny.
    filename = configlib.GetConfigFilenameWithDefaults(None)
    config = configlib.ParseFile(filename)
    imported = petl.frompickle(config.output.nontrades_pickle)
    jtransfers = imported.selectin("rowtype", {"InternalTransfer", "ExternalTransfer"})

    # Look for matches.
    for date, number, account, entry in btransfers:
        print("-" * 200)
        print((date, number, account))
        print(bn.format_entry(entry))
        print(
            jtransfers.selecteq("amount", number)
            .addfield("datediff", lambda r: abs(r.datetime.date() - date))
            .selectle("datediff", dt.timedelta(days=5))
            .sort("datediff")
            .lookallstr()
        )
        print()

    # print(jtransfers.lookallstr())

    # Our goal should be for Beancount to provide the transfer destinations for
    # the Beancount exporter, by inserting suitable metadata on each of those
    # transactions. This tool is intended just to find those matches easily.
    # TODO(blais):


if __name__ == "__main__":
    main()
