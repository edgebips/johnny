"""Interactive Brokers - Parse account statement CSV files.

  Login > Performance & Statements > Reports > Flex Queries > (period) > Run

You need to select

- "Statement of Funds"
- "Trades"
- "Commission Details"

in the same Flex report to produce everything as a single file with two tables,
and enable all the columns for each, and it has to be done from a Flex query.
This is the way to do this properly. Also, ensure you include header and trailer
records so we can split the multiple tables from the file.

("Custom reports" from the first page are joint reports (includes many tables in
a single CSV file with a prefix column), they have an option to output the
statement of funds table, but it doesn't contain the stock detail. You want a
"Flex report", which has joins between the tables, e.g., the statement of funds
and the trades table.)

Note that during the weekends you may not be able to download up to the day's
date (an error about the report/statement not being ready to download will be
shown). Simply select a Custom Date Range, and select the last valid market open
date / business date in order to produce a valid report.

"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from decimal import Decimal
from functools import partial
from itertools import chain
from os import path
from typing import Any, Dict, List, Optional, Tuple, Union, Iterable
import collections
import csv
import datetime as dt
import hashlib
import itertools
import logging
import os
import pprint
import re
import typing

import click
from dateutil import parser

import mulmat
from mulmat import multipliers
from johnny.base.config import Account
from johnny.base import config as configlib
from johnny.base import discovery
from johnny.base import instrument
from johnny.base import inventories
from johnny.base import number
from johnny.base import transactions as txnlib
from johnny.base.etl import petl, Table, Record, WrapRecords
from johnny.sources.thinkorswim_csv import symbols
from johnny.sources.thinkorswim_csv import utils
from johnny.utils import csv_utils
from johnny.sources.interactive_csv import nontrades

Table = petl.Table
Record = petl.Record
Config = Any
ZERO = Decimal(0)


def CheckValues(values, value):
    assert value in values
    return value


def GetAssetClass(v: str) -> str:
    if v == "STK":
        return "Equity"
    elif v == "OPT":
        return "EquityOption"
    else:
        raise ValueError(f"Unknown value {v}")


def GetExpiration(v: str) -> Optional[dt.date]:
    return dt.datetime.strptime(v, "%Y-%m-%d").date() if v else ""


def GetSymbol(rec: Record) -> instrument.Instrument:
    if rec.instype == "Equity":
        return instrument.FromColumns(
            rec.Symbol, None, None, None, None, rec.multiplier
        )
    elif rec.instype == "EquityOption":
        return instrument.FromColumns(
            rec.underlying,
            rec.expiration,
            rec.expcode,
            rec.putcall,
            rec.strike,
            rec.multiplier,
        )
    else:
        raise ValueError(f"Unknown instrument type: {rec}")


def SplitTables(filename: str) -> Dict[str, Table]:
    """Split an IBKR table into multiple tables.
    This requires the headers and trailers."""

    tablemap = {}
    with open(filename, encoding="utf8") as f:
        rowset = None
        for row in csv.reader(f):
            if row[0] in {"BOF", "BOA", "EOA", "EOF"}:
                continue
            if row[0] == "BOS":
                rowset = tablemap[row[1]] = []
            elif row[0] == "EOS":
                pass
            else:
                rowset.append(row)
    return {key: petl.wrap(rows) for key, rows in tablemap.items()}


def GetEffect(oc: str) -> str:
    if oc == "O":
        return "OPENING"
    elif oc == "C":
        return "CLOSING"
    else:
        assert not oc
        return ""


def ProcessCommissions(table: petl.Table) -> petl.Table:
    """Prepare commissions table for joining."""
    table = (
        table
        # Keep the number columns.
        .cut(
            "TradeID",
            "TotalCommission",
            "BrokerExecutionCharge",
            "BrokerClearingCharge",
            "ThirdPartyExecutionCharge",
            "ThirdPartyClearingCharge",
            "ThirdPartyRegulatoryCharge",
            "RegFINRATradingActivityFee",
            "RegSection31TransactionFee",
            "RegOther",
            "Other",
        )
        .convert(
            [
                "TotalCommission",
                "BrokerExecutionCharge",
                "BrokerClearingCharge",
                "ThirdPartyExecutionCharge",
                "ThirdPartyClearingCharge",
                "ThirdPartyRegulatoryCharge",
                "RegFINRATradingActivityFee",
                "RegSection31TransactionFee",
                "RegOther",
                "Other",
            ],
            lambda v: Decimal(v) if v else "",
        )
        # Check the sum of commissions.
        .addfield(
            "rcommissions",
            lambda r: (
                r["BrokerExecutionCharge"]
                + r["BrokerClearingCharge"]
                + r["ThirdPartyExecutionCharge"]
                + r["ThirdPartyClearingCharge"]
                + r["ThirdPartyRegulatoryCharge"]
            ),
        )
        .addfield(
            "dcommissions",
            lambda r: (
                (r.TotalCommission - r.rcommissions).quantize(Decimal("0.00001"))
            ),
        )
        # Check the sum of fees.
        .addfield(
            "rfees",
            lambda r: (
                r["RegFINRATradingActivityFee"]
                + r["RegSection31TransactionFee"]
                + r["RegOther"]
                + r["Other"]
            ),
        )
        .addfield(
            "dfees",
            lambda r: (
                (r["ThirdPartyRegulatoryCharge"] - r.rfees).quantize(Decimal("0.00001"))
            ),
        )
    )

    # Compute commissions ex-fees and fees and return a table to join with the trades.
    return (
        table.addfield(
            "commissions",
            lambda r: (r["TotalCommission"] - r["ThirdPartyRegulatoryCharge"]),
        )
        .addfield("fees", lambda r: r["ThirdPartyRegulatoryCharge"])
        .cut("TradeID", "commissions", "fees")
    )


def GetTransactions(filename: str) -> Tuple[Table, Table]:
    """Read and prepare all the tables to be joined."""

    # Split the rows into multiople tables:
    # 1. Statement of funds
    # 2. Trades
    # 3. Commision Details
    # The table of trades is used to pull up OPENING, CLOSING trade status.
    # The table of commission details is used to split up commissions and fees.
    tablemap = SplitTables(filename)
    assert set(tablemap) == {"STFU", "TRNT", "UNBC"}

    # Check that the list of trades matches the subset of trade rows from the
    # statement of funds.
    funds_map = tablemap["STFU"].selecttrue("TradeID").recordlookupone("TradeID")
    trades_map = tablemap["TRNT"].recordlookupone("TradeID")
    comm_map = tablemap["UNBC"].recordlookupone("TradeID")
    assert set(funds_map) == set(trades_map)
    assert set(comm_map) <= set(funds_map)

    # Perform general preparation to the statement of funds table that will be
    # useful for trade and non-trade data.
    table = (
        tablemap["STFU"]
        # Convert date.
        .convert("Date", lambda v: dt.datetime.strptime(v, "%Y-%m-%d").date())
        # .sort('Date')
        # Keep just some of the fields.
        .cut(
            "ClientAccountID",
            "TransactionID",
            "OrderID",
            "TradeID",
            "AssetClass",
            "Symbol",
            "UnderlyingSymbol",
            "Multiplier",
            "Strike",
            "Expiry",
            "Put/Call",
            "Date",
            "ActivityCode",
            "ActivityDescription",
            "Buy/Sell",
            "TradeQuantity",
            "TradePrice",
            "TradeGross",
            "TradeCommission",
            "Debit",
            "Credit",
            "Amount",
            "Balance",
        )
        # Convert numbers.
        .convert(
            [
                "Multiplier",
                "Strike",
                "TradeQuantity",
                "TradePrice",
                "TradeGross",
                "TradeCommission",
                "Debit",
                "Credit",
                "Amount",
                "Balance",
            ],
            lambda v: Decimal(v) if v else "",
        )
        # Check: (Debit + Credit) == Amount; then remove.
        .addfield(
            "Cr+Dr",
            lambda r: round((r.Debit or ZERO) + (r.Credit or ZERO) - r.Amount, 6),
        )
        .cutout("Cr+Dr", "Debit", "Credit")
        # Check: that (Multiplier * TradeQuantity * TradePrice) == TradeGross
        .addfield(
            "Gross",
            lambda r: (
                ((r.Multiplier * r.TradeQuantity * r.TradePrice) + r.TradeGross)
                if r.Multiplier
                else ""
            ),
        )
        .cutout("Gross")
    )

    # We're going to split the statement of funds between trade and non-trade data.
    trade, nontrade = table.biselect(lambda r: r.ActivityCode in {"BUY", "SELL", "DIV"})

    # Join datetime and pos-effect rom the trades table.
    trnt = (
        tablemap["TRNT"]
        .convert("DateTime", lambda v: parser.parse(v))
        .convert("Open/CloseIndicator", GetEffect)
        .cut("TradeID", "DateTime", "Open/CloseIndicator")
        .rename(
            {
                "DateTime": "datetime",
                "Open/CloseIndicator": "effect",
            }
        )
    )
    trade = petl.leftjoin(trade, trnt, key="TradeID")

    # Dividends don't have a datetime and pos-effect. Fill them in.
    time_open = dt.time(9, 30)
    trade = (
        trade.convert("effect", lambda v: "" if v is None else v)
        .convert(
            "datetime",
            lambda v, r: (
                dt.datetime.combine(r["Date"], time_open) if v is None else v
            ),
            pass_row=True,
        )
        .cutout("Date")
    )

    # Join some of the rows rom the commissions table.
    unbc = ProcessCommissions(tablemap["UNBC"])
    trade = petl.leftjoin(trade, unbc, key="TradeID").replace(
        ["commissions", "fees"], None, Decimal(0)
    )

    # Setup all instrument fields, derive a unique symbol from them, and shrink
    # the fields away.
    trade = (
        trade
        # Compute normalized instrument type.
        .convert("AssetClass", GetAssetClass)
        .rename("AssetClass", "instype")
        # Compute normalized instrument fields.
        .rename(
            {
                "UnderlyingSymbol": "underlying",
                "Put/Call": "putcall",
                "Strike": "strike",
                "Multiplier": "multiplier",
            }
        )
        # Compute and normawlize expiration, if present.
        #
        # Note: we haven't traded futures in IBKR yet, so don't know what
        # their expcode looks like.
        .convert("Expiry", GetExpiration)
        .rename("Expiry", "expiration")
        .addfield("expcode", "")
        # Produce normalized instrument/symbol.
        .addfield("symbol", GetSymbol)
        .convert("symbol", str)
        .applyfn(instrument.Shrink)
        .cutout("Symbol")
    )

    # Normalize the rest of the fields.
    trade = (
        # Verify the trade commission.
        trade.addfield(
            "cdiff",
            lambda r: (r["TradeCommission"] - (r["commissions"] + r["fees"])).quantize(
                Decimal("0.00001")
            ),
        )
        .cutout("TradeCommission", "cdiff")
        .rename("ActivityCode", "rowtype")
        .convert("rowtype", lambda v: "Dividend" if v == "DIV" else "Trade")
        .addfield("cash", lambda r: r["Amount"] if r["rowtype"] == "Dividend" else ZERO)
    )
    trade = (
        trade.rename(
            {
                "ClientAccountID": "account",
                "TransactionID": "transaction_id",
                "OrderID": "order_id",
                "Buy/Sell": "instruction",
                "TradeGross": "cost",
                "TradeQuantity": "quantity",
                "TradePrice": "price",
                "ActivityDescription": "description",
            }
        )
        # Absolute value for quantity.
        .convert("quantity", abs)
        .addfield("init", None)
    )

    # Reorder the final fields.
    trade = trade.cut(txnlib.FIELDS)

    return trade, nontrade


def ImportAll(source: str, config: configlib.Config) -> Dict["LogType", Table]:
    fnmap = discovery.GetLatestFilePerYear(source)

    transactions_list = []
    other_list = []
    for year, filename in sorted(fnmap.items()):
        transactions, other = GetTransactions(filename)
        transactions_list.append(
            transactions.select(lambda r, y=year: r.datetime.year == y)
        )
        other_list.append(other.select(lambda r, y=year: r.Date.year == y))

    transactions = petl.cat(*transactions_list)
    other = petl.cat(*other_list)

    return {Account.TRANSACTIONS: transactions, Account.OTHER: other}


def Import(source: str, config: configlib.Config, logtype: "LogType") -> Table:
    """Process the filename, normalize, and output as a table."""
    return ImportAll(source, config)[logtype]


@click.command()
@click.argument("source", type=click.Path())
@click.option("--cash", is_flag=True, help="Print out cash transactions.")
def main(source: str, cash: bool):
    """Simple local runner for this translator."""

    alltypes = ImportAll(source, None)

    if 1:
        other = alltypes[Account.OTHER]
        if 0:
            for rec in other.aggregate("ActivityCode", WrapRecords).records():
                print(rec.value.lookallstr())
        else:
            nother = nontrades.ConvertNonTrades(other)
            for rec in nother.aggregate("type", WrapRecords).records():
                print(rec.value.lookallstr())
            print(nother.lookallstr())

    if 0:
        transactions = alltypes[Account.TRANSACTIONS]
        print(transactions.lookallstr())


if __name__ == "__main__":
    main()
