"""Conversions for non-trades."""

from decimal import Decimal
import datetime as dt
import re
from os import path

from johnny.base import discovery
from johnny.base import nontrades
from johnny.base.etl import Record, Table, petl
from johnny.sources.ameritrade import config_pb2
from johnny.sources.ameritrade import transactions as txnlib


Q2 = Decimal("0.01")


def GetSymbol(rec: Record) -> str:
    mobj = re.search(r"~([A-Z]+)\s*$", rec.description)
    return mobj.group(1) if mobj else None


def GetRowType(rec: Record) -> nontrades.Type:
    Type = nontrades.Type
    rtype = rec.type
    if not rtype:
        raise ValueError(f"Invalid row without a 'type' field: {rec}")

    if rtype == "BAL":
        if rec.description.startswith("Cash balance at the start"):
            return Type.CashBalance
        elif rec.description.startswith("Futures cash balance at the start"):
            return Type.FuturesBalance

    elif rtype == "ADJ":
        if re.match(
            r"(courtesy|courteys) +(adjustment|credit)", rec.description, flags=re.I
        ):
            return Type.Adjustment
        elif re.search(
            r"mark to market at .* official settlement price", rec.description
        ):
            return Type.FuturesMarkToMarket

    elif rtype == "DOI":
        if rec.description.startswith("FREE BALANCE INTEREST ADJUSTMENT"):
            return Type.BalanceInterest
        elif rec.description.startswith("MARGIN INTEREST ADJUSTMENT"):
            return Type.MarginInterest
        elif rec.description.startswith("ORDINARY DIVIDEND"):
            return Type.Dividend

    elif rtype == "EFN":
        if rec.description.startswith("CLIENT REQUESTED ELECTRONIC FUNDING RECEIPT"):
            return Type.ExternalTransfer
        elif rec.description.startswith(
            "CLIENT REQUESTED ELECTRONIC FUNDING DISBURSEMENT"
        ):
            return Type.ExternalTransfer

    elif rtype == "FSWP":
        return Type.Sweep

    elif rtype == "JRN":
        if rec.description.startswith("MISCELLANEOUS JOURNAL ENTRY"):
            return Type.Adjustment
        elif rec.description.startswith("MARK TO THE MARKET"):
            return Type.FuturesMarkToMarket
        elif rec.description.startswith("INTRA-ACCOUNT TRANSFER"):
            return Type.InternalTransfer
        elif rec.description.startswith("HARD TO BORROW FEE"):
            return Type.HardToBorrowFee

    elif rtype == "RAD":
        if rec.description.startswith("CASH ALTERNATIVES INTEREST"):
            return Type.BalanceInterest
        elif rec.description.startswith("INTERNAL TRANSFER BETWEEN ACCOUNTS"):
            return Type.InternalTransfer

    elif rtype == "WIN":
        if rec.description.startswith("THIRD PARTY"):
            return Type.ExternalTransfer

    elif rtype == "WOU":
        if rec.description.startswith("WIRE OUTGOING"):
            return Type.ExternalTransfer

    raise ValueError(f"Unknown {rtype} row: {rec}")


def ConvertNonTrades(other: Table, account_id: str) -> Table:
    """Convert input non-trades to normalized non-trades."""

    assert len(other.fieldnames()) == len(set(other.fieldnames())), other.fieldnames()
    remove_fields = ["commissions_fees", "misc_fees", "symbol", "strategy", "quantity"]
    for field in remove_fields:
        assert len(set(other.values(field))) == 1
    other = other.cutout(*remove_fields)

    other = (
        other.addfield("rowtype", GetRowType)
        .addfield("account", account_id)
        .rename("rowid", "transaction_id")
        .addfield("symbol", GetSymbol)
        .rename("type", "nativetype")
        .convert("ref", lambda v: str(v) if v else None)
        .cut(nontrades.FIELDS)
    )

    return other


def ImportNonTrades(config: config_pb2.Config) -> petl.Table:
    pattern = path.expandvars(config.thinkorswim_account_statement_csv_file_pattern)
    fnmap = discovery.GetLatestFilePerYear(pattern)
    other_list = []
    for year, filename in sorted(fnmap.items()):
        _, other = txnlib.GetTransactions(filename)
        other = other.select(lambda r, y=year: r.datetime.year == y)
        other_list.append(other)
    table = petl.cat(*other_list)
    nontrades = ConvertNonTrades(table, "<ameritrade>")
    return nontrades
