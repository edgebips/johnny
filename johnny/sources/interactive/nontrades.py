"""Conversions for non-trades."""

from decimal import Decimal
import datetime as dt
from os import path
import re

from johnny.base import discovery
from johnny.base import nontrades as nontradeslib
from johnny.base.etl import Record, Table, petl
from johnny.sources.interactive import transactions
from johnny.sources.interactive import config_pb2


Q2 = Decimal("0.01")


def GetRowType(rec: Record) -> str:
    Type = nontradeslib.Type
    code = rec.ActivityCode
    if not code:
        return Type.Balance
    if code == "DEP":
        return Type.ExternalTransfer if rec.Amount >= 0 else Type.ExternalTransfer
    if code == "CINT":
        return Type.CreditInterest
    if code == "OFEE":
        if re.search(r"\bSNAPSHOT\b", rec.ActivityDescription):
            return Type.DataFee
        if re.search(r"Monthly Minimum Fee", rec.ActivityDescription):
            return Type.MonthlyFee
    if code == "ADJ":
        return Type.Adjustment
    if code == "DIV":
        return Type.Cash
    raise ValueError(f"Could not find rowtype for {rec}")


def ConvertNonTrades(other: Table) -> Table:
    """Convert input non-trades to normalized non-trades."""

    return (
        other.addfield("rowtype", GetRowType)
        .rename("ClientAccountID", "account")
        .rename("TransactionID", "transaction_id")
        .rename("TradeID", "ref")
        .convert("ref", lambda v: v or None)
        .addfield("datetime", lambda r: dt.datetime.combine(r.Date, dt.time()))
        .rename("ActivityDescription", "description")
        .rename("ActivityCode", "type")
        .rename("Symbol", "symbol")
        .convert("symbol", lambda v: v or None)
        .addfield("amount", lambda r: r.Amount.quantize(Q2))
        .addfield("balance", lambda r: r.Balance.quantize(Q2))
        .rename("type", "nativetype")
        .cut(nontradeslib.FIELDS)
    )


def ImportNonTrades(config: config_pb2.Config) -> petl.Table:
    pattern = path.expandvars(config.transactions_flex_report_csv_file_pattern)
    fnmap = discovery.GetLatestFilePerYear(pattern)
    other_list = []
    for year, filename in sorted(fnmap.items()):
        _, other = transactions.GetTransactions(filename)
        other_list.append(other.select(lambda r, y=year: r.Date.year == y))
    nontrades_raw = petl.cat(*other_list)
    nontrades = ConvertNonTrades(nontrades_raw)
    return nontrades
