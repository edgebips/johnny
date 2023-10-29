"""Conversions for non-trades."""

from decimal import Decimal
import datetime as dt
from os import path

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
        return Type.CashBalance
    if code == "DEP":
        return Type.ExternalTransfer if rec.Amount >= 0 else Type.ExternalTransfer
    if code == "CINT":
        return Type.BalanceInterest
    if code == "OFEE":
        return Type.MonthlyFee
    if code == "ADJ":
        return Type.Adjustment
    if code == "DIV":
        return Type.Dividend
    raise ValueError(f"Could not find rowtype for {rec}")


def ConvertNonTrades(other: Table) -> Table:
    """Convert input non-trades to normalized non-trades."""

    other = (
        other.addfield("rowtype", GetRowType)
        .rename("ClientAccountID", "account")
        .rename("TransactionID", "transaction_id")
        .rename("TradeID", "ref")
        .addfield("datetime", lambda r: dt.datetime.combine(r.Date, dt.time()))
        .rename("ActivityDescription", "description")
        .rename("ActivityCode", "type")
        .rename("Symbol", "symbol")
        .addfield("amount", lambda r: r.Amount.quantize(Q2))
        .addfield("balance", lambda r: r.Balance.quantize(Q2))
        .addfield("ref", None)
        .rename("type", "nativetype")
        .cut(nontradeslib.FIELDS)
    )

    #  OrderID TradeID AssetClass Symbol UnderlyingSymbol
    # Multiplier Strike Expiry Put/Call Date ActivityCode
    # Buy/Sell TradeQuantity TradePrice TradeGross TradeCommission Amount
    # Balance rowtype

    return other


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
