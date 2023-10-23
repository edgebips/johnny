"""Conversions for non-trades."""

from decimal import Decimal
import datetime as dt

from johnny.base.etl import Record, Table
from johnny.base import nontrades


Q2 = Decimal("0.01")


def ConvertNonTrades(other: Table) -> Table:
    """Convert input non-trades to normalized non-trades."""

    other = (
        other.addfield("rowtype", GetRowType)
        .rename("ClientAccountID", "account")
        .rename("TransactionID", "transaction_id")
        .addfield("datetime", lambda r: dt.datetime.combine(r.Date, dt.time()))
        .rename("ActivityDescription", "description")
        .rename("ActivityCode", "type")
        .rename("Symbol", "symbol")
        .addfield("amount", lambda r: r.Amount.quantize(Q2))
        .addfield("balance", lambda r: r.Balance.quantize(Q2))
        .addfield("ref", None)
        .cut(nontrades.FIELDS)
    )

    #  OrderID TradeID AssetClass Symbol UnderlyingSymbol
    # Multiplier Strike Expiry Put/Call Date ActivityCode
    # Buy/Sell TradeQuantity TradePrice TradeGross TradeCommission Amount
    # Balance rowtype

    return other


def GetRowType(rec: Record) -> str:
    code = rec.ActivityCode
    if not code:
        return "CashBalance"
    if code == "DEP":
        return "TransferIn" if rec.Amount >= 0 else "TransferOut"
    if code == "CINT":
        return "BalanceInterest"
    if code == "OFEE":
        return "MonthlyFee"
    if code == "ADJ":
        return "Adjustment"
    if code == "DIV":
        return "Dividend"
    raise ValueError(f"Could not find rowtype for {rec}")
