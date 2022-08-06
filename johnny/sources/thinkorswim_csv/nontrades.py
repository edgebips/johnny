"""Conversions for non-trades."""

from decimal import Decimal
import datetime as dt
import re

from johnny.base.etl import Record, Table
from johnny.base import nontrades


Q2 = Decimal("0.01")


def ConvertNonTrades(other: Table, account_id: str) -> Table:
    """Convert input non-trades to normalized non-trades."""

    other = (
        other.addfield("rowtype", GetRowType)
        .addfield("account", account_id)
        # .rename("ClientAccountID", "account")
        # .rename("TransactionID", "transaction_id")
        # .addfield("datetime", lambda r: dt.datetime.combine(r.Date, dt.time()))
        # .rename("ActivityDescription", "description")
        # .rename("ActivityCode", "type")
        # .rename("Symbol", "symbol")
        # .addfield("amount", lambda r: r.Amount.quantize(Q2))
        # .addfield("balance", lambda r: r.Balance.quantize(Q2))
        # .addfield("ref", None)
        # .cut(nontrades.FIELDS)
    )

    #  OrderID TradeID AssetClass Symbol UnderlyingSymbol
    # Multiplier Strike Expiry Put/Call Date ActivityCode
    # Buy/Sell TradeQuantity TradePrice TradeGross TradeCommission Amount
    # Balance rowtype

    return other


def GetRowType(rec: Record) -> str:
    rtype = rec.type
    if not rtype:
        raise ValueError(f"Invalid row without a 'type' field: {rec}")

    if rtype == "BAL":
        if rec.description.startswith("Cash balance at the start"):
            return "CashBalance"
        elif rec.description.startswith("Futures cash balance at the start"):
            return "FuturesBalance"

    elif rtype == "ADJ":
        if re.match(
            r"(courtesy|courteys) +(adjustment|credit)", rec.description, flags=re.I
        ):
            return "Adjustment"
        elif re.search(
            r"mark to market at .* official settlement price", rec.description
        ):
            return "FuturesMTM"

    elif rtype == "DOI":
        if rec.description.startswith("FREE BALANCE INTEREST ADJUSTMENT"):
            return "BalanceInterestd"
        elif rec.description.startswith("MARGIN INTEREST ADJUSTMENT"):
            return "MarginInterest"
        elif re.match(r".* TERM GAIN DISTRIBUTION~", rec.description):
            return "Distribution"
        elif rec.description.startswith("ORDINARY DIVIDEND"):
            return "Dividend"

    elif rtype == "EFN":
        if rec.description.startswith("CLIENT REQUESTED ELECTRONIC FUNDING RECEIPT"):
            return "TransferIn"
        elif rec.description.startswith(
            "CLIENT REQUESTED ELECTRONIC FUNDING DISBURSEMENT"
        ):
            return "TransferOut"

    elif rtype == "FSWP":
        return "Sweep"

    elif rtype == "JRN":
        if rec.description.startswith("MISCELLANEOUS JOURNAL ENTRY"):
            return "Adjustment"
        elif rec.description.startswith("MARK TO THE MARKET"):
            return "FuturesMTM"
        elif rec.description.startswith("INTRA-ACCOUNT TRANSFER"):
            return "TransferInternal"
        elif rec.description.startswith("HARD TO BORROW FEE"):
            return "HTBFee"

    elif rtype == "RAD":
        if rec.description.startswith("CASH ALTERNATIVES INTEREST"):
            return "BalanceInterest"
        elif rec.description.startswith("INTERNAL TRANSFER BETWEEN ACCOUNTS"):
            return "TransferInternal"

    elif rtype == "WIN":
        if rec.description.startswith("THIRD PARTY"):
            return "TransferIn"

    elif rtype == "WOU":
        if rec.description.startswith("WIRE OUTGOING"):
            return "TransferOut"

    raise ValueError(f"Unknown {rtype} row: {rec}")
