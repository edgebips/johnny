"""Conversions for non-trades."""

from decimal import Decimal
import datetime as dt
import re

from johnny.base.etl import Record, Table, petl
from johnny.base import nontrades
from johnny.base import discovery
from johnny.sources.ameritrade import config_pb2


Q2 = Decimal("0.01")


def ConvertNonTrades(other: Table, account_id: str) -> Table:
    """Convert input non-trades to normalized non-trades."""

    other = (
        other.addfield("rowtype", GetRowType).addfield("account", account_id)
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


def GetRowType(rec: Record) -> nontrades.NonTrade.RowType:
    rtype = rec.type
    if not rtype:
        raise ValueError(f"Invalid row without a 'type' field: {rec}")

    if rtype == "BAL":
        if rec.description.startswith("Cash balance at the start"):
            return nontrades.CashBalance
        elif rec.description.startswith("Futures cash balance at the start"):
            return nontrades.FuturesBalance

    elif rtype == "ADJ":
        if re.match(
            r"(courtesy|courteys) +(adjustment|credit)", rec.description, flags=re.I
        ):
            return nontrades.Adjustment
        elif re.search(
            r"mark to market at .* official settlement price", rec.description
        ):
            return nontrades.FuturesMarkToMarket

    elif rtype == "DOI":
        if rec.description.startswith("FREE BALANCE INTEREST ADJUSTMENT"):
            return nontrades.BalanceInterestd
        elif rec.description.startswith("MARGIN INTEREST ADJUSTMENT"):
            return nontrades.MarginInterest
        elif re.match(r".* TERM GAIN DISTRIBUTION~", rec.description):
            return nontrades.Distribution
        elif rec.description.startswith("ORDINARY DIVIDEND"):
            return nontrades.Dividend

    elif rtype == "EFN":
        if rec.description.startswith("CLIENT REQUESTED ELECTRONIC FUNDING RECEIPT"):
            return nontrades.ExternalTransfer
        elif rec.description.startswith(
            "CLIENT REQUESTED ELECTRONIC FUNDING DISBURSEMENT"
        ):
            return nontrades.ExternalTransfer

    elif rtype == "FSWP":
        return nontrades.Sweep

    elif rtype == "JRN":
        if rec.description.startswith("MISCELLANEOUS JOURNAL ENTRY"):
            return nontrades.Adjustment
        elif rec.description.startswith("MARK TO THE MARKET"):
            return nontrades.FuturesMarkToMarket
        elif rec.description.startswith("INTRA-ACCOUNT TRANSFER"):
            return nontrades.InternalTransfer
        elif rec.description.startswith("HARD TO BORROW FEE"):
            return nontrades.HardToBorrowFee

    elif rtype == "RAD":
        if rec.description.startswith("CASH ALTERNATIVES INTEREST"):
            return nontrades.BalanceInterest
        elif rec.description.startswith("INTERNAL TRANSFER BETWEEN ACCOUNTS"):
            return nontrades.InternalTransfer

    elif rtype == "WIN":
        if rec.description.startswith("THIRD PARTY"):
            return nontrades.ExternalTransfer

    elif rtype == "WOU":
        if rec.description.startswith("WIRE OUTGOING"):
            return nontrades.ExternalTransfer

    raise ValueError(f"Unknown {rtype} row: {rec}")


def ImportNonTrades(config: config_pb2.Config) -> petl.Table:
    pattern = path.expandvars(config.thinkorswim_account_statement_csv_file_pattern)
    fnmap = discovery.GetLatestFilePerYear(pattern)
    other_list = []
    for year, filename in sorted(fnmap.items()):
        _, other = GetTransactions(filename)
        other = other.select(lambda r, y=year: r.datetime.year == y)
        other_list.append(other)
    return petl.cat(*other_list)


def ImportNonTrades(config: config_pb2.Config) -> petl.Table:
    return petl.empty()
