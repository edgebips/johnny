"""Think-or-Swim - Parse account statement CSV files.

Instructions:
- Start TOS
- Go to the "Monitor" tab
- Select the "Account Statement" page
- Select the desired time period
- Right on the rightmost hamburger menu and select "Export to File..."

This module implements a pretty tight reconciliation from the AccountStatement
export to CSV, joining and verifying the equities cash and futures cash
statements with the trade history.

Caveats:
- Transaction IDs are missing can have to be joined in later from the API.
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
import datetime
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

from johnny.base import config as configlib
from johnny.base import discovery
from johnny.base import instrument
from johnny.base import inventories
from johnny.base import number
from johnny.base import transactions as txnlib
from johnny.base.config import Account
from johnny.base.etl import petl, Table, Record, Replace, WrapRecords, Assert
from johnny.sources.thinkorswim_csv import nontrades
from johnny.sources.thinkorswim_csv import symbols
from johnny.sources.thinkorswim_csv import utils
from johnny.sources.thinkorswim_csv import config_pb2
from johnny.utils import csv_utils


Table = petl.Table
Record = petl.Record
debug = False
ONE = Decimal(1)
ZERO = Decimal(0)
Q3 = Decimal("0.001")


# Include the commnissions on the last leg. This matches the worksheets.
DO_COMMISSIONS_LAST_LEG = False


def SplitCashBalance(statement: Table, trade_hist: Table) -> Tuple[Table, Table]:
    """Split the cash statement between simple cash effects vs. trades.
    Trades includes expirations and dividend events."""

    # Strategy has been inferred from the preparation and can be used to
    # distinguish trading and non-trading rows.
    #
    # TODO(blais): Use biselect() here.
    nontrade = statement.select(lambda r: not r.strategy)
    trade = statement.select(lambda r: bool(r.strategy))

    # Check that the non-trade cash statement transactions have no overlap
    # whatsoever with the trades on.
    keyed_statement = nontrade.aggregate("datetime", list)
    keyed_trades = trade_hist.aggregate("exec_time", list)

    joined = petl.join(
        keyed_statement,
        keyed_trades,
        lkey="datetime",
        rkey="exec_time",
        lprefix="cash",
        rprefix="trade",
    )

    if joined.nrows() != 0:
        raise ValueError("Statement table contains trade data: {}".format(joined))

    return trade, nontrade


def SplitFuturesStatements(futures: Table, trade_hist: Table) -> Tuple[Table, Table]:
    """Split the cash statement between simple cash effects vs. trades.
    Trades includes expirations and dividend events."""

    # Splitting up the futures statement is trivial because the "Ref" columns is
    # present and consistently all trading data has a ref but not non-trading
    # data.
    nontrade = futures.select(lambda r: not r.ref)
    trade = futures.select(lambda r: bool(r.ref))

    # Check that the non-trade cash statement transactions have no overlap
    # whatsoever with the trades on.
    keyed_statement = nontrade.aggregate("datetime", list)
    keyed_trades = trade_hist.aggregate("exec_time", list)
    joined = petl.join(
        keyed_statement,
        keyed_trades,
        lkey="datetime",
        rkey="exec_time",
        lprefix="cash",
        rprefix="trade",
    )
    if joined.nrows() != 0:
        raise ValueError("Statement table contains trade data: {}".format(joined))

    return trade, nontrade


def ReconcilePairsOrderIds(table: Table, threshold: int) -> Table:
    """On a pairs trade, the time issued will be identical, but we will find two
    distinct symbols and order ids (one that is 1 or 2 integer apart). We reduce
    the ids to the smallest one by looking at increments below some threshold
    and squashing the later ones. This way we can link together pairs trades or
    blast-alls (probably).
    """

    def AdjustedOrder(head_id, rec: Record) -> int:
        if head_id[0] is None:
            head_id[0] = rec.order_id
            return rec.order_id
        diff = rec.order_id - head_id[0]
        if diff == 0:
            return rec.order_id
        if diff < threshold:
            return head_id[0]
        head_id[0] = rec.order_id
        return rec.order_id

    table = (
        table.sort("order_id")
        .addfield("pair_id", partial(AdjustedOrder, [None]))
        .addfield(
            "order_diff",
            lambda r: ((r.order_id - r.pair_id) if (r.order_id != r.pair_id) else ""),
        )
    )

    if 0:
        # Debug print.
        for order_id, group in table.aggregate("pair_id", list).records():
            if len(set(rec.order_id for rec in group)) > 1:
                print(petl.wrap(chain([table.header()], group)).lookallstr())

    return table


def ProcessTradeHistory(
    equities_cash: Table, futures_cash: Table, trade_hist: Table
) -> Tuple[List[Any], List[Any], Table, Table]:
    """Join the trade history table with the equities table.

    Note that the equities table does not contian the ref ids, so they we have
    to use the symbol as the second key to disambiguate further from the time of
    execution. (If TD allowed exporting the ref from the Cash Statement we would
    just use that, that would resolve the problem. There is a bug in the
    software, it doesn't get exported.)
    """

    # Fix up order ids to join pairs trades.
    trade_hist = ReconcilePairsOrderIds(trade_hist, 5)

    # We want to pair up the trades from the equities and futures statements
    # with the trades from the trade history table. We will aggregate the trade
    # history table by a unique key (using the time, seems to be pretty good)
    # and decimate it by matching rows from the cash tables. Then we verify that
    # the trade history has been fully accounted for by checking that it's empty.
    trade_hist_map = trade_hist.recordlookup("exec_time")

    # Process the equities cash table.
    def MatchTradingRows(cash_table: Table):
        order_groups = []
        other_rows = []
        mapping = cash_table.recordlookup("datetime")
        for dtime, cash_rows in mapping.items():
            # If the transaction is not a trade, ignore it.
            # Dividends, expirations are processed elsewhere.
            if not any(crow.type == "TRD" for crow in cash_rows):
                other_rows.extend(cash_rows)
                continue

            # Pull up the rows corresponding to this cash statement and remove
            # them from the trade history.
            try:
                trade_rows = trade_hist_map.pop(dtime)
            except KeyError:
                message = "Trade history for cash rows: '{}' not found".format(
                    cash_rows
                )
                logging.error(message)
                continue

            order_groups.append((dtime, cash_rows, trade_rows))

        # Ensure that even an empty table has the right field names.
        other_table = petl.wrap([cash_table.header()] + other_rows)

        return order_groups, other_table

    # Fetch the trade history rows for equities.
    equities_groups, equities_others = MatchTradingRows(equities_cash)
    # Fetch the trade history rows for futures.
    futures_groups, futures_others = MatchTradingRows(futures_cash)

    # Assert that the trade history table has been fully accounted for.
    if trade_hist_map:
        raise ValueError(
            "Some trades from the trade history not covered by cash: "
            "{}".format(trade_hist_map)
        )

    return (equities_groups, futures_groups, equities_others, futures_others)


def _CreateInstrument(r: Record) -> str:
    """Create an instrument from the expiration data."""
    return instrument.FromColumns(
        r.underlying, r.expiration, r.expcode or None, r.putcall, r.strike, r.multiplier
    )


def GetOrderIdFromSymbol(rec: Record) -> str:
    """Make up a unique order id for an expiration."""
    md5 = hashlib.blake2s(digest_size=4)
    md5.update(rec.symbol.encode("ascii"))
    return md5.hexdigest()


def ProcessExpirationsToTransactions(cash_table: Table) -> Table:
    """Look at cash table and extract and normalize expirations from it."""

    expirations, rest = cash_table.biselect(lambda r: r.type == "RAD")

    expirations = (
        expirations.addfield(
            "valid_expi",
            lambda r: Assert(
                re.match(r"REMOVAL OF OPTION DUE TO EXPIRATION", r.description)
            ),
        )
        .addfield("_x", _ParseExpirationDescriptionDetailed)
        .convert("quantity", lambda _, r: r._x["quantity"], pass_row=True)
        .rename("symbol", "symbol_in")
        .addfields(
            [
                (name, lambda r, n=name: r._x.get(n))
                for name in [
                    "instype",
                    "underlying",
                    "expiration",
                ]
            ]
        )
        .addfield("expcode", "")
        .addfields(
            [
                (name, lambda r, n=name: r._x.get(n))
                for name in ["putcall", "strike", "multiplier", "instruction"]
            ]
        )
        .cutout("_x")
        .addfield("symbol", lambda r: str(_CreateInstrument(r)))
        # Fix up the remaining fields.
        .addfield("order_id", GetOrderIdFromSymbol)
        .addfield("effect", "CLOSING")
        .addfield("rowtype", "Expire")
        .addfield("instype", None)
        .addfield("commissions", ZERO)
        .rename("commissions_fees", "fees")
        .addfield("price", ZERO)
        .addfield("cash", ZERO)
        # Clean up for the final table.
        .cut(
            "datetime",
            "order_id",
            "rowtype",
            "effect",
            "instruction",
            "symbol",
            "instype",
            "underlying",
            "expiration",
            "expcode",
            "putcall",
            "strike",
            "multiplier",
            "quantity",
            "price",
            "cash",
            "commissions",
            "fees",
            "description",
        )
    )
    return expirations, rest


def ProcessDividends(table: Table) -> Tuple[Table, Table]:
    """Check that the entire table contains only dividends."""
    table = (
        table.addfield(
            "valid_div",
            lambda r: Assert(
                r.type == "DOI" and r.description.startswith("ORDINARY DIVIDEND")
            ),
        )
        .addfield("order_id", lambda r: r["rowid"])
        .addfield("pair_id", lambda r: r["rowid"])
        .capture(
            "description", r"ORDINARY DIVIDEND~(.*)", ["symbol"], include_original=True
        )
        .addfield("underlying", lambda r: r["symbol"])
        .addfield("rowtype", "Dividend")
        .addfield("instruction", "")
        .addfield("effect", "")
        .addfield("instype", "Equity")
        .addfield("expiration", None)
        .addfield("expcode", None)
        .addfield("putcall", None)
        .addfield("strike", None)
        .addfield("multiplier", ONE)
        .cutout("quantity")
        .addfield("quantity", ZERO)
        .addfield("price", ZERO)
        .rename("amount", "cash")
        .addfield("commissions", ZERO)
        .rename("misc_fees", "fees")
        .cut(
            [
                "datetime",
                "order_id",
                "pair_id",
                "rowtype",
                "instruction",
                "effect",
                "symbol",
                "instype",
                "underlying",
                "expiration",
                "expcode",
                "putcall",
                "strike",
                "multiplier",
                "quantity",
                "price",
                "cash",
                "commissions",
                "fees",
                "description",
            ]
        )
    )

    return table, petl.empty()


Group = Tuple[datetime.date, List[Record], List[Record]]


def PrintGroup(group: Group):
    dtime, cash_rows, trade_rows = group
    print("-" * 200)
    print(dtime)
    ctable = petl.wrap(chain([cash_rows[0].flds], cash_rows))
    print(ctable.lookallstr())
    ttable = petl.wrap(chain([trade_rows[0].flds], trade_rows))
    print(ttable.lookallstr())


def FindMultiplierInDescription(string: str) -> Decimal:
    """Find a multiplier spec in the given description string."""
    match = re.search(r"\b1/(\d+)\b", string)
    if not match:
        match = re.search(r"(?:\s|^)(/[A-Z0-9]*?)[FGHJKMNQUVXZ]2[0-9]\b", string)
        if not match:
            raise ValueError("No symbol to find multiplier: '{}'".format(string))
        symbol = match.group(1)
        try:
            multiplier = multipliers.MULTIPLIERS[symbol]
        except KeyError:
            raise ValueError("No multiplier for symbol: '{}'".format(symbol))
        return Decimal(multiplier)
    return Decimal(match.group(1))


_TXN_FIELDS = (
    "datetime",
    "order_id",
    "pair_id",
    "rowtype",
    "instruction",
    "effect",
    "symbol",
    "instype",
    "underlying",
    "expiration",
    "expcode",
    "putcall",
    "strike",
    "multiplier",
    "quantity",
    "price",
    "cash",
    "commissions",
    "fees",
    "description",
)


def SplitGroupsToTransactions(groups: List[Group], is_futures: bool) -> Table:
    """Convert groups of cash and trade rows to transactions."""

    rows = [_TXN_FIELDS]
    for group in groups:
        dtime, cash_rows, trade_rows = group
        if 0:
            PrintGroup(group)

        # Attempt to match up each cash row to each trade rows. We assert that
        # we always find only two situations: N:N matches, where we can pair up
        # the transactions, and 1:n matches (for options strategies) where the
        # fees will be inserted on one of the resulting transactions.
        subgroups = []
        if len(cash_rows) == 1:
            subgroups.append((cash_rows, trade_rows))

        elif len(cash_rows) == len(trade_rows):
            # If we have an N:N situation, pair up the two groups by using quantity.
            cash_rows_copy = list(cash_rows)
            for trow in trade_rows:
                for index, crow in enumerate(cash_rows_copy):
                    if crow.quantity == trow.quantity:
                        break
                else:
                    raise ValueError(
                        "Could not find cash row matching the quantity of a trade row"
                    )
                crow = cash_rows_copy.pop(index)
                subgroups.append(([crow], [trow]))
            if cash_rows_copy:
                raise ValueError("Internal error: residual row after matching.")

        else:
            message = "Impossible to match up cash and trade rows."
            if is_futures:
                # Match up multiples of repeated rows.
                multiple, mod = divmod(len(trade_rows), len(cash_rows))
                if mod == 0:
                    urows = set(
                        Replace(crow, rowid=None, balance=None) for crow in cash_rows
                    )
                    if len(urows) == 1:
                        subgroups.append((cash_rows, trade_rows))
                    else:
                        raise ValueError(message)
            else:
                # logging.warning(message)
                subgroups.append((cash_rows, trade_rows))

        # Process each of the subgroups.
        for cash_rows, trade_rows in subgroups:
            # Pick up all the fees from the cash transactions.
            description = cash_rows[0].description
            cash_commissions = sum(crow.commissions_fees for crow in cash_rows)
            cash_fees = sum(crow.misc_fees for crow in cash_rows)

            commissions = (cash_commissions / len(trade_rows)).quantize(Q3)
            fees = (cash_fees / len(trade_rows)).quantize(Q3)

            for index, trow in enumerate(trade_rows, start=1):
                row_desc = (
                    "{}  [{}/{}]".format(description, index, len(trade_rows))
                    if len(trade_rows) > 1
                    else description
                )

                inst = instrument.FromColumns(
                    trow.underlying,
                    trow.expiration,
                    trow.expcode.lstrip("/") if trow.expcode else None,
                    trow.putcall,
                    trow.strike,
                    trow.multiplier,
                )
                symbol = str(inst)

                if DO_COMMISSIONS_LAST_LEG:
                    # Include the commnissions on the last leg. This matches the
                    # worksheets.
                    if index == 1:
                        commissions = cash_commissions
                        fees = cash_fees
                    else:
                        commissions = ZERO
                        fees = ZERO

                txn = (
                    trow.exec_time,
                    trow.order_id,
                    trow.pair_id,
                    "Trade",
                    trow.side,
                    trow.pos_effect,
                    symbol,
                    # TODO(blais): Remove these.
                    trow.instype,
                    trow.underlying,
                    trow.expiration,
                    trow.expcode,
                    trow.putcall,
                    trow.strike,
                    trow.multiplier,
                    trow.quantity,
                    trow.price,
                    ZERO,
                    commissions,
                    fees,
                    row_desc,
                )

                rows.append(txn)

    return petl.wrap(rows)


# -------------------------------------------------------------------------------
# Prepare all the tables for processing


def CashBalance_Prepare(table: Table) -> Table:
    """Process the cash account statement balance."""
    table = (
        table
        # Add unique row id right at the input.
        .addfield(
            "rowid",
            partial(
                _CreateRowId,
                fields=(
                    "date",
                    "time",
                    "type",
                    "description",
                    "commissions_fees",
                    "amount",
                    "balance",
                ),
            ),
            index=0,
        )
        # Remove bottom totals line.
        .select("description", lambda v: v != "TOTAL")
        # Convert date/time to a single field.
        .addfield(
            "datetime", partial(ParseDateTimePair, "date", "time"), index=1
        ).cutout("date", "time")
        # Convert numbers to Decimal instances.
        .convert(
            ("misc_fees", "commissions_fees", "amount", "balance"), number.ToDecimal
        )
        # Back out the "Misc Fees" field that is missing using consecutive
        # balances.
        .addfieldusingcontext("misc_fees", _ComputeMiscFees)
    )
    return ParseDescription(table)


def _CreateRowId(r: Record, fields: List[str]) -> str:
    """Create a unique row if from the given field values."""
    md5 = hashlib.blake2s(digest_size=4)
    for fname in fields:
        value = getattr(r, fname)
        md5.update(value.encode("utf8"))
    return md5.hexdigest()


def _ComputeMiscFees(prev: Record, rec: Record, _: Record) -> Decimal:
    """Compute the Misc Fees backed from balance difference."""
    if rec is None or prev is None:
        return ZERO
    diff_balance = rec.balance - prev.balance
    return diff_balance - ((rec.amount or ZERO) + (rec.commissions_fees or ZERO))


def FuturesStatements_Prepare(table: Table) -> Table:
    table = (
        table
        # Add unique row id right at the input.
        .addfield(
            "rowid",
            partial(
                _CreateRowId,
                fields=(
                    "trade_date",
                    "exec_date",
                    "exec_time",
                    "type",
                    "description",
                    "commissions_fees",
                    "misc_fees",
                    "amount",
                    "balance",
                ),
            ),
            index=0,
        )
        # Remove bottom totals line.
        .select("description", lambda v: v != "TOTAL")
        # Convert date/time to a single field.
        .addfield(
            "datetime", partial(ParseDateTimePair, "exec_date", "exec_time"), index=1
        )
        .cutout("exec_date", "exec_time")
        .convert("trade_date", _ParseFuturesDate)
        # Remove dashes from empty fields (making them truly empty).
        .convert(("ref", "misc_fees", "commissions_fees", "amount"), RemoveDashEmpty)
        # Convert numbers to Decimal or integer instances.
        .convert(
            ("misc_fees", "commissions_fees", "amount", "balance"), number.ToDecimal
        )
        .convert("ref", lambda v: int(v) if v else 0)
    )
    return ParseDescription(table)


def _ParseFuturesDate(string: str) -> datetime.date:
    """Parse a date from the futures section."""
    if string == "*":
        return datetime.date.today()
    else:
        return datetime.datetime.strptime(string, "%m/%d/%y").date()


def ForexStatements_Prepare(table: Table) -> Table:
    return []


def GetPutCall(rec: Record) -> str:
    return (
        ("PUT" if rec._instrument.putcall == "P" else "CALL")
        if rec._instrument.strike
        else None
    )


def AccountTradeHistory_Prepare(table: Table) -> Table:
    """Prepare the account trade history table."""

    # Read database for resolving expirations.
    db = mulmat.read_cme_database()
    db_lookup = mulmat.get_expirations_lookup(db)

    table = (
        table
        # Remove empty columns.
        .cutout("col0")
        # Convert date/time fields to objects.
        .convert(
            "exec_time",
            lambda string: datetime.datetime.strptime(string, "%m/%d/%y %H:%M:%S")
            if string
            else None,
        )
        # Fill in missing values.
        .filldown("exec_time")
        .convert(("spread", "order_id"), lambda v: v or None)
        .filldown("spread", "order_id")
        # Convert numbers to Decimal instances.
        .convert(("qty", "price", "strike"), number.ToDecimal)
        # Convert pos effect to single word naming.
        .convert("pos_effect", lambda r: "OPENING" if r == "TO OPEN" else "CLOSING")
        # Convert order ids to integers (because they are).
        .convert("order_id", lambda v: int(v) if v else 0)
        # Infer instrument type.
        .addfield("instype", InferInstrumentType)
        # Generate Beancount symbol from the row.
        .addfield("_instrument", partial(symbols.ToInstrument, db_lookup))
        # TODO(blais): Can we simply replace this antiquated code by instrument.Expand()?
        .addfield("underlying", lambda r: r._instrument.underlying)
        .addfield("expiration", lambda r: r._instrument.expiration)
        .addfield("expcode", lambda r: r._instrument.expcode)
        .addfield("putcall", GetPutCall)
        .addfield("strike", lambda r: r._instrument.strike)
        .addfield("multiplier", lambda r: Decimal(r._instrument.multiplier))
        .cutout("symbol", "exp", "strike", "type")
        .addfield("symbol", lambda r: str(r._instrument))
        .cutout("_instrument")
        # Remove unnecessary fields.
        .cutout("order_type")
        .cutout("net_price")
    )
    return table


def InferInstrumentType(rec: Record) -> str:
    """Infer the instrument type from the rows of the trading table."""
    if rec.type in {"STOCK", "ETF"}:
        assert rec.spread in {"STOCK", "COVERED"}, rec
        # Stock.
        return "Equity"
    elif rec.type == "FUTURE":
        # Futures outright.
        return "Future"
    elif rec.type in {"CALL", "PUT"}:
        if rec.exp.startswith("/"):
            # Process an equity option.
            return "Future Option"
        else:
            return "Equity Option"
    raise ValueError("Could not infer instrument type for {}".format(rec))


def ParseDateTimePair(date_field: str, time_field: str, rec: Record) -> datetime.date:
    """Parse a pair of date and time fields."""
    return datetime.datetime.strptime(
        "{} {}".format(getattr(rec, date_field), getattr(rec, time_field)),
        "%m/%d/%y %H:%M:%S",
    )


def RemoveDashEmpty(value: str) -> str:
    return value if value != "--" else ""


# -------------------------------------------------------------------------------
# Inference from descriptions


def ParseDescription(table: Table) -> Table:
    """Parse description to synthesize the symbol for later, if present.
    This also adds missing entries.
    """
    return (
        table
        # Clean up uselesss prefixed from the descriptions.
        .convert("description", CleanDescriptionPrefixes)
        # Parse the description string and insert new columns.
        .addfield("_desc", _ParseDescriptionRecord)
        .addfield("symbol", lambda r: r._desc.get("symbol", ""))
        .addfield("strategy", lambda r: r._desc.get("strategy", ""))
        .addfield("quantity", lambda r: r._desc.get("quantity", ""))
        .cutout("_desc")
    )


def _ParseDescriptionRecord(row: Record) -> Dict[str, Any]:
    """Parse the description field to a dict."""
    if row.type == "TRD":
        return _ParseTradeDescription(row.description)
    if row.type == "RAD":
        if row.description.startswith("REMOVAL OF OPTION"):
            return _ParseExpirationDescription(row.description)
    if row.type == "DOI":
        if re.match(".* DIVIDEND", row.description):
            return _ParseDividendDescription(row.description)
    return {}


def _ParseTradeDescription(description: str) -> Dict[str, Any]:
    """Parse the description field of a trade."""

    regexp = "".join(
        [
            "(?P<web>TOSWeb )?",
            "(?P<type>MSO )?",
            "(?P<side>BOT|SOLD) ",
            "(?P<quantity>[+-]?[0-9.,]+) ",
            "(?P<rest>.*?)",
            "(?P<price> @-?[0-9.]+)?",
            "(?P<venue> [A-Z]+(?: GEMINI)?)?",
            "$",
        ]
    )
    match = re.match(regexp, description)
    assert match, description
    matches = match.groupdict()
    matches["side"] = "BUY" if matches["side"] == "BOT" else "SELL"
    matches["quantity"] = abs(number.ToDecimal(matches["quantity"]))
    quantity = matches["quantity"]
    matches["price"] = (
        number.ToDecimal(matches["price"].lstrip(" @")) if matches["price"] else ""
    )
    matches["venue"] = matches["venue"].lstrip() if matches["venue"] else ""
    rest = matches.pop("rest")

    underlying = "(?P<underlying>/?[A-Z0-9]+)(?::[A-Z]+)?"
    underlying2 = "(?P<underlying2>/?[A-Z0-9]+)(?::[A-Z]+)?"
    details = "(?P<details>.*)"

    # Standard Options strategies.
    # 'VERTICAL SPY 100 (Weeklys) 8 JAN 21 355/350 PUT'
    # 'IRON CONDOR NFLX 100 (Weeklys) 5 FEB 21 502.5/505/500/497.5 CALL/PUT'
    # 'CONDOR NDX 100 16 APR 21 [AM] 13500/13625/13875/13975 CALL"
    # 'BUTTERFLY GS 100 (Weeklys) 5 FEB 21 300/295/290 PUT'
    # 'VERT ROLL NDX 100 (Weeklys) 29 JAN 21/22 JAN 21 13250/13275/13250/13275 CALL'
    # 'DIAGONAL SPX 100 (Weeklys) 16 APR 21/16 APR 21 [AM] 3990/3995 CALL'
    # 'CALENDAR SPY 100 16 APR 21/19 MAR 21 386 PUT'
    # 'STRANGLE NVDA 100 (Weeklys) 1 APR 21 580/520 CALL/PUT'
    # 'COVERED LIT 100 16 APR 21 64 CALL/LIT'
    match = re.match(
        f"(?P<strategy>"
        f"COVERED|VERTICAL|BUTTERFLY|VERT ROLL|DIAGONAL|CALENDAR|STRANGLE"
        f"|CONDOR|IRON CONDOR) {underlying} {details}",
        rest,
    )
    if match:
        sub = match.groupdict()
        return {
            "strategy": sub["strategy"],
            "quantity": quantity,
            "symbol": sub["underlying"],
        }

    # Custom options combos.
    # '2/2/1/1 ~IRON CONDOR RUT 100 16 APR 21 [AM] 2230/2250/2150/2055 CALL/PUT'
    # '-1 1/2 BACKRATIO /ZSU21:XCBT 1/50 SEP 21 /OZSU21:XCBT 1230/1340 CALL'
    # '1/-1/1/-1 CUSTOM SPX 100 (Weeklys) 16 APR 21/16 APR 21 [AM]/19 MAR 21/19 MAR 21 3990/3980/4000/4010 CALL/CALL/CALL/CALL'
    # '5/-4 CUSTOM SPX 100 16 APR 21 [AM]/16 APR 21 [AM] 3750/3695 PUT/PUT'
    match = re.match(
        rf"(?P<shape>-?\d+(?:/-?\d+)*) (?P<strategy>~IRON CONDOR|CUSTOM|BACKRATIO) "
        f"{underlying} {details}",
        rest,
    )
    if match:
        sub = match.groupdict()
        return {
            "strategy": sub["strategy"],
            "quantity": quantity,
            "symbol": sub["underlying"],
        }

    # Futures calendars.
    match = re.match(f"(?P<strategy>FUT CALENDAR) {underlying}-{underlying2}", rest)
    if match:
        sub = match.groupdict()
        # Note: Return the front month instrument as the underlying.
        return {
            "strategy": sub["strategy"],
            "quantity": quantity,
            "symbol": sub["underlying"],
        }

    # Single option.
    match = re.match(f"{underlying} {details}", rest)
    if match:
        sub = match.groupdict()
        return {"strategy": "SINGLE", "quantity": quantity, "symbol": sub["underlying"]}

    # 'GAMR 100 16 APR 21 100 PUT'  (-> SINGLE)
    match = re.match(rf"{underlying} \d+ {details}", rest)
    if match:
        sub = match.groupdict()
        return {
            "strategy": sub["strategy"],
            "quantity": quantity,
            "symbol": sub["underlying"],
        }

    # Regular stock or future.
    # 'EWW'
    match = re.fullmatch(f"{underlying}", rest)
    if match:
        sub = match.groupdict()
        return {
            "strategy": "OUTRIGHT",
            "quantity": quantity,
            "symbol": sub["underlying"],
        }

    message = "Unknown description: '{}'".format(description)
    raise ValueError(message)


def _ParseDividendDescription(description: str) -> Dict[str, Any]:
    """Parse the description field of an expiration."""
    match = re.match(
        "ORDINARY (?P<strategy>DIVIDEND)~(?P<symbol>[A-Z0-9]+)", description
    )
    assert match, description
    matches = match.groupdict()
    matches["quantity"] = Decimal("0")
    return matches


def _ParseExpirationDescription(description: str) -> Dict[str, Any]:
    """Parse the description field of an expiration."""
    regexp = "".join(
        [
            "REMOVAL OF OPTION DUE TO EXPIRATION ",
            "(?P<quantity>[+-]?[0-9.]+) ",
            "(?P<underlying>[A-Z/:]+) ",
            r"(?P<multiplier>\d+) ",
            r"(?P<suffix>\(.*\) )?",
            r"(?P<expiration>\d+ [A-Z]{3} \d+) ",
            "(?P<strike>[0-9.]+) ",
            "(?P<side>PUT|CALL)",
        ]
    )
    match = re.match(regexp, description)
    assert match, description
    matches = match.groupdict()
    matches["expiration"] = parser.parse(matches["expiration"]).date()
    matches["strike"] = Decimal(matches["strike"])
    matches["multiplier"] = Decimal(matches["multiplier"])
    matches["quantity"] = Decimal(matches["quantity"])
    return {
        "strategy": "EXPIRATION",
        "quantity": Decimal("0"),
        "symbol": matches["underlying"],
    }


# A second version of this that provides all the required detail for any
# instrument.
def _ParseExpirationDescriptionDetailed(rec: Record) -> Dict[str, Any]:
    """Parse the description field of an expiration."""
    regexp = "".join(
        [
            "REMOVAL OF OPTION DUE TO EXPIRATION ",
            "(?P<quantity>[+-]?[0-9.]+) ",
            "(?P<underlying>[A-Z/:]+) ",
            r"(?P<multiplier>\d+) ",
            r"(?P<suffix>\(.*\) )?",
            r"(?P<expiration>\d+ [A-Z]{3} \d+) ",
            "(?P<strike>[0-9.]+) ",
            "(?P<putcall>PUT|CALL)",
        ]
    )
    match = re.match(regexp, rec.description)
    assert match, description
    matches = match.groupdict()

    underlying = matches["underlying"]
    matches["instype"] = (
        "Future Option" if underlying.startswith("/") else "Equity Option"
    )
    matches["expiration"] = parser.parse(matches["expiration"]).date()
    matches["strike"] = Decimal(matches["strike"])
    matches["multiplier"] = Decimal(matches["multiplier"])

    # Note that the TOS cash transaction has the benefit of containing the
    # signed quantity.
    signed_quantity = Decimal(matches["quantity"])
    matches["quantity"] = abs(signed_quantity)
    matches["instruction"] = "SELL" if signed_quantity < ZERO else "BUY"
    return matches


def CleanDescriptionPrefixes(string: str) -> str:
    return re.sub("(WEB:(AA_[A-Z]+|WEB_GRID_SNAP)|tAndroid) ", "", string)


def GetTransactions(filename: str) -> Tuple[Table, Table]:
    """Read and prepare all the tables to be joined."""

    tables = PrepareTables(filename)

    # Pull out the trading log which contains trade information over all the
    # instrument but not any of the fees.
    trade_hist = (
        tables["Account Trade History"]
        # Add an absolute value quantity field.
        .addfield("quantity", lambda r: abs(r.qty))
    )

    # Split up the "Cash Balance" table and process non-trade entries.
    cashbal = tables["Cash Balance"]
    equities_trade, cashbal_nontrade = SplitCashBalance(cashbal, trade_hist)

    # Split up the "Futures Statements" table and process non-trade entries.
    futures = tables["Futures Statements"]
    futures_trade, futures_nontrade = SplitFuturesStatements(futures, trade_hist)

    # Match up the equities and futures statements entries to the trade
    # history and ensure a perfect match, returning groups of (date-time,
    # cash-rows, trade-rows), properly matched.
    equities_groups, futures_groups, equities_rest, futures_rest = ProcessTradeHistory(
        equities_trade, futures_trade, trade_hist
    )

    # Convert matched groups of rows to trnasctions.
    equities_txns = SplitGroupsToTransactions(equities_groups, False)
    futures_txns = SplitGroupsToTransactions(futures_groups, True)

    # Extract and process expirations.
    equities_expi, equities_rest = ProcessExpirationsToTransactions(equities_rest)
    futures_expi, futures_rest = ProcessExpirationsToTransactions(futures_rest)

    # Extract and process dividends.
    equities_divs, equities_rest = ProcessDividends(equities_rest)
    futures_divs, futures_rest = ProcessDividends(futures_rest)

    # Check we processed all transactions and that the rest are empty.
    for rest in equities_rest, futures_rest:
        if rest.nrows() != 0:
            raise ValueError(f"Remaining unprocessed transactions: {rest}")

    # Concatenate the tables.
    fieldnames = equities_txns.columns()
    txns = petl.cat(
        equities_txns,
        equities_expi,
        equities_divs,
        futures_txns,
        futures_expi,
        futures_divs,
    ).sort("datetime")

    # Add a cost column, calculated from the data.
    # Note that for futures contracts this includes the notional value.
    def CalculateCost(r: Record) -> Decimal:
        sign = -1 if r.instruction == "BUY" else +1
        return sign * r.quantity * r.multiplier * r.price

    # Add some more missing columns.
    txns = (
        txns.sort("order_id")
        # Add the account number to the table.
        .addfield("account", utils.GetAccountNumber(filename), index=0)
        # Make up a transaction id. It's a real bummer that the one that's
        # available in the API does not show up anywhere in this file.
        .addfieldusingcontext("order_sequence", OrderSequence)
        .addfield("transaction_id", GetTransactionId)
        .cutout("order_sequence")
        # Convert the order ids to match those from the API.
        .convert("order_id", lambda oid: "T{}".format(oid) if oid else oid)
        # Add a cost row.
        .addfield("cost", CalculateCost)
        .addfield("init", None)
    )

    # Make the final ordering correct and finalize the columns.
    txns = txns.cut(txnlib.FIELDS)

    nontrade = petl.cat(cashbal_nontrade, futures_nontrade)

    return txns, nontrade


def OrderSequence(prv: Optional[int], cur: Optional[int], nxt: Optional[int]) -> int:
    """Return a sequence number for order ids."""
    if (prv is not None and cur.order_id == prv.order_id) or (
        nxt is not None and cur.order_id == nxt.order_id
    ):
        if prv is None or prv.order_sequence is None or prv.order_id != cur.order_id:
            sequence = 1
        else:
            sequence = prv.order_sequence + 1
        return sequence
    else:
        return None


def GetTransactionId(rec: Record) -> str:
    """Make up a unique transaction id."""
    # We use the order id + sequence, if not unique.
    if rec.order_sequence is None:
        return str(rec.order_id)
    else:
        return "{}.{}".format(rec.order_id, rec.order_sequence)


def PrepareTables(filename: str) -> Dict[str, Table]:
    """Clean up all the input tables."""

    # Handlers for each of the sections.
    handlers = {
        "Cash Balance": CashBalance_Prepare,
        "Futures Statements": FuturesStatements_Prepare,
        "Forex Statements": None,
        "Account Order History": None,
        "Account Trade History": AccountTradeHistory_Prepare,
        "Equities": None,
        "Options": None,
        "Futures": None,
        "Futures Options": None,
        "Profits and Losses": None,
        "Forex Account Summary": None,
        "Account Summary": None,
    }

    # Read the CSV file.
    prepared_tables = {}
    with open(filename, encoding="utf8") as infile:
        # Iterate through the sections.
        sections = csv_utils.csv_split_sections_with_titles(csv.reader(infile))
        for section_name, rows in sections.items():
            handler = handlers.get(section_name, None)
            if not handler:
                continue
            header = csv_utils.csv_clean_header(rows[0])
            rows[0] = header
            table = petl.wrap(rows)
            ptable = handler(table)
            if ptable is None:
                continue
            prepared_tables[section_name] = ptable

    return prepared_tables


def ImportTransactions(config: config_pb2.Config) -> petl.Table:
    pattern = path.expandvars(config.thinkorswim_account_statement_csv_file_pattern)
    fnmap = discovery.GetLatestFilePerYear(pattern)
    transactions_list = []
    for year, filename in sorted(fnmap.items()):
        transactions, _ = GetTransactions(filename)
        transactions_list.append(
            transactions.select(lambda r, y=year: r.datetime.year == y)
        )

    return petl.cat(*transactions_list)


def ImportNonTrades(config: config_pb2.Config) -> petl.Table:
    pattern = path.expandvars(config.thinkorswim_account_statement_csv_file_pattern)
    fnmap = discovery.GetLatestFilePerYear(pattern)
    transactions_list = []
    other_list = []
    for year, filename in sorted(fnmap.items()):
        _, other = GetTransactions(filename)
        other = other.select(lambda r, y=year: r.datetime.year == y)
        other_list.append(other)
    return petl.cat(*other_list)


@click.command()
@click.argument("source", type=click.Path())
@click.option("--cash", is_flag=True, help="Print out cash transactions.")
def main(source: str, cash: bool):
    """Simple local runner for this translator.

    `source` is a globbing pattern matching the files we can process.
    """
    alltypes = ImportAll(source, None)

    if 0:
        other = alltypes[Account.OTHER]
        print(other.lookallstr())

        if 0:
            if 0:
                for rec in other.aggregate("type", WrapRecords).records():
                    print(rec.value.lookallstr())
            else:
                nother = nontrades.ConvertNonTrades(other)
                for rec in nother.aggregate("type", WrapRecords).records():
                    print(rec.value.lookallstr())
                # print(nother.lookallstr())

    if 1:
        transactions = alltypes[Account.TRANSACTIONS]
        print(transactions.lookallstr())


if __name__ == "__main__":
    main()
