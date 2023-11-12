"""A new version of the matching code, which integrates state-based processing.

This code processes a transactions log in order and matches reductions of
positions against each other, in order to:

- Set the position effect where it is missing in the input,
- Split rows for futures crossing the null position boundary,
- Compute expiration quantities and signs,
- Creates a corresponding match id column to save this in the table.
- Add missing expiration rows (thinkorswim suffers from some of these),
- Add mark rows for positions still open and active,

Note that adding opening rows for positions created before the transactions time
window is done separately (see discovery.ReadInitialPositions).

The purpose is to (a) relax the requirements on the particular importers and (b)
run through a single loop of this expensive accumulation (for performance
reasons). The result we seek is a log with the ability to be processed without
any state accumulation--all fields correctly rectified with proper
opening/closing effect and opening and marking entries. This makes any further
processing much easier and faster.

All of this processing requires state accumulation and correction for missing
information before the beginning of the transaction log, and is reasonably
difficult to factor into independent pieces, because the corrections affect the
rest of the computations.

Basically, if you have only a partial window of time of a transactions log, you
have to be ready to reconcile:

1. positions opened and closed within the window
2. positions opened BEFORE the window and closed within the window
3. positions opened within the window and never closed (or closed after the window, if the window isn't up to now)
4. positions opened before the window, and never closed during the window (or closed after the window)

Comments:

- (1) is never a problem.
- (2) will require synthesizing a fake opening trade, and can be detected by
  their unmatched closing
- (3) is going to result in residual positions leftover in the inventory
  accumulator after processing the log
- (4) is basically undetectable, except if you have a positions file to
  reconcile against (you will find positions you didn't expect and for those you
  need to synthesize opens for those)

Finally, we'd like to do away with a "current positions" log; if one is
provided, the positions computed from this code can be asserted and missing
positions can be inserted.

"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from decimal import Decimal
from functools import partial
from typing import List, Mapping, NamedTuple, Optional
import collections
import datetime
import enum
import hashlib
import itertools

from johnny.base.etl import petl, AssertColumns, Record, Table
from johnny.base import instrument
from johnny.base import inventories
from johnny.base import transactions as txnlib


ZERO = Decimal(0)
Q = Decimal("0.01")


# TODO(blais): Rename this.
class ShortBasisReportingMethod(enum.Enum):
    """How short positions cost and proceeds are handled."""

    # Do nothing, keep the numbers inverted.
    NONE = "none"

    # Swap cost and proceeds. TODO(blais): Rename this.
    INVERT = "invert"

    # Nullify the cost and set proceeds equal to P/L.
    NULLIFY = "nullify"


class InstKey(NamedTuple):
    """Instrument key."""

    account: str
    symbol: str


def Process(
    transactions: Table,
    mark_time: Optional[datetime.datetime] = None,
    debug: bool = False,
) -> Table:
    """Run state-based processing over the transactions log.

    Args:
      transactions: The table of transactions, as normalized by each importer code.
      mark_time: The datetime to use for marking position.
    Returns:
      A fixed up table of processed, transformed and normalized transactions, as
      per the description of this module.
    """
    AssertColumns(
        transactions,
        ("account", str),
        ("transaction_id", str),
        ("datetime", datetime.datetime),
        ("rowtype", str),
        ("order_id", str),
        ("symbol", str),
        ("effect", str),
        ("instruction", str),
        ("quantity", Decimal),
        ("price", Decimal),
        ("cost", Decimal),
        ("cash", Decimal),
        ("commissions", Decimal),
        ("fees", Decimal),
        ("description", str),
    )

    invs = collections.defaultdict(
        lambda: inventories.OpenCloseFifoInventory(debug=debug)
    )

    # Accumulator for new records to output.
    new_rows = []

    def accum(nrec, _):
        new_rows.append(nrec)

    # Note: Unfortunately we require two sortings; one to ensure that inventory
    # matching is done in time order, and a final one to reorder the newly
    # synthesized outputs {123a4903c212}.
    transactions = transactions.addfield("match_id", "").sort("datetime")
    for rec in transactions.namedtuples():
        inv = invs[InstKey(rec.account, rec.symbol)]

        if rec.rowtype in {txnlib.Type.Trade, txnlib.Type.Open}:
            if rec.effect == "OPENING":
                inv.opening(rec, accum)
            elif rec.effect == "CLOSING":
                inv.closing(rec, accum)
            else:
                assert not rec.effect
                inv.match(rec, accum)

        elif rec.rowtype in {
            txnlib.Type.Expire,
            txnlib.Type.Assign,
            txnlib.Type.Exercise,
        }:
            inv.expire(rec, accum, rec.rowtype)

        elif rec.rowtype in {txnlib.Type.Cash}:
            inv.receive(rec, accum, rec.rowtype)

        else:
            raise ValueError(f"Invalid row type: {rec.rowtype}")

    if mark_time is None:
        mark_time = _GetMarkTime()

    # Insert missing expirations.
    prototype = type(rec)(*[None] * len(transactions.header()))
    _AddMissingExpirations(invs, mark_time, accum, prototype)

    # Add closing transactions for existing positions.
    _AddMarkTransactions(invs, mark_time, accum, prototype)

    # Note: We sort again to ensure newly synthesized rows are ordered in
    # datetime order {123a4903c212}.
    return petl.wrap(itertools.chain([transactions.header()], new_rows)).sort(
        "datetime"
    )


def _GetMarkTime() -> datetime.datetime:
    """Get the mark time date. Override for tests."""
    return datetime.datetime.now().replace(microsecond=0)


def _GetOrderIdFromSymbol(symbol: str, digest_size: int) -> str:
    """Make up a unique order id for an expiration."""
    md5 = hashlib.blake2s(digest_size=digest_size)
    md5.update(symbol.encode("ascii"))
    return md5.hexdigest()


def _AddMissingExpirations(
    invs: Mapping[str, Decimal],
    mark_time: datetime.datetime,
    accum: inventories.TxnAccumFn,
    prototype_row: tuple,
):
    """Create missing expirations. Some sources miss them."""

    mark_date = mark_time.date()
    for key, inv in sorted(invs.items()):
        inst = instrument.FromString(key.symbol)
        if (
            inst.expiration is not None
            and inst.expiration < mark_date
            and inv.quantity() != ZERO
        ):
            expiration_time = datetime.datetime.combine(
                inst.expiration + datetime.timedelta(days=1), datetime.time(0, 0, 0)
            )
            rec = prototype_row._replace(
                transaction_id=_GetOrderIdFromSymbol(key.symbol, 6),
                order_id=_GetOrderIdFromSymbol(key.symbol, 4),
                account=key.account,
                symbol=key.symbol,
                datetime=expiration_time,
                description=f"Synthetic expiration for {key.symbol}",
                cost=ZERO,
                price=ZERO,
                cash=ZERO,
                quantity=ZERO,
                commissions=ZERO,
                fees=ZERO,
            )
            inv.expire(rec, accum, txnlib.Type.Expire)


def _AddMarkTransactions(
    invs: Mapping[str, Decimal],
    mark_time: datetime.datetime,
    accum: inventories.TxnAccumFn,
    prototype_row: tuple,
):
    """Add mark transactions to close residual inventory positions."""

    for key, inv in sorted(invs.items()):
        pquantity = inv.quantity()
        if pquantity == ZERO:
            continue

        # Note: We should be able to ignore the input record because this
        # inventory, having an unclosed position, should already have a valid
        # match id. An error would be raised here otherwise.
        match_id = inv.get_match_id(None)

        # Compute a transaction id that will be invariable. Each symbol can only
        # be marked once, so we use a hash on that.
        h = hashlib.blake2s(digest_size=6)
        h.update(key.account.encode("ascii"))
        h.update(key.symbol.encode("ascii"))
        transaction_id = "mark-{}".format(h.hexdigest()[:6])

        rec = prototype_row._replace(
            account=key.account,
            transaction_id=transaction_id,
            symbol=key.symbol,
            datetime=mark_time,
            description=f"Mark for {key.symbol}",
            cost=ZERO,
            price=ZERO,
            quantity=abs(pquantity),
            cash=ZERO,
            commissions=ZERO,
            fees=ZERO,
            rowtype=txnlib.Type.Mark,
            instruction=("SELL" if pquantity >= 0 else "BUY"),
            effect="CLOSING",
            match_id=match_id,
        )
        accum(rec, "MARK")


def GetChainMatchesFromTransactions(
    txns: Table, short_method: ShortBasisReportingMethod
) -> Table:
    """Extract a list of trade matches from the list of transactions.

    Note: This will work only for a single chain.
    """
    chain_ids = set(txns.values("chain_id"))
    if len(chain_ids) > 1:
        raise ValueError(
            "GetChainMatchesFromTransactions() is called for multiple chains. "
            "This is an error"
        )
    chain_id = next(iter(chain_ids))
    ctxns = (
        txns.selectin("effect", {"OPENING", "CLOSING"})  # Remove Dividends.
        .movefield("chain_id", 0)
        .movefield("account", 1)
        .convert("chain_id", lambda _: "")
        .sort(["match_id", "datetime"])
        .applyfn(instrument.Expand, "symbol")
    )

    # Append a list of aggregated matches for the purpose of reporting.
    funcs = {
        "date_acquired": partial(_DateSub, "OPENING"),
        "date_disposed": partial(_DateSub, "CLOSING"),
        "date_min": ("datetime", lambda g: min(g).date()),
        "date_max": ("datetime", lambda g: max(g).date()),
        "cost": _CostOpened,
        "proceeds": _CostClosed,
        "futures_notional_open": _FuturesNotionalOpen,
        "account": ("account", lambda g: next(iter(g))),
        "symbol": ("symbol", lambda g: next(iter(set(g)))),
        "instype": ("instype", lambda g: next(iter(set(g)))),
        "quantity": _EstimateMatchQuantity,
        "long_short": _LongShortIndicator,
        "infer_term": _LongTermShortTerm,
    }
    cmatches = ctxns.aggregate("match_id", funcs)

    # For futures contracts, remove notional value from cost and add the
    # corresponding notional to proceeds. Opening futures positions should
    # have 0 cost (excluding commissions and fees) and closing positions
    # should be the matched P/L. This should produce proceeds and cost
    # numbers much closer to those on the 1099s.
    denotionalize_futures = True
    if denotionalize_futures:
        cmatches = cmatches.convert(
            "cost",
            lambda _, r: r.cost - r.futures_notional_open,
            pass_row=True,
        ).convert(
            "proceeds",
            lambda _, r: r.proceeds + r.futures_notional_open,
            pass_row=True,
        )

    cmatches = (
        cmatches.addfield("chain_id", chain_id)
        .addfield("pnl", lambda r: (r.proceeds + r.cost).quantize(Q))
        .convert("proceeds", lambda v: v.quantize(Q))
        # Flip the signs on cost, so that pnl = proceeds - cost, not proceeds + cost.
        .convert("cost", lambda v: -v.quantize(Q))
    )

    # Handle P/L specially on short options.
    if short_method == ShortBasisReportingMethod.INVERT:
        cmatches = _ShortOptionsInvert(cmatches)
    elif short_method == ShortBasisReportingMethod.NULLIFY:
        cmatches = _ShortOptionsNullify(cmatches)

    return cmatches.cut(
        # "description",
        "date_acquired",
        "date_disposed",
        "cost",
        "proceeds",
        "pnl",
        "long_short",
        # "*",
        "match_id",
        "symbol",
        "quantity",
        "date_min",
        "date_max",
        # "instype",
        # "underlying",
        "account",
        "chain_id",
        "infer_term",
        # "category",
    )


def _DateSub(effect: str, rows: List[Record]) -> str:
    dtimes = set([r.datetime.date() for r in rows if r.effect == effect])
    if len(dtimes) > 1:
        return "Various"
    else:
        return next(iter(dtimes)).isoformat()


def _EstimateMatchQuantity(rows: List[Record]) -> int:
    return sum(r.quantity for r in rows if r.effect == "OPENING")


def _LongShortIndicator(rows: List[Record]) -> int:
    """Whether we bought or sold."""
    first_row = min(rows, key=lambda row: row.datetime)
    return first_row.instruction


ONE_YEAR = datetime.timedelta(days=365)


def _LongTermShortTerm(rows: List[Record]) -> int:
    """Attempt to identify Long-term vs. short-term of a match."""
    irows = iter(rows)
    opening_date = next(irows).datetime.date()
    lt_st = set((row.datetime.date() - opening_date) >= ONE_YEAR for row in irows)
    if len(lt_st) == 1:
        return "LT" if lt_st.pop() else "ST"
    else:
        return "?"


def _CostOpened(rows: List[Record]) -> str:
    return sum((r.cost + r.commissions + r.fees) for r in rows if r.effect == "OPENING")


def _CostClosed(rows: List[Record]) -> str:
    return sum((r.cost + r.commissions + r.fees) for r in rows if r.effect == "CLOSING")


def _FuturesNotionalOpen(rows: List[Record]) -> Decimal:
    return sum(r.cost for r in rows if r.instype == "Future" and r.effect == "OPENING")


def _ShortOptionsNullify(matches: Table) -> Table:
    """
    On short options sales, nullify the cost basis as per the following rule:
    https://support.tastytrade.com/support/solutions/articles/43000615420--0-00-cost-basis-on-short-equity-options-
    """
    return (
        matches.addfield(
            "null",
            lambda r: (
                r.instype == "EquityOption" and r.cost <= ZERO and r.proceeds <= ZERO
            ),
        )
        .addfield("offset", lambda r: -r.cost)
        .addfield("cost_null", lambda r: r.cost + r.offset if r.null else r.cost)
        .addfield(
            "proceeds_null", lambda r: r.proceeds + r.offset if r.null else r.proceeds
        )
        .cutout("null", "offset", "cost", "proceeds")
        .rename({"cost_null": "cost", "proceeds_null": "proceeds"})
    )


def _ShortOptionsInvert(matches: Table) -> Table:
    """
    Invert sell-to-open then buy-to-close in order to have positive
    numbers. We swap the dates, swap cost and proceeds and swap the signs
    on them. The P/L should be the same.
    """
    return (
        matches.addfield("inv", lambda r: r.cost <= ZERO and r.proceeds <= ZERO)
        .addfield("cost_inv", lambda r: -r.proceeds if r.inv else r.cost)
        .addfield("proceeds_inv", lambda r: -r.cost if r.inv else r.proceeds)
        .cutout("inv", "cost", "proceeds")
        .rename({"cost_inv": "cost", "proceeds_inv": "proceeds"})
    )
