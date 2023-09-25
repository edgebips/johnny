"""Module to compute chains of related transactions over time.

Transactions are grouped together if

- They share the same order id (i.e., multiple legs of a single order
  placement).

- If they reduce each other, e.g., a closing transaction is grouped with its
  corresponding transaction.

- If they overlap in time (optionally). The idea is to identify "episodes" where
  the position went flat for the given instrument.

This code is designed to be independent of the source, as we went to be able to
do this on all options platforms, such as Ameritrade, InteractiveBrokers,
Vanguard and Tastyworks.
"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from functools import partial
from decimal import Decimal
from typing import Any, Iterator, List, Mapping, Optional, Tuple, Union, Dict, Set
import functools
import string
import hashlib
import math
import copy
import sys
import collections
import datetime
import itertools
import logging

from more_itertools import first
import networkx as nx
import pyarrow as pa
import pyarrow.parquet as pq

from johnny.base import config as configlib
from johnny.base import strategy as strategylib
from johnny.base import instrument
from johnny.base import inventories
from johnny.base import mark
from johnny.base.etl import AssertColumns, Record, Table, WrapRecords
from johnny.utils import timing

ChainStatus = configlib.ChainStatus
Chain = configlib.Chain
Chains = configlib.Chains


ZERO = Decimal(0)
Q0 = Decimal("1")
Q1 = Decimal("0.1")
Q2 = Decimal("0.01")
Q3 = Decimal("0.001")


def ChainTransactions(
    matched_transactions: Table, chains_db: Chains
) -> Tuple[Table, Chains]:
    """Cluster the transactions and return a new table, with added 'chain_id' and an
    update chains configuration on a config object."""

    log = functools.partial(timing.log_time, log_timings=logging.info, indent=1)

    # Clean up the configuration before clustering with it as a side-input.
    with log("scrub"):
        clean_chains_db = ScrubConfig(matched_transactions, chains_db)

    # Run the chains heuristic. (Note: We need to temporarily expand the
    # instrument fields, as they are needed by the match and chains modules.)
    with log("group"):
        chained_transactions = (
            matched_transactions.applyfn(instrument.Expand, "symbol")
            .applyfn(Group, clean_chains_db.chains)
            .applyfn(instrument.Shrink)
        )

    with log("update"):
        updated_chains_db = UpdateConfig(chained_transactions, clean_chains_db)

    return chained_transactions, updated_chains_db


def Group(
    transactions: Table, chains: List[Chain], by_match=True, by_order=True, by_time=True
) -> Table:
    """Cluster transactions to create options chains.

    This function inserts the `chain_id` column to the table and returns a
    modified transactions table.

    Args:
      transactions: A normalized transactions log with a 'match' column.
      chains: A list of pre-existing chains from the input. This is used to extract out
        finalized chains, and partially settled ones (i.e., with `ids` set).
      by_match: A flag, indicating that matching transactions should be chained.
      by_order: A flag, indicating that transactions from the same order should be chained.
      by_time: A flag, indicating that transactions overlapping over time should be chained.
    Returns:
      A modified table with an extra "chain" column, identifying groups of
      related transactions, by episode, or chain.
    """
    # Extract finalized chains explicitly. They don't have to be part of the graph.
    final_chains, match_chains = [], []
    for chain in chains:
        (final_chains if chain.status == ChainStatus.FINAL else match_chains).append(
            chain
        )
    final_chain_ids = {chain.chain_id for chain in final_chains}

    # Initialize the resulting mapping of transactions to chains with finalized
    # chains.
    final_txn_chain_map = {
        transaction_id: chain.chain_id
        for chain in final_chains
        for transaction_id in chain.ids
    }

    # Select only remaining transactions not accounted for by final chains.
    match_transactions = transactions.selectnotin("transaction_id", final_txn_chain_map)

    # Update the chain mapping with chains that are active or closed, so
    # explicit names can resolve.
    txn_chain_map = final_txn_chain_map.copy()
    txn_chain_map.update(
        {
            transaction_id: chain.chain_id
            for chain in match_chains
            for transaction_id in chain.ids
        }
    )

    # Create a graph and process each connected component to an individual trade.
    graph = CreateGraph(match_transactions, match_chains, by_match, by_order, by_time)
    for cc in nx.connected_components(graph):
        chain_txns = []
        for transaction_id in cc:
            node = graph.nodes[transaction_id]
            try:
                unused_node_type = node["type"]
            except KeyError:
                raise KeyError("Node without type for: {}".format(transaction_id))
            if node["type"] == "txn":
                chain_txns.append(node["rec"])
        assert chain_txns, "Invalid empty chain: {}".format(cc)

        chain_id = ChainName(chain_txns, txn_chain_map)

        # This should never happen; but if you somehow used the wrong
        # transaction - with the same (account, datetime, underlying) in a final
        # chain, it could. Best is to adjust the input file, but we could
        # eventually just insert a random character here.
        assert (
            chain_id not in final_chain_ids
        ), f"Collision with FINAL chain names at '{chain_id}'."

        # Add tagged transactions to the chain map.
        for rec in chain_txns:
            txn_chain_map[rec.transaction_id] = chain_id

    return transactions.addfield("chain_id", lambda r: txn_chain_map[r.transaction_id])


def CreateGraph(
    transactions: Table,
    chains: List[Chain],
    by_match=True,
    by_order=True,
    by_time=True,
    explicit_chain_map=None,
) -> nx.Graph:
    """Create a graph to link together related transactions."""

    AssertColumns(
        transactions,
        ("transaction_id", str),
        ("order_id", {None, str}),
        ("match_id", str),
        ("datetime", datetime.datetime),
        ("expiration", {None, datetime.date}),
        ("account", str),
        ("underlying", str),
    )

    # Create a mapping of transaction id to their chain id.
    chain_map = {
        transaction_id: chain.chain_id
        for chain in chains
        for transaction_id in chain.ids
    }

    graph = nx.Graph()
    for rec in transactions.records():
        graph.add_node(rec.transaction_id, type="txn", rec=rec)

        # Link together explicit chains that aren't finalized.
        explicit_chain = chain_map.pop(rec.transaction_id, None)
        if explicit_chain:
            graph.add_node(explicit_chain, type="expchain")
            graph.add_edge(rec.transaction_id, explicit_chain)

        # Link together by order id.
        if by_order:
            if rec.order_id:
                graph.add_node(rec.order_id, type="order")
                graph.add_edge(rec.transaction_id, rec.order_id)

        # Link together by match id.
        if by_match:
            if rec.match_id:
                graph.add_node(rec.match_id, type="match")
                graph.add_edge(rec.transaction_id, rec.match_id)

    # Link together matches that overlap in underlying and time.
    if by_time:
        links, transaction_links = _LinkByOverlapping(transactions)

        ids = set(id1 for id1, _ in links)
        ids.update(id2 for _, id2 in links)
        ids.update(id1 for id1, _ in transaction_links)
        for idx in ids:
            graph.add_node(idx, type="time")
        for id1, id2 in itertools.chain(links, transaction_links):
            graph.add_edge(id1, id2)

    for item in chain_map.items():
        logging.warning(f"Explicit transaction id from chains not seen in log: {item}")

    return graph


def _GetExpiration(rec: Record) -> Union[datetime.date, str]:
    """Get a unique expiration date or code for the instrument."""
    return rec.expiration or rec.expcode


def _LinkByOverlapping(transactions: Table) -> List[Tuple[str, str]]:
    """Return pairs of linked matches, linking all transactions where either of (a)
    an outright position exists in that underlying and/or (b) a common
    expiration exists in that underlying. We're not bothering to inspect
    match_id at all. This is a bit more general and correct than
    _LinkByOverlappingMatch().

    Note that this function also supports dividends and ties them into
    transactions with matching positions in the underlying present.
    """
    AssertColumns(
        transactions,
        ("transaction_id", str),
        ("instruction", str),
        ("symbol", str),
        ("quantity", Decimal),
        ("account", str),
        ("underlying", str),
        ("expiration", {None, datetime.date}),
    )

    class Term:
        """All the positions associated with an expiration term.
        A unique id is associated with each of the terms."""

        def __init__(self, term_id):
            self.id = term_id
            # A mapping of option name to outstanding quantity for that name.
            self.quantities = collections.defaultdict(Decimal)

        def __repr__(self):
            return "<Term {} {}>".format(self.id, self.quantities)

    # Run through matching in order to figure out overlaps.
    # This is a mapping of (account, underlying) to a mapping of (expiration, Term).
    # A Term object contains all the options positions for that expiration in
    # the `quantities` attribute.
    # `expiration` can be `None` in order to track positions in the underlying.
    inventory = collections.defaultdict(dict)
    links = []
    transaction_links = []
    idgen = iter(itertools.count(start=1))
    for rec in transactions.records():
        # Get a mapping for each underlying. In that submapping, the special key
        # 'None' refers to the position of the underlying outright.
        undermap = inventory[(rec.account, rec.underlying)]

        # Potentially allocate a new position for the expiration (or lack
        # thereof). (Note that undermap is mutating if the key is new.) Also
        # note that in order to open separate positions in the same terms, we
        # need to insert a unique id.
        expiration = _GetExpiration(rec)
        isnew = expiration not in undermap
        if rec["rowtype"] == "Dividend":
            # There needs to be an active underlying, otherwise why are we
            # receiving a dividend?.
            assert not isnew

        term_id = "{}/{}/{}/{}".format(
            rec.account, rec.underlying, expiration, next(idgen)
        )
        term = undermap.get(expiration, None)
        if term is None:
            term = undermap[expiration] = Term(term_id)

        if isnew:
            if expiration is None and rec.rowtype != "Dividend":
                # This is an underlying, not an option on one.
                # Link it to all the currently active expirations.
                for uexpiration, expiration_term in undermap.items():
                    if uexpiration is None:
                        continue
                    links.append((term.id, expiration_term.id))
            else:
                # This is an option.
                # Link it to the underlying if it is active.
                if None in undermap:
                    outright_term = undermap[None]
                    links.append((term.id, outright_term.id))
        else:
            if rec.rowtype == "Dividend":
                # This is a dividend.
                # Link it to the underlying.
                # Assert that it's active.
                if None in undermap:
                    outright_term = undermap[None]
                    links.append((term.id, outright_term.id))

        # Link this record (by transaction id) to this term.
        transaction_links.append((term.id, rec.transaction_id))

        # Update quantities.
        if rec.rowtype != "Dividend":
            sign = -1 if rec.instruction == "SELL" else +1
            term.quantities[rec.symbol] += sign * rec.quantity
            if term.quantities[rec.symbol] == ZERO:
                del term.quantities[rec.symbol]

        if not term.quantities:
            del undermap[expiration]

    # Sanity check. All positions should have been closed (`Mark` rows close
    # outstanding positions) and the resulting inventory should be completely
    # empty.
    inventory = {key: undermap for key, undermap in inventory.items() if undermap}
    if inventory:
        logging.error("Inventory not empty: {}".format(inventory))

    return links, transaction_links


def _CreateChainId(transaction_id: str, _: datetime.datetime) -> str:
    """Create a unique match id from the given transaction id."""
    md5 = hashlib.blake2s(digest_size=4)
    md5.update(transaction_id.encode("ascii"))
    return "{}".format(md5.hexdigest())


def ChainName(txns: List[Record], chain_map: Mapping[str, str]):
    """Generate a unique chain name."""

    # Look for an explicit chain id from one of the transactions.
    sorted_txns = sorted(txns, key=lambda r: (r.datetime, r.underlying))
    explicit_chain_ids = set()
    for txn in sorted_txns:
        explicit_chain_id = chain_map.get(txn.transaction_id, None)
        if explicit_chain_id is not None:
            explicit_chain_ids.add(explicit_chain_id)
    num_ids = len(explicit_chain_ids)
    if num_ids == 1:
        return first(explicit_chain_ids)
    elif num_ids > 1:
        logging.error(
            "Multiple explicit chains for cluster {}: {}".format(
                [t.transaction_id for t in txns], explicit_chain_ids
            )
        )

    # Note: We don't know the max date, so we stick with the front date only in
    # the readable chain name.
    first_txn = next(iter(sorted_txns))
    chain_id = ".".join(
        [
            first_txn.account,
            "{:%y%m%d_%H%M%S}".format(first_txn.datetime),
            first_txn.underlying.lstrip("/"),
        ]
    )
    return chain_id


# Initial threshold within which we consider the orders a part of the initial
# order. This allows us some time flexibility to leg in orders.
INITIAL_ORDER_THRESHOLD = datetime.timedelta(seconds=300)


def InitialTransactions(
    pairs: Iterator[Tuple[str, str, datetime.datetime, str]]
) -> Decimal:
    """Extract the transaction ids for the initial order. This input are tupled of
    (transaction_id, order_id, datetime, effect) for each transaction."""
    init_order_id = None
    init_txns = []
    init_dt = None
    for transaction_id, order_id, dt, effect in sorted(
        pairs, key=lambda r: (r[2], r[0])
    ):
        if effect == "CLOSING":
            continue
        if (
            init_order_id is not None
            and order_id != init_order_id
            and (dt - init_dt) > INITIAL_ORDER_THRESHOLD
        ):
            break
        init_order_id = order_id
        init_dt = dt
        init_txns.append(transaction_id)
    return init_txns


def MarkTransactions(pairs: Iterator[Tuple[str, str]]) -> Decimal:
    """Extract the active positions transactions."""
    return [transaction_id for transaction_id, rowtype in pairs if rowtype == "Mark"]


def InitialCredits(rec: Record) -> Decimal:
    """Compute the initial credits from a group of chain rows."""
    return sum(rec.cost for rec in rec.init_txns)


def PositionCost(rec: Record) -> Decimal:
    """Compute the cost of the position from a group of chain rows, using FIFO
    matching."""
    if not any(rec.rowtype == "Mark" for rec in rec.txns):
        return

    inv = collections.defaultdict(inventories.FifoInventory)
    for txn in rec.txns:
        if txn.rowtype in {"Mark", "Dividend"}:
            continue
        sign = +1 if txn.instruction == "SELL" else -1
        unit_cost = abs(txn.cost / txn.quantity)
        inv[txn.symbol].match(sign * txn.quantity, unit_cost, txn.transaction_id)

    return sum(syminv.cost() for syminv in inv.values())


def _CalculateNetLiq(pairs: Iterator[Tuple[str, Decimal]]):
    return Decimal(sum(cost for rowtype, cost in pairs if rowtype == "Mark")).quantize(
        Q2
    )


def _CalculateCash(pairs: Iterator[Tuple[str, Decimal]]):
    return Decimal(sum(cost for rowtype, cost in pairs if rowtype == "Dividend")).quantize(
        Q2
    )


def _GetUnderlyings(symbols: Iterator[str]) -> List[str]:
    return ",".join(
        sorted(set(instrument.FromString(symbol).underlying for symbol in symbols))
    )


def _GetChain(chain_map, rec: Record) -> Chain:
    """Get the chain from the id."""
    chain = chain_map.get(rec.chain_id, None)
    if chain is None:
        logging.warning(f"Could not get chain '{rec.chain_id}'")
        return None
    return chain


def _GetChainAttribute(attrname: str, rec: Record) -> Any:
    """Get the chain from the id."""
    return None if rec.chain is None else getattr(rec.chain, attrname)


DEFAULT_POP50 = 0.80
DEFAULT_TARGET_FRAC = 0.50

Probabilities = collections.namedtuple(
    "Probabilities", ["pop", "target", "pnl_win", "pnl_loss"]
)


def _CalculateProbabilities(rec: Record) -> Optional[Decimal]:
    """Pull the explicit win target or compute it using simple Kelly criterion."""
    chain = rec.chain
    if chain is None:
        return None
    pop = chain.pop or DEFAULT_POP50
    assert 0 < pop < 1
    target_frac = chain.target or DEFAULT_TARGET_FRAC
    assert 0 < target_frac
    pnl_win = abs(float(rec.init)) * target_frac
    pnl_loss = pnl_win * (pop / (1 - pop))
    return Probabilities(
        Decimal(pop).quantize(Q2),
        Decimal(target_frac).quantize(Q2),
        Decimal(pnl_win).quantize(Q2),
        Decimal(-pnl_loss).quantize(Q2),
    )


# Threshold under which successive trades are considered a single one. We look
# for a gap at least this large in order to count distinct adjustments.
MIN_TIME_GAP = datetime.timedelta(seconds=10 * 60)


def _NumAdjustments(rec: Record) -> int:
    """Compute the number of adjustments made to a chain, excluding the initial
    opening and closing."""

    exclude_ids = set(t.transaction_id for t in rec.init_txns)
    last_time = datetime.datetime.fromtimestamp(0)
    num_adjustments = 0
    for txn in sorted(rec.txns, key=lambda t: t.datetime):
        # Don't count opening transactions.
        if txn.transaction_id in exclude_ids:
            continue
        # Look for a large enough time gap.
        if txn.datetime - last_time > MIN_TIME_GAP:
            num_adjustments += 1
        last_time = txn.datetime

    # Don't count closing transactions.
    num_adjustments -= 1
    return num_adjustments


def _CalculatePnlFrac(r: Record) -> Decimal:
    """Calculate the P/L fraction."""
    denom = r.pnl_win if r.pnl_chain * r.pnl_win > 0 else -r.pnl_loss
    return (r.pnl_chain / denom).quantize(Q2) if denom else ZERO


Returns = collections.namedtuple("Returns", "volatility returns move stdev")


# TODO(blais): Compute IRR annualized returns using cash flows.
def GetReturns(fieldname: str, rec: Record) -> float:
    volatility = getattr(rec.chain, fieldname, 0.0) or 0.0
    days = (rec.maxdate - rec.mindate).days
    time = days / 365
    total_vol = Decimal(math.sqrt((volatility**2) * time))
    cost = -rec.init
    mark = rec.net_liq
    returns = rec.pnl_chain / cost
    move = Decimal(total_vol * cost).quantize(Q2) if volatility else None
    stdev = (returns / total_vol).quantize(Q2) if volatility else None
    return Returns(
        Decimal(total_vol).quantize(Q3), Decimal(returns).quantize(Q3), move, stdev
    )


def TransactionsTableToChainsTable(
    transactions: Table, chains_db: Chains
) -> Tuple[Table, Table]:
    """Aggregate a table of already identified transactions row (with a `chain_id` column)
    to a table of aggregated chains. The `config` object is used to join attributes in the
    table.

    A new table of transactions is returned. It will contain additional columns,
    columns that can only be computed with the chains data, such as flags that
    mark rows as initial or not.
    """

    agg = {
        "txns": ("transaction_id", list),
        "init_txns": (
            ("transaction_id", "order_id", "datetime", "effect"),
            InitialTransactions,
        ),
        "mark_txns": (("transaction_id", "rowtype"), MarkTransactions),
        "account": ("account", first),
        "mindate": ("datetime", lambda g: min(g).date()),
        "maxdate": ("datetime", lambda g: max(g).date()),
        "underlyings": ("symbol", _GetUnderlyings),
        "pnl_chain": ("cost", lambda vlist: sum(vlist).quantize(Q2)),
        "net_liq": (("rowtype", "cost"), _CalculateNetLiq),
        "commissions": ("commissions", sum),
        "fees": ("fees", sum),
        "pnl_cash": (("rowtype", "cash"), _CalculateCash),
    }

    transaction_map = transactions.recordlookupone("transaction_id")
    chain_map = {c.chain_id: c for c in chains_db.chains}

    chains_table = (
        transactions
        # Add the underlying.
        .addfield("underlying", lambda r: instrument.ParseUnderlying(r.symbol))
        # Aggregate over the chain id.
        .aggregate("chain_id", agg)
        .convert("txns", lambda txns: list(map(transaction_map.__getitem__, txns)))
        .convert("init_txns", lambda txns: list(map(transaction_map.__getitem__, txns)))
        .convert("mark_txns", lambda txns: list(map(transaction_map.__getitem__, txns)))
        # Add calculations off the initial position records.
        .addfield("init", InitialCredits)
        .addfield("fifo_cost", PositionCost)
        .addfield("init_legs", lambda r: len(r.init_txns))
        .addfield("adjust", _NumAdjustments)
        .addfield("days", lambda r: (r.maxdate - r.mindate).days + 1)
        # Chain attributes.
        .addfield("chain", partial(_GetChain, chain_map))
        .addfield("status", partial(_GetChainAttribute, "status"))
        .convert(
            "status", lambda e: ChainStatus.Name(e) if e is not None else "NoStatus"
        )
        .addfield("group", partial(_GetChainAttribute, "group"))
        .addfield("strategy", partial(_GetChainAttribute, "strategy"))
        .addfield("investment", partial(_GetChainAttribute, "investment"))
        .convert("investment", lambda v: "INVEST" if True else "TRADING")
        .addfield(
            "term", lambda r: "LT" if _GetChainAttribute("long_term", r) else "ST"
        )
        # Probability & targets.
        .addfield("prob", _CalculateProbabilities)
        .addfield("pop", lambda r: r.prob.pop if r.prob else ZERO)
        .addfield("target", lambda r: r.prob.target if r.prob else ZERO)
        .addfield("pnl_win", lambda r: r.prob.pnl_win if r.prob else ZERO)
        .addfield("pnl_loss", lambda r: r.prob.pnl_loss if r.prob else ZERO)
        .addfield("pnl_frac", _CalculatePnlFrac)
        # Calculate net liq win/loss equivalent to match on the platform.
        .addfield("net_win", lambda r: r.net_liq + (r.pnl_win - r.pnl_chain))
        .addfield("net_loss", lambda r: r.net_liq + (r.pnl_loss - r.pnl_chain))
        .cutout("prob")
        # Numbers Vs. Volatility
        .addfield("_realized", partial(GetReturns, "vol_realized"))
        .addfields(
            [
                ("vol_real", lambda r: r._realized.volatility),
                ("return_real", lambda r: r._realized.returns),
                ("move_real", lambda r: r._realized.move),
                ("stdev_real", lambda r: r._realized.stdev),
            ]
        )
        .cutout("_realized")
        .addfield("_implied", partial(GetReturns, "vol_implied"))
        .addfields(
            [
                ("vol_impl", lambda r: r._implied.volatility),
                ("return_impl", lambda r: r._implied.returns),
                ("move_impl", lambda r: r._implied.move),
                ("stdev_impl", lambda r: r._implied.stdev),
            ]
        )
        .cutout("_implied")
        .cutout("chain")
        # Mark missing groups with a string that can be filtered on.
        # TODO(blais): Move this to the presentation layer.
        .convert("group", lambda v: v or "NoGroup")
        .sort("maxdate")
    )

    # Add a row marking transactions with the initial flag.
    init_txns = set(
        rec.transaction_id
        for chain_row in chains_table.records()
        for rec in chain_row.init_txns
    )
    itransactions = transactions.addfield(
        "init", lambda r: r.transaction_id in init_txns
    )

    # Strip unnecessary columns.
    chains_table = chains_table.cut(
        "chain_id",
        "account",
        "underlyings",
        "status",
        "mindate",
        "maxdate",
        "days",
        "init",
        "init_legs",
        "adjust",
        "pnl_win",
        "pnl_chain",
        "pnl_loss",
        "pnl_frac",
        "target",
        "pop",
        "pnl_cash",
        "net_win",
        "net_liq",
        "net_loss",
        "fifo_cost",
        #
        "vol_real",
        "return_real",
        "move_real",
        "stdev_real",
        "vol_impl",
        "return_impl",
        "move_impl",
        "stdev_impl",
        #
        "commissions",
        "fees",
        "group",
        "strategy",
        "investment",
        "term",
    )

    return chains_table, itransactions


def GetChainsAndTransactions(chains: Table, transactions: Table) -> Tuple[Table, Table]:
    """A routine that produces chains and their associated lists of transactions."""
    sorted_chains = chains.sort(["underlyings", "chain_id", "mindate"])
    txns_chain_map = transactions.recordlookup("chain_id")
    bychain_map = collections.defaultdict(list)
    for chain in sorted_chains.records():
        tchain = WrapRecords([chain])
        txns = WrapRecords(txns_chain_map[chain.chain_id])
        yield tchain, txns


def ScrubConfig(transactions: Table, chains_db: Chains) -> Chains:
    """Update and clean configuration from the processed transactions table."""

    # Create a new result configuration object.
    new_chains_db = Chains()
    new_chains_db.CopyFrom(chains_db)

    # If a chain is in FINAL state, automatically promote all of its `auto_ids`
    # to `ids`.
    transaction_ids = set(transactions.values("transaction_id"))
    for chain in new_chains_db.chains:
        if chain.status == ChainStatus.FINAL and chain.auto_ids:
            chain.ids.extend(chain.auto_ids)

        # Clear the `auto_ids` field on all the chains.
        chain.ClearField("auto_ids")

        # If any of the referenced ids aren't valid transactions, issue a
        # warning. (Idea: We could eventually move these ids to a junk chain in
        # the output instead.)
        for transaction_id in chain.ids:
            if transaction_id not in transaction_ids:
                logging.error(
                    f"Invalid transaction id from chain file: '{transaction_id}'"
                )

    return new_chains_db


def UpdateConfig(transactions: Table, chains_db: Chains) -> Chains:
    """Insert new transaction ids from updated transactions and update the status of
    all the non-finalized chains. This assumes a transactions Table with freshly
    clustered chain ids.
    """

    # Create a new result configuration object. Note that we copy them in the
    # same order as in the input filein order to be able to diff the output with
    # the # original file while minimizing the text differences.
    new_chains_db = Chains()
    new_chains_db.CopyFrom(chains_db)

    # Gather the set of already existing ids.
    inserted_ids = {
        transaction_id for chain in new_chains_db.chains for transaction_id in chain.ids
    }

    # Initialize a few mappings. This is much faster than running the more
    # convenient petl materialization routines, minimizing the number of runs
    # over the transactions table.
    transactions_map = {}
    referenced_chain_ids = set()
    active_chain_ids = set()

    # Add new transactions to all the chains as `auto_ids`, where they weren't
    # already included. Create new Chain objects as necessary. Exclude marks for
    # active positions.
    chain_map = {c.chain_id: c for c in new_chains_db.chains}
    for txn in transactions.records():
        # Update various maps.
        transactions_map[txn.transaction_id] = txn
        referenced_chain_ids.add(txn.chain_id)
        if txn.rowtype == "Mark":
            active_chain_ids.add(txn.chain_id)

        if txn.rowtype == "Mark":
            continue

        transaction_id = txn.transaction_id
        if transaction_id in inserted_ids:
            continue
        inserted_ids.add(transaction_id)

        chain = chain_map.get(txn.chain_id, None)
        if chain is None:
            chain = new_chains_db.chains.add()
            chain.chain_id = txn.chain_id
            chain_map[chain.chain_id] = chain
        chain.auto_ids.append(transaction_id)

    InferStatus(referenced_chain_ids, active_chain_ids, new_chains_db)

    # TODO(blais): Save markers for the initial transactions in the produced table.
    # Then use this to share all earnings setups with @zero.
    InferStrategy(transactions_map, new_chains_db)

    return new_chains_db


def InferStatus(
    referenced_chain_ids: Set[str], active_chain_ids: Set[str], chains_db: Chains
):
    """Update (mutate) `status` on chains, from transactions."""

    # Infer the status of non-finalized chains.
    for chain in chains_db.chains:
        # Preserve finalized and ignored chains, don't override the status on
        # those. We only recalculate and update the status on ACTIVE and CLOSED
        # chains.
        if chain.status == ChainStatus.FINAL:
            if chain.chain_id in active_chain_ids:
                logging.error("ACTIVE chain {chain.chain_id} marked FINAL.")
            continue

        # If a previously present chain is now absent from the list of chains
        # referenced by the transactions table, mark it as ignored. Don't remove
        # the chain from the output in order to preserve its data and allow the
        # user to diagnose problems.
        if chain.chain_id not in referenced_chain_ids:
            chain.status = ChainStatus.IGNORE
            continue

        # We update the active status of the chain.
        # Note: Mutate in-place.
        chain.status = (
            ChainStatus.ACTIVE
            if chain.chain_id in active_chain_ids
            else ChainStatus.CLOSED
        )


def InferStrategy(transactions_map: Dict[str, Record], chains_db: Chains):
    """Update (mutate) `strategy` on chains, inferring where missing."""

    for chain in chains_db.chains:
        # Don't override already present values for `strategy`.
        if chain.strategy:
            continue

        # Fetch the list of initial transactions.
        init_tuples = []
        for transaction_id in itertools.chain(chain.ids, chain.auto_ids):
            rec = transactions_map.get(transaction_id, None)
            if rec is not None:
                init_tuples.append(
                    (rec.transaction_id, rec.order_id, rec.datetime, rec.effect)
                )
        init_transaction_ids = InitialTransactions(init_tuples)
        init_transactions = [
            transactions_map[transaction_id] for transaction_id in init_transaction_ids
        ]

        strategy, signature = strategylib.InferStrategy(init_transactions)
        if strategy:
            # Note: Mutate in-place. (I know.)
            chain.strategy = strategy
        else:
            logging.warning(
                f"Could not infer strategy for chain "
                f"http://localhost:5000/chain/{chain.chain_id} : {signature}"
            )


def AcceptChain(
    chain: Chain, group: Optional[str] = None, status: Optional[int] = None
):
    """Mutate the chain to bake the ids and modify some of its attributes."""

    # Move `auto_ids` to `ids`.
    for iid in chain.auto_ids:
        chain.ids.append(iid)
    chain.ClearField("auto_ids")

    # Set status.
    if status is not None:
        chain.status = status

    # Set group.
    if group:
        chain.group = group



def ToParquet(chains: Table, filename: str):
    """Write a transactions table to Parquet.

    This is used because we have to convert all the data types.
    """
    # We don't have a proper schema for chains. TODO: Define one nicely.
    # For now, use automated conversion from Pandas.
    df = chains.todataframe()
    df.to_parquet(filename, index=False)



# TODO: First import all historical without dividends and commit.
# TODO: UnFINAL all chains for the dividends to CLOSED (script?)
# TODO: Detect dividends that couldn't be matched to any trade and fail them
# TODO: Match up all the existing dividends to those trades.
# TODO: Implement for TW as well.
# TODO: Work on the rest of nontrades.
