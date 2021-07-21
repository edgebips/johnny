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


# TODO(blais): Also implement the boolean flags for matching.
# TODO(blais): Write unit tests.

# TODO(blais): Make sure not just date is taken into account, but also time,
# when span matching. An intra-day reset is valuable.
"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"


from decimal import Decimal
from typing import Iterator, List, Mapping, Tuple
import hashlib
import collections
import datetime
import itertools
import logging

from more_itertools import first
import networkx as nx

from johnny.base import config as configlib
from johnny.base import instrument
from johnny.base import mark
from johnny.base.etl import AssertColumns, Record, Table


ZERO = Decimal(0)


def Group(transactions: Table,
          by_match=True,
          by_order=True,
          by_time=True,
          explicit_transactions_chain_map=None,
          explicit_orders_chain_map=None) -> Table:
    """Aggregate transaction rows by options chain.

    Args:
      transactions: A normalized transactions log with a 'match' column.
      by_match: A flag, indicating that matching transactions should be chained.
      by_order: A flag, indicating that transactions from the same order should be chained.
      by_time: A flag, indicating that transactions overlapping over time should be chained.
      explicit_transactions_chain_map: A mapping of (transaction-id, unique-chain-id).
      explicit_orders_chain_map: A mapping of (order-id, unique-chain-id).
    Returns:
      A modified table with an extra "chain" column, identifying groups of
      related transactions, by episode, or chain.
    """
    explicit_transactions_chain_map = explicit_transactions_chain_map or {}
    explicit_orders_chain_map = explicit_orders_chain_map or {}

    # Create the graph.
    graph = CreateGraph(transactions, by_match, by_order, by_time,
                        explicit_transactions_chain_map, explicit_orders_chain_map)

    # Process each connected component to an individual trade.
    # Note: This includes rolls if they were carried one as a single order.
    chain_map = {}
    for cc in nx.connected_components(graph):
        chain_txns = []
        for transaction_id in cc:
            node = graph.nodes[transaction_id]
            try:
                unused_node_type = node['type']
            except KeyError:
                raise KeyError("Node with no type for transaction: {}".format(
                    transaction_id))
            if node['type'] == 'txn':
                chain_txns.append(node['rec'])

        assert chain_txns, "Invalid empty chain: {}".format(chain_txns)
        chain_id = ChainName(chain_txns,
                             explicit_transactions_chain_map,
                             explicit_orders_chain_map)
        for rec in chain_txns:
            chain_map[rec.transaction_id] = chain_id

    return (transactions
            .addfield('chain_id', lambda r: chain_map[r.transaction_id]))


def CreateGraph(transactions: Table,
                by_match=True,
                by_order=True,
                by_time=True,
                explicit_transactions_chain_map=None,
                explicit_orders_chain_map=None) -> nx.Graph:
    """Create a graph to link together related transactions."""

    AssertColumns(transactions,
                  ('transaction_id', str),
                  ('order_id', {None, str}),
                  ('match_id', str),
                  ('datetime', datetime.datetime),
                  ('expiration', {None, datetime.date}),
                  ('account', str),
                  ('underlying', str))
    explicit_transactions_chain_map = explicit_transactions_chain_map or {}
    explicit_orders_chain_map = explicit_orders_chain_map or {}

    # Extract out transactions that are explicitly chained.
    explicit_transactions, implicit_transactions = transactions.biselect(
        lambda rec: (rec.transaction_id in explicit_transactions_chain_map or
                     rec.order_id in explicit_orders_chain_map))

    # Process explicitly specified chains.
    graph = nx.Graph()
    for rec in explicit_transactions.records():
        graph.add_node(rec.transaction_id, type='txn', rec=rec)

        # Extract explicit chains to their own components and don't link them
        # with others.
        explicit_chain = (explicit_transactions_chain_map.get(rec.transaction_id)  or
                          explicit_orders_chain_map.get(rec.order_id))
        if explicit_chain:
            graph.add_node(explicit_chain, type='expchain')
            graph.add_edge(rec.transaction_id, explicit_chain)

    # Process implicitly defined chains.
    for rec in implicit_transactions.records():
        graph.add_node(rec.transaction_id, type='txn', rec=rec)

        # Link together by order id.
        if by_order:
            if rec.order_id:
                graph.add_node(rec.order_id, type='order')
                graph.add_edge(rec.transaction_id, rec.order_id)

        # Link together by match id.
        if by_match:
            if rec.match_id:
                graph.add_node(rec.match_id, type='match')
                graph.add_edge(rec.transaction_id, rec.match_id)

    # Link together matches that overlap in underlying and time.
    if by_time:
        links, transaction_links = _LinkByOverlapping(implicit_transactions)

        ids = set(id1 for id1, _ in links)
        ids.update(id2 for _, id2 in links)
        ids.update(id1 for id1, _ in transaction_links)
        for idx in ids:
            graph.add_node(idx, type='time')
        for id1, id2 in itertools.chain(links, transaction_links):
            graph.add_edge(id1, id2)

    return graph


def _LinkByOverlappingMatch(transactions: Table,
                            unused_explicit_chains=None) -> List[Tuple[str, str]]:
    """Return pairs of linked matches, matched strictly by common expiration."""

    AssertColumns(transactions,
                  ('match_id', str),
                  ('datetime', datetime.datetime),
                  ('expiration', {None, datetime.date}),
                  ('account', str),
                  ('underlying', str))

    # Gather min and max time for each trade match into a changelist.
    spans = []
    def GatherMatchSpans(grouper):
        rows = list(grouper)
        min_datetime = datetime.datetime(2100, 1, 1)
        max_datetime = datetime.datetime(1970, 1, 1)
        for rec in sorted(rows, key=lambda r: r.datetime):
            min_datetime = min(min_datetime, rec.datetime)
            max_datetime = max(max_datetime, rec.datetime)
        expiration = rec.expiration if rec.expiration else datetime.date(1970, 1, 1)
        assert rec.underlying is not None
        assert rec.match_id is not None
        spans.append((min_datetime, rec.account, rec.underlying, expiration, rec.match_id))
        spans.append((max_datetime, rec.account, rec.underlying, expiration, rec.match_id))
        return 0
    # Note: we do not match if the expiration is different; if there is an order
    # id rolling the position to the next month (from the previous function),
    # this is sufficient to connect them.
    list(transactions.aggregate(('underlying', 'expiration', 'match_id'), GatherMatchSpans)
         .records())

    # Process the spans in the order of time and allocate a new span id whenever
    # there's a gap without a position/match within one underlying.
    under_map = {(account, underlying, expiration): set()
                 for _, account, underlying, expiration, __ in spans}
    linked_matches = []
    for _, account, underlying, expiration, match_id in sorted(spans):
        # Update the set of active matches, removing or adding.
        active_set = under_map[(account, underlying, expiration)]
        if match_id in active_set:
            active_set.remove(match_id)
        else:
            if active_set:
                # Link the current match-id to any other match id.
                other_match_id = next(iter(active_set))
                linked_matches.append((match_id, other_match_id))
            active_set.add(match_id)
    assert all(not active for active in under_map.values())

    return set(linked_matches), set()


def _LinkByOverlapping(transactions: Table) -> List[Tuple[str, str]]:
    """Return pairs of linked matches, linking all transactions where either of (a)
    an outright position exists in that underlying and/or (b) a common
    expiration exists in that underlying. We're not bothering to inspect
    match_id at all. This is a bit more general and correct than
    _LinkByOverlappingMatch().
    """

    AssertColumns(transactions,
                  ('instruction', str),
                  ('quantity', Decimal),
                  ('account', str),
                  ('underlying', str),
                  ('expiration', {None, datetime.date}),
                  ('expcode', {None, str}),
                  ('putcall', {None, str}),
                  ('strike', {None, Decimal}))

    # Run through matching in order to figure out overlaps.
    idgen = iter("overlap{}".format(x) for x in itertools.count(start=1))
    class Pos:
        def __init__(self):
            self.id = next(idgen)
            self.quanmap = collections.defaultdict(Decimal)
        def __repr__(self):
            return "<Pos {} {}>".format(self.id, self.quanmap)

    inventory = collections.defaultdict(lambda: collections.defaultdict(Pos))
    links = []
    transaction_links = []
    for rec in transactions.records():
        # Get a mapping for each underlying. In that submapping, the special key
        # 'None' refers to the position of the underlying outright.
        undermap = inventory[(rec.account, rec.underlying)]

        # Potentially allocate a new position for the expiration (or lack thereof).
        isnew = rec.expiration not in undermap
        pos = undermap[rec.expiration]

        if rec.expiration is None:
            if isnew:
                # Link it to all the active expirations for that underlying.
                for expiration, exppos in undermap.items():
                    if expiration is None:
                        continue
                    links.append((pos.id, exppos.id))
        else:
            if isnew:
                # Link it to the underlying if it is active.
                if None in undermap:
                    outpos = undermap[None]
                    links.append((pos.id, outpos.id))

        sign = -1 if rec.instruction == 'SELL' else +1
        pos.quanmap[rec.symbol] += sign * rec.quantity
        transaction_links.append((pos.id, rec.transaction_id))
        if pos.quanmap[rec.symbol] == ZERO:
            del pos.quanmap[rec.symbol]
        if not pos.quanmap:
            del undermap[rec.expiration]

    # Clean up zeros.
    inventory = {key: undermap
                 for key, undermap in undermap.items()
                 if undermap}
    if inventory:
        logging.error("Inventory not empty: {}".format(inventory))

    return links, transaction_links


def _CreateChainId(transaction_id: str, _: datetime.datetime) -> str:
    """Create a unique match id from the given transaction id."""
    md5 = hashlib.blake2s(digest_size=4)
    md5.update(transaction_id.encode('ascii'))
    return "{}".format(md5.hexdigest())


# TODO(blais): Assign chain names after grouping so that this isn't necessary.
def ChainName(txns: List[Record],
              explicit_transactions_chain_map: Mapping[str, str],
              explicit_orders_chain_map: Mapping[str, str]):
    """Generate a unique chain name. This assumes 'account', 'mindate' and
    'underlying' columns."""

    # Look for an explicit chain id.
    for txn in txns:
        explicit_chain_id = explicit_transactions_chain_map.get(txn.transaction_id, None)
        if explicit_chain_id is not None:
            return explicit_chain_id
    for txn in txns:
        explicit_chain_id = explicit_orders_chain_map.get(txn.order_id, None)
        if explicit_chain_id is not None:
            return explicit_chain_id

    # Note: We don't know the max date, so we stick with the front date only in
    # the readable chain name.
    first_txn = next(iter(sorted(txns, key=lambda r: (r.datetime, r.underlying))))
    return ".".join([first_txn.account,
                     "{:%y%m%d_%H%M%S}".format(first_txn.datetime),
                     first_txn.underlying.lstrip('/')])


def InitialCredits(pairs: Iterator[Tuple[str, Decimal]]) -> Decimal:
    """Compute the initial credits from a group of chain rows."""
    first_order_id = None
    first_order_sum = ZERO
    for order_id, cost in pairs:
        if first_order_id is None or order_id is None or order_id < first_order_id:
            first_order_id = order_id
            first_order_sum = cost
        elif order_id == first_order_id:
            first_order_sum += cost
    return first_order_sum


def _GetStatus(rowtypes):
    return 'ACTIVE' if any(rowtype == 'Mark' for rowtype in rowtypes) else 'DONE'


def _CalculateNetLiq(pairs: Iterator[Tuple[str, Decimal]]):
    return Decimal(sum(cost
                       for rowtype, cost in pairs
                       if rowtype == 'Mark'))


def _GetInstruments(symbols: Iterator[str]) -> List[str]:
    return set(instrument.FromString(symbol).underlying
               for symbol in symbols)


def TransactionsToChains(transactions: Table, config: configlib.Config) -> Table:
    """Aggregate already chained transactions to aggregated chains."""

    # Mark the transactions.
    price_map = mark.GetPriceMap(transactions, config)
    transactions = mark.Mark(transactions, price_map)

    type_map = {chain.chain_id: chain.trade_type for chain in config.chains}
    agg = {
        'account': ('account', first),
        'mindate': ('datetime', lambda g: min(g).date()),
        'maxdate': ('datetime', lambda g: max(g).date()),
        'underlying': ('underlying', first),
        'pnl_chain': ('cost', sum),
        'init': (('order_id', 'cost'), InitialCredits),
        'net_liq': (('rowtype', 'cost'), _CalculateNetLiq),
        'commissions': ('commissions', sum),
        'fees': ('fees', sum),
        'status': ('rowtype', _GetStatus),
        'trade_type': ('chain_id', lambda cids: type_map.get(next(cids), '')),
        'instruments': ('symbol', _GetInstruments),
    }
    chains = (
        transactions
        .addfield('underlying', lambda r: instrument.ParseUnderlying(r.symbol))
        .replace('commissions', None, ZERO)
        .replace('fees', None, ZERO)

        .aggregate('chain_id', agg)
        .addfield('days', lambda r: (r.maxdate - r.mindate).days + 1)
        .sort('maxdate')
        .cut('chain_id', 'account', 'underlying', 'status',
             'mindate', 'maxdate', 'days',
             'init', 'pnl_chain', 'net_liq', 'commissions', 'fees',
             'trade_type', 'instruments'))

    return chains
