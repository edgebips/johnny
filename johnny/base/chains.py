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

from functools import partial
from decimal import Decimal
from typing import Any, Iterator, List, Mapping, Tuple
import functools
import hashlib
import copy
import sys
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
          explicit_chain_map=None) -> Table:
    """Cluster transactions to create options chains.

    This function inserts the `chain_id` column to the table and returns a
    modified transactions table.

    Args:
      transactions: A normalized transactions log with a 'match' column.
      by_match: A flag, indicating that matching transactions should be chained.
      by_order: A flag, indicating that transactions from the same order should be chained.
      by_time: A flag, indicating that transactions overlapping over time should be chained.
      explicit_chain_map: A mapping of (transaction-id, unique-chain-id).
    Returns:
      A modified table with an extra "chain" column, identifying groups of
      related transactions, by episode, or chain.

    """
    explicit_chain_map = explicit_chain_map or {}

    # Create the graph.
    graph = CreateGraph(transactions, by_match, by_order, by_time,
                        explicit_chain_map)

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
        chain_id = ChainName(chain_txns, explicit_chain_map)
        for rec in chain_txns:
            chain_map[rec.transaction_id] = chain_id

    return (transactions
            .addfield('chain_id', lambda r: chain_map[r.transaction_id]))


def CreateGraph(transactions: Table,
                by_match=True,
                by_order=True,
                by_time=True,
                explicit_chain_map=None) -> nx.Graph:
    """Create a graph to link together related transactions."""

    AssertColumns(transactions,
                  ('transaction_id', str),
                  ('order_id', {None, str}),
                  ('match_id', str),
                  ('datetime', datetime.datetime),
                  ('expiration', {None, datetime.date}),
                  ('account', str),
                  ('underlying', str))
    explicit_chain_map = copy.copy(explicit_chain_map) or {}

    # Extract out transactions that are explicitly chained.
    explicit_transactions, implicit_transactions = transactions.biselect(
        lambda rec: rec.transaction_id in explicit_chain_map)

    def is_explicit(rec):
        return rec.transaction_id in explicit_chain_map
    explicit_transactions, implicit_transactions = transactions.biselect(is_explicit)
    remain_chain_map = copy.copy(explicit_chain_map)

    # Process explicitly specified chains.
    graph = nx.Graph()
    for rec in explicit_transactions.records():
        graph.add_node(rec.transaction_id, type='txn', rec=rec)

        # Extract explicit chains to their own components and don't link them
        # with others.
        explicit_chain = remain_chain_map.pop(rec.transaction_id, None)
        if explicit_chain:
            graph.add_node(explicit_chain, type='expchain')
            graph.add_edge(rec.transaction_id, explicit_chain)

    # Process implicitly defined chains.
    for rec in implicit_transactions.records():
        graph.add_node(rec.transaction_id, type='txn', rec=rec)

        # Link together by order id.
        if by_order:
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

    for item in remain_chain_map.items():
        logging.warning(f"Remaining transaction id: {item}")

    return graph


def _LinkByOverlapping(transactions: Table) -> List[Tuple[str, str]]:
    """Return pairs of linked matches, linking all transactions where either of (a)
    an outright position exists in that underlying and/or (b) a common
    expiration exists in that underlying. We're not bothering to inspect
    match_id at all. This is a bit more general and correct than
    _LinkByOverlappingMatch().
    """
    AssertColumns(transactions,
                  ('transaction_id', str),
                  ('instruction', str),
                  ('symbol', str),
                  ('quantity', Decimal),
                  ('account', str),
                  ('underlying', str),
                  ('expiration', {None, datetime.date}))

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
    # the `quantities` attribue.
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
        isnew = rec.expiration not in undermap
        term_id = "{}/{}/{}/{}".format(rec.account, rec.underlying,
                                       rec.expiration, next(idgen))
        term = undermap.get(rec.expiration, None)
        if term is None:
            term = undermap[rec.expiration] = Term(term_id)

        if isnew:
            if rec.expiration is None:
                # This is an underlying, not an option on one.
                # Link it to all the currently active expirations.
                for expiration, expterm in undermap.items():
                    if expiration is None:
                        continue
                    links.append((term.id, expterm.id))
            else:
                # This is an option.
                # Link it to the underlying if it is active.
                if None in undermap:
                    outterm = undermap[None]
                    links.append((term.id, outterm.id))

        sign = -1 if rec.instruction == 'SELL' else +1
        term.quantities[rec.symbol] += sign * rec.quantity
        transaction_links.append((term.id, rec.transaction_id))
        if term.quantities[rec.symbol] == ZERO:
            del term.quantities[rec.symbol]
        if not term.quantities:
            del undermap[rec.expiration]

    # Sanity check. All positions should have been closed (`Mark` rows close
    # outstanding positions) and the resulting inventory should be completely
    # empty.
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
              explicit_chain_map: Mapping[str, str]):
    """Generate a unique chain name. This assumes 'account', 'mindate' and
    'underlying' columns."""

    # Look for an explicit chain id.
    for txn in txns:
        explicit_chain_id = explicit_chain_map.get(txn.transaction_id, None)
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
    return ",".join(sorted(set(instrument.FromString(symbol).underlying
                               for symbol in symbols)))


def _GetChainAttribute(chain_map, attrname, chain_id) -> Any:
    """Get a chain attribute."""
    chain = chain_map.get(next(chain_id), None)
    if chain is None:
        return ''
    return getattr(chain, attrname)


def TransactionsTableToChainsTable(transactions: Table, config: configlib.Config) -> Table:
    """Aggregate a table of already identified transactions row (with a `chain_id` column)
    to a table of aggregated chains."""

    # Mark the transactions.
    price_map = mark.GetPriceMap(transactions, config)
    transactions = mark.Mark(transactions, price_map)

    chain_map = {chain.chain_id: chain for chain in config.chains}
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
        'trade_type': ('chain_id', partial(_GetChainAttribute, chain_map, 'trade_type')),
        'strategy': ('chain_id', partial(_GetChainAttribute, chain_map, 'strategy')),
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
             'trade_type', 'strategy', 'instruments'))

    return chains


def CleanConfig(config: configlib.Config,
                chains_table: Table,
                transactions: Table) -> configlib.Config:

    """Create configuration objects and a clean config from the processed chains table."""
    new_config = configlib.Config()
    new_config.CopyFrom(config)
    new_config.ClearField('chains')

    # Copy the original chains in the same order as in the input file. We
    # produce the output in this order in order to be able to compare (diff) the
    # output with the original file while minimizing the differences.
    rec_map = {rec.chain_id: rec for rec in chains_table.records()}
    inserted = set()
    for old_chain in config.chains:
        new_chain = new_config.chains.add()
        rec = rec_map.get(old_chain.chain_id, None)
        if rec is None:
            new_chain.status = configlib.ChainStatus.IGNORE
        new_chain.CopyFrom(old_chain)
        new_chain.ClearField('auto_ids')
        if rec is not None:
            new_chain.trade_type = rec.trade_type
            if rec.strategy:
                new_chain.strategy = rec.strategy
        inserted.add(old_chain.chain_id)

    # Copy all the other chains from the table into the list of chains.
    for rec in chains_table.records():
        if rec.chain_id in inserted:
            continue
        new_chain = new_config.chains.add()
        new_chain.chain_id = rec.chain_id
        new_chain.trade_type = rec.trade_type
        if rec.strategy:
            new_chain.strategy = rec.strategy

    # Add the order ids to all the chains, where they weren't already included.
    chain_map = {c.chain_id: c for c in new_config.chains}
    for rec in transactions.records():
        if rec.rowtype == 'Mark':
            continue
        chain = chain_map.get(rec.chain_id, None)
        if rec.transaction_id in chain.ids:
            continue
        if rec.transaction_id in chain.auto_ids:
            continue
        chain.ids.append(rec.transaction_id)

    return new_config
