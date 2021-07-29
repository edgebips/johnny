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
from typing import Any, Iterator, List, Mapping, Optional, Tuple
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
Q = Decimal('0.01')


def Group(transactions: Table,
          chains: List[configlib.Chain],
          by_match=True,
          by_order=True,
          by_time=True) -> Table:
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
      explicit_chain_map: A mapping of (transaction-id, unique-chain-id).
    Returns:
      A modified table with an extra "chain" column, identifying groups of
      related transactions, by episode, or chain.

    """
    # Extract finalized chains explicitly. They don't have to be part of the graph.
    final_chains, match_chains = [], []
    for chain in chains:
        (final_chains
         if chain.status == configlib.ChainStatus.FINAL
         else match_chains).append(chain)

    # Start with final chains.
    chain_map = {iid: chain.chain_id
                 for chain in final_chains
                 for iid in chain.ids}

    # Select remaining transactions.
    match_transactions = (transactions
                          .selectnotin('transaction_id', chain_map))
    match_chain_map = {iid: chain.chain_id
                       for chain in final_chains
                       for iid in chain.ids}

    # Create a graph and process each connected component to an individual trade.
    graph = CreateGraph(match_transactions, match_chains, by_match, by_order, by_time)
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

        chain_id = ChainName(chain_txns, match_chain_map)
        for rec in chain_txns:
            chain_map[rec.transaction_id] = chain_id

    return (transactions
            .addfield('chain_id', lambda r: chain_map[r.transaction_id]))


def CreateGraph(transactions: Table,
                chains: List[configlib.Chain],
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

    # Create a mapping of transaction id to their chain id.
    chain_map = {iid: chain.chain_id
                 for chain in chains
                 for iid in chain.ids}

    graph = nx.Graph()
    for rec in transactions.records():
        graph.add_node(rec.transaction_id, type='txn', rec=rec)

        # Link together explicit chains that aren't finalized.
        explicit_chain = chain_map.pop(rec.transaction_id, None)
        if explicit_chain:
            graph.add_node(explicit_chain, type='expchain')
            graph.add_edge(rec.transaction_id, explicit_chain)

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
        links, transaction_links = _LinkByOverlapping(transactions)

        ids = set(id1 for id1, _ in links)
        ids.update(id2 for _, id2 in links)
        ids.update(id1 for id1, _ in transaction_links)
        for idx in ids:
            graph.add_node(idx, type='time')
        for id1, id2 in itertools.chain(links, transaction_links):
            graph.add_edge(id1, id2)

    for item in chain_map.items():
        logging.warning(f"Explicit transaction id from chains not seen in log: {item}")

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


def ChainName(txns: List[Record],
              chain_map: Mapping[str, str]):
    """Generate a unique chain name."""

    # Look for an explicit chain id.
    sorted_txns = sorted(txns, key=lambda r: (r.datetime, r.underlying))
    for txn in sorted_txns:
        explicit_chain_id = chain_map.get(txn.transaction_id, None)
        if explicit_chain_id is not None:
            return explicit_chain_id

    # Note: We don't know the max date, so we stick with the front date only in
    # the readable chain name.
    first_txn = next(iter(sorted_txns))
    return ".".join([first_txn.account,
                     "{:%y%m%d_%H%M%S}".format(first_txn.datetime),
                     first_txn.underlying.lstrip('/')])


# Initial threshold within which we consider the orders a part of the initial
# order. This allows us some time flexibility to leg in orders.
INITIAL_ORDER_THRESHOLD = datetime.timedelta(seconds=300)


def InitialOrder(pairs: Iterator[Tuple[str, str, datetime.datetime, str]]) -> Decimal:
    """Extract the transaction ids for the initial order."""
    init_order_id = None
    init_txns = []
    init_dt = None
    for order_id, transaction_id, dt, effect in sorted(pairs, key=lambda r: (r[2], r[0])):
        if effect == 'CLOSING':
            continue
        if (init_order_id is not None and
            order_id != init_order_id and
            (dt - init_dt) > INITIAL_ORDER_THRESHOLD):
            break
        init_order_id = order_id
        init_dt = dt
        init_txns.append(transaction_id)
    return init_txns


def InitialCredits(rec: Record) -> Decimal:
    """Compute the initial credits from a group of chain rows."""
    return sum(rec.cost for rec in rec.init_txns)


def _GetStatus(rowtypes):
    return 'ACTIVE' if any(rowtype == 'Mark' for rowtype in rowtypes) else 'CLOSED'


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

    chain_map = {chain.chain_id: chain for chain in config.chains}
    agg = {
        'account': ('account', first),
        'mindate': ('datetime', lambda g: min(g).date()),
        'maxdate': ('datetime', lambda g: max(g).date()),
        'underlying': ('underlying', first),
        'pnl_chain': ('cost', lambda vlist: sum(vlist).quantize(Q)),
        'init_txns': (('order_id', 'transaction_id', 'datetime', 'effect'), InitialOrder),
        'net_liq': (('rowtype', 'cost'), _CalculateNetLiq),
        'commissions': ('commissions', sum),
        'fees': ('fees', sum),
        'status': ('rowtype', _GetStatus),
        'group': ('chain_id', partial(_GetChainAttribute, chain_map, 'group')),
        'strategy': ('chain_id', partial(_GetChainAttribute, chain_map, 'strategy')),
        'instruments': ('symbol', _GetInstruments),
    }

    txn_map = transactions.recordlookupone('transaction_id')
    chains = (
        transactions

        # Add the underlying.
        .addfield('underlying', lambda r: instrument.ParseUnderlying(r.symbol))

        # TODO(blais): Can we remove this? Assume it from the input.
        .replace('commissions', None, ZERO)
        .replace('fees', None, ZERO)

        # Aggregate over the chain id.
        .aggregate('chain_id', agg)

        # Add calculations off the initial position records.
        .convert('init_txns', lambda txns: list(map(txn_map.__getitem__, txns)))
        .addfield('init', InitialCredits)
        .addfield('init_legs', lambda r: len(r.init_txns))
        # TODO(blais): Infer the strategy here.

        .addfield('days', lambda r: (r.maxdate - r.mindate).days + 1)
        .sort('maxdate')
        .cut('chain_id', 'account', 'underlying', 'status',
             'mindate', 'maxdate', 'days',
             'init', 'init_legs', 'pnl_chain', 'net_liq', 'commissions', 'fees',
             'group', 'strategy', 'instruments'))

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
            new_chain.group = rec.group
            if rec.strategy:
                new_chain.strategy = rec.strategy
        inserted.add(old_chain.chain_id)

    # Copy all the other chains from the table into the list of chains.
    for rec in chains_table.records():
        if rec.chain_id in inserted:
            continue
        new_chain = new_config.chains.add()
        new_chain.chain_id = rec.chain_id
        new_chain.group = rec.group
        if rec.strategy:
            new_chain.strategy = rec.strategy

    # Add the order ids to all the chains, where they weren't already included.
    chain_map = {c.chain_id: c for c in new_config.chains}
    for rec in transactions.records():
        if rec.rowtype == 'Mark':
            continue
        chain = chain_map.get(rec.chain_id, None)
        if rec.transaction_id in chain.ids or rec.transaction_id in chain.auto_ids:
            continue
        chain.auto_ids.append(rec.transaction_id)

    # Override the status of some chain to computed ones.
    for chain in new_config.chains:
        # Preserve finalized and ignored chains, don't override the status on those.
        # We only recalculate status on ACTIVE and CLOSED chains.
        if chain.status in {configlib.ChainStatus.FINAL, configlib.ChainStatus.IGNORE}:
            continue
        chain_row = rec_map.get(chain.chain_id, None)
        if chain_row is None:
            continue
        if chain_row.status == 'CLOSED':
            chain.status = configlib.ChainStatus.CLOSED

    # TODO(blais): Use the enum in th status of the chains in the table.
    # The row should be the same as that from the enum.

    return new_config


def FinalizeChain(chain: configlib.Chain, group: Optional[str]):
    """Mutate the given chain to finalize it."""
    chain.status = configlib.ChainStatus.FINAL
    if group:
        chain.group = group
    for iid in chain.auto_ids:
        chain.ids.append(iid)
    chain.ClearField('auto_ids')
