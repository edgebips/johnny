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
from typing import Any, Iterator, List, Mapping, Optional, Tuple, Dict, Set
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

from johnny.base import config as configlib
from johnny.base import strategy as strategylib
from johnny.base import instrument
from johnny.base import mark
from johnny.base.etl import AssertColumns, Record, Table
from johnny.utils import timing
ChainStatus = configlib.ChainStatus


ZERO = Decimal(0)
Q = Decimal('0.01')


def ChainTransactions(matched_transactions: Table,
                      config: configlib.Config) -> Tuple[Table, configlib.Config]:
    """Cluster the transactions and return a new table, with added 'chain_id' and an
    update chains configuration on a config object."""

    log = functools.partial(timing.log_time, log_timings=logging.info, indent=1)

    # Clean up the configuration before clustering with it as a side-input.
    with log('scrub'):
        clean_config = ScrubConfig(matched_transactions, config)

    # Run the chains heuristic. (Note: We need to temporarily expand the
    # instrument fields, as they are needed by the match and chains modules.)
    with log('group'):
        chained_transactions = (matched_transactions
                                .applyfn(instrument.Expand, 'symbol')
                                .applyfn(Group, clean_config.chains)
                                .applyfn(instrument.Shrink))

    with log('update'):
        updated_config = UpdateConfig(chained_transactions, clean_config)

    return chained_transactions, updated_config


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
    Returns:
      A modified table with an extra "chain" column, identifying groups of
      related transactions, by episode, or chain.
    """
    # Extract finalized chains explicitly. They don't have to be part of the graph.
    final_chains, match_chains = [], []
    for chain in chains:
        (final_chains
         if chain.status == ChainStatus.FINAL
         else match_chains).append(chain)
    final_chain_ids = {chain.chain_id for chain in final_chains}

    # Initialize the resulting mapping of transactions to chains with finalized
    # chains.
    txn_chain_map = {transaction_id: chain.chain_id
                     for chain in final_chains
                     for transaction_id in chain.ids}

    # Select only remaining transactions.
    match_transactions = (transactions
                          .selectnotin('transaction_id', txn_chain_map))

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

        chain_id = ChainName(chain_txns, txn_chain_map)

        # This should never happen; but if you somehow used the wrong
        # transaction - with the same (account, datetime, underlying) in a final
        # chain, it could. Best is to adjust the input file, but we could
        # eventually just insert a random character here.
        assert chain_id not in final_chain_ids, (
            f"Collision with FINAL chain names at '{chain_id}'.")
        for rec in chain_txns:
            txn_chain_map[rec.transaction_id] = chain_id

    return (transactions
            .addfield('chain_id', lambda r: txn_chain_map[r.transaction_id]))


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
    chain_id = ".".join([first_txn.account,
                         "{:%y%m%d_%H%M%S}".format(first_txn.datetime),
                         first_txn.underlying.lstrip('/')])
    return chain_id


# Initial threshold within which we consider the orders a part of the initial
# order. This allows us some time flexibility to leg in orders.
INITIAL_ORDER_THRESHOLD = datetime.timedelta(seconds=300)


def InitialTransactions(pairs: Iterator[Tuple[str, str, datetime.datetime, str]]) -> Decimal:
    """Extract the transaction ids for the initial order. This input are tupled of
    (transaction_id, order_id, datetime, effect) for each transaction."""
    init_order_id = None
    init_txns = []
    init_dt = None
    for transaction_id, order_id, dt, effect in sorted(pairs, key=lambda r: (r[2], r[0])):
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


def MarkTransactions(pairs: Iterator[Tuple[str, str]]) -> Decimal:
    """Extract the active positions transactions."""
    return [transaction_id
            for transaction_id, rowtype in pairs
            if rowtype == 'Mark']


def InitialCredits(rec: Record) -> Decimal:
    """Compute the initial credits from a group of chain rows."""
    return sum(rec.cost for rec in rec.init_txns)


def _CalculateNetLiq(pairs: Iterator[Tuple[str, Decimal]]):
    return Decimal(sum(cost
                       for rowtype, cost in pairs
                       if rowtype == 'Mark'))


def _GetInstruments(symbols: Iterator[str]) -> List[str]:
    return ",".join(sorted(set(instrument.FromString(symbol).underlying
                               for symbol in symbols)))


def _GetChainAttribute(chain_map, attrname, rec: Record) -> Any:
    """Get a chain attribute."""
    chain = chain_map.get(rec.chain_id, None)
    if chain is None:
        logging.warning(f"Could not get chain '{rec.chain_id}' for attribute '{attrname}'")
        return None
    return getattr(chain, attrname)


def TransactionsTableToChainsTable(transactions: Table,
                                   config: configlib.Config) -> Tuple[Table, Table]:
    """Aggregate a table of already identified transactions row (with a `chain_id` column)
    to a table of aggregated chains. The `config` object is used to join attributes in the
    table.

    A new table of transactions is returned. It will contain additional columns,
    columns that can only be computed with the chains data, such as flags that
    mark rows as initial or not.
    """

    agg = {
        'txns': ('transaction_id', list),
        'init_txns': (('transaction_id', 'order_id', 'datetime', 'effect'),
                      InitialTransactions),
        'mark_txns': (('transaction_id', 'rowtype'),
                      MarkTransactions),

        'account': ('account', first),
        'mindate': ('datetime', lambda g: min(g).date()),
        'maxdate': ('datetime', lambda g: max(g).date()),
        'underlying': ('underlying', first),
        'pnl_chain': ('cost', lambda vlist: sum(vlist).quantize(Q)),
        'net_liq': (('rowtype', 'cost'), _CalculateNetLiq),
        'commissions': ('commissions', sum),
        'fees': ('fees', sum),
        'instruments': ('symbol', _GetInstruments),
    }

    transaction_map = transactions.recordlookupone('transaction_id')
    chain_map = {c.chain_id: c for c in config.chains}

    chains_table = (
        transactions

        # Add the underlying.
        .addfield('underlying', lambda r: instrument.ParseUnderlying(r.symbol))

        # Aggregate over the chain id.
        .aggregate('chain_id', agg)
        .convert('txns', lambda txns: list(map(transaction_map.__getitem__, txns)))
        .convert('init_txns', lambda txns: list(map(transaction_map.__getitem__, txns)))
        .convert('mark_txns', lambda txns: list(map(transaction_map.__getitem__, txns)))

        # Add calculations off the initial position records.
        .addfield('init', InitialCredits)
        .addfield('init_legs', lambda r: len(r.init_txns))
        .addfield('init_frac', lambda r: (r.pnl_chain / r.init).quantize(Q) if r.init else ZERO)

        .addfield('days', lambda r: (r.maxdate - r.mindate).days + 1)

        .addfield('status', partial(_GetChainAttribute, chain_map, 'status'))
        .convert('status', lambda e: ChainStatus.Name(e) if e is not None else 'NoStatus')
        .addfield('group', partial(_GetChainAttribute, chain_map, 'group'))
        .addfield('strategy', partial(_GetChainAttribute, chain_map, 'strategy'))

        # Mark missing groups with a string that can be filtered on.
        .convert('group', lambda v: v or 'NoGroup')

        .sort('maxdate'))

    # Add a row marking transactions with the initial flag.
    init_txns = set(rec.transaction_id
                    for chain_row in chains_table.records()
                    for rec in chain_row.init_txns)
    itransactions = (transactions
                     .addfield('init', lambda r: r.transaction_id in init_txns))

    # Strip unnecessary columns.
    chains_table = (chains_table
                    .cut('chain_id', 'account', 'underlying', 'status',
                         'mindate', 'maxdate', 'days',
                         'init', 'init_legs', 'init_frac', 'pnl_chain',
                         'net_liq', 'commissions', 'fees',
                         'group', 'strategy', 'instruments'))

    return chains_table, itransactions


def ScrubConfig(transactions: Table,
                config: configlib.Config) -> configlib.Config:
    """Update and clean configuration from the processed transactions table."""

    # Create a new result configuration object.
    new_config = configlib.Config()
    new_config.CopyFrom(config)

    # If a chain is in FINAL state, automatically promote all of its `auto_ids`
    # to `ids`.
    transaction_ids = set(transactions.values('transaction_id'))
    for chain in new_config.chains:
        if chain.status == ChainStatus.FINAL and chain.auto_ids:
            chain.ids.extend(chain.auto_ids)

        # Clear the `auto_ids` field on all the chains.
        chain.ClearField('auto_ids')

        # If any of the referenced ids aren't valid transactions, issue a
        # warning. (Idea: We could eventually move these ids to a junk chain in
        # the output instead.)
        for transaction_id in chain.ids:
            if transaction_id not in transaction_ids:
                logging.error(f"Invalid transaction id from chain file: '{transaction_id}'")

    return new_config


def UpdateConfig(transactions: Table,
                 config: configlib.Config) -> configlib.Config:
    """Insert new transaction ids from updated transactions and update the status of
    all the non-finalized chains. This assumes a transactions Table with freshly
    clustered chain ids.
    """

    # Create a new result configuration object. Note that we copy them in the
    # same order as in the input filein order to be able to diff the output with
    # the # original file while minimizing the text differences.
    new_config = configlib.Config()
    new_config.CopyFrom(config)

    # Gather the set of already existing ids.
    inserted_ids = {transaction_id
                    for chain in new_config.chains
                    for transaction_id in chain.ids}

    # Initialize a few mappings. This is much faster than running the more
    # convenient petl materialization routines, minimizing the number of runs
    # over the transactions table.
    transactions_map = {}
    referenced_chain_ids = set()
    active_chain_ids = set()

    # Add new transactions to all the chains as `auto_ids`, where they weren't
    # already included. Create new Chain objects as necessary. Exclude marks for
    # active positions.
    chain_map = {c.chain_id: c for c in new_config.chains}
    for txn in transactions.records():
        # Update various maps.
        transactions_map[txn.transaction_id] = txn
        referenced_chain_ids.add(txn.chain_id)
        if txn.rowtype == 'Mark':
            active_chain_ids.add(txn.chain_id)

        if txn.rowtype == 'Mark':
            continue

        transaction_id = txn.transaction_id
        if transaction_id in inserted_ids:
            continue
        inserted_ids.add(transaction_id)

        chain = chain_map.get(txn.chain_id, None)
        if chain is None:
            chain = new_config.chains.add()
            chain.chain_id = txn.chain_id
            chain_map[chain.chain_id] = chain
        chain.auto_ids.append(transaction_id)

    InferStatus(referenced_chain_ids, active_chain_ids, new_config)

    # TODO(blais): Save markers for the initial transactions in the produced table.
    # Then use this to share all earnings setups with @zero.
    InferStrategy(transactions_map, new_config)

    return new_config


def InferStatus(referenced_chain_ids: Set[str],
                active_chain_ids: Set[str],
                config: configlib.Config):
    """Update (mutate) `status` on chains, from transactions."""

    # Infer the status of non-finalized chains.
    for chain in config.chains:
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
        chain.status = (ChainStatus.ACTIVE
                        if chain.chain_id in active_chain_ids
                        else ChainStatus.CLOSED)


def InferStrategy(transactions_map: Dict[str, Record], config: configlib.Config):
    """Update (mutate) `strategy` on chains, inferring where missing."""

    for chain in config.chains:
        # Don't override already present values for `strategy`.
        if chain.strategy:
            continue

        # Fetch the list of initial transactions.
        init_tuples = []
        for transaction_id in itertools.chain(chain.ids, chain.auto_ids):
            rec = transactions_map.get(transaction_id, None)
            if rec is not None:
                init_tuples.append(
                    (rec.transaction_id, rec.order_id, rec.datetime, rec.effect))
        init_transaction_ids = InitialTransactions(init_tuples)
        init_transactions = [transactions_map[transaction_id]
                             for transaction_id in init_transaction_ids]

        strategy, signature = strategylib.InferStrategy(init_transactions)
        if strategy:
            # Note: Mutate in-place. (I know.)
            chain.strategy = strategy
        else:
            logging.warning(f"Could not infer strategy for chain "
                            f"http://localhost:5000/chain/{chain.chain_id} : {signature}")


def AcceptChain(chain: configlib.Chain,
                group: Optional[str]=None,
                status: Optional[int]=ChainStatus.FINAL):
    """Mutate the chain to bake the ids and modify some of its attributes."""

    # Move `auto_ids` to `ids`.
    for iid in chain.auto_ids:
        chain.ids.append(iid)
    chain.ClearField('auto_ids')

    # Set status.
    if status is not None:
        chain.status = status

    # Set group.
    if group:
        chain.group = group
