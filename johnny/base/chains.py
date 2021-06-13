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
from enum import Enum
from typing import Any, Dict, Iterable, Iterator, List, Mapping, NamedTuple, Union, Set
from typing import Optional, Tuple
import argparse
import hashlib
import functools
import collections
import pprint
import datetime
import itertools
from os import path
import os
import logging
import re
import sys
from more_itertools import first, unzip
from pprint import pformat

import numpy
from dateutil import parser
import networkx as nx
from matplotlib import pyplot

from johnny.base import instrument
from johnny.base.etl import petl, AssertColumns, PrintGroups, Record, Table, WrapRecords


ZERO = Decimal(0)


def Group(transactions: Table,
          by_match=True,
          by_order=True,
          by_time=True,
          explicit_chains=None,
          transaction_links=None,
          order_links=None) -> Table:
    """Aggregate transaction rows by options chain.

    Args:
      transactions: A normalized transactions log with a 'match' column.
      by_match: A flag, indicating that matching transactions should be chained.
      by_order: A flag, indicating that transactions from the same order should be chained.
      by_time: A flag, indicating that transactions overlapping over time should be chained.
      explicit_chains: An optional mapping of (transaction-id, unique-chain-id).
      transaction_links: A list of [transaction-id, ...] lists of explicit linkage.
      order_links: A list of [order-id, ...] lists of explicit linkage.
    Returns:
      A modified table with an extra "chain" column, identifying groups of
      related transactions, by episode, or chain.
    """
    # Create the graph.
    graph = CreateGraph(transactions, by_match, by_order, by_time,
                        explicit_chains, transaction_links, order_links)

    # Process each connected component to an individual trade.
    # Note: This includes rolls if they were carried one as a single order.
    chain_map = {}
    for cc in nx.connected_components(graph):
        chain_txns = []
        for transaction_id in cc:
            node = graph.nodes[transaction_id]
            try:
                node_type = node['type']
            except KeyError:
                raise KeyError("Node with no type for transaction: {}".format(
                    transaction_id))
            if node['type'] == 'txn':
                chain_txns.append(node['rec'])

        chain_id = ChainName(chain_txns, explicit_chains)
        for rec in chain_txns:
            chain_map[rec.transaction_id] = chain_id

    return (transactions
            .addfield('chain_id', lambda r: chain_map[r.transaction_id]))


def CreateGraph(transactions: Table,
                by_match=True,
                by_order=True,
                by_time=True,
                explicit_chains=None,
                transaction_links=None,
                order_links=None) -> nx.Graph:
    """Create a graph to link together related transactions."""

    AssertColumns(transactions,
                  ('transaction_id', str),
                  ('order_id', {None, str}),
                  ('match_id', str),
                  ('datetime', datetime.datetime),
                  ('expiration', {None, datetime.date}),
                  ('account', str),
                  ('underlying', str))

    # Extract out transactions that are explicitly chained.
    explicit_chains = explicit_chains or {}
    explicit_transactions, implicit_transactions = transactions.biselect(
        lambda rec: rec.transaction_id in explicit_chains)

    # Process explicitly specified chains.
    graph = nx.Graph()
    for rec in explicit_transactions.records():
        graph.add_node(rec.transaction_id, type='txn', rec=rec)

        # Extract explicit chains to their own components and don't link them
        # with others.
        explicit_chain = explicit_chains.get(rec.transaction_id)
        if explicit_chain:
            graph.add_node(explicit_chain, type='expchain')
            graph.add_edge(rec.transaction_id, explicit_chain)
            continue

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

    # Add explicit linkage between chains (e.g. for pairs).
    if transaction_links:
        for index, transaction_ids in enumerate(transaction_links):
            link_id = 'txnlink{}'.format(index)
            graph.add_node(link_id, type='txnlink')
            for transaction_id in transaction_ids:
                graph.add_edge(transaction_id, link_id, type='explink')
    if order_links:
        for index, order_ids in enumerate(order_links):
            link_id = 'ordlink{}'.format(index)
            graph.add_node(link_id, type='ordlink')
            for order_id in order_ids:
                graph.add_edge(order_id, link_id, type='explink')

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
                            explicit_chains=None) -> List[Tuple[str, str]]:
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
    for dt, account, underlying, expiration, match_id in sorted(spans):
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
    assert not inventory, "Inventory not empty: {}".format(inventory)

    return links, transaction_links


def _CreateChainId(transaction_id: str, _: datetime.datetime) -> str:
    """Create a unique match id from the given transaction id."""
    md5 = hashlib.blake2s(digest_size=4)
    md5.update(transaction_id.encode('ascii'))
    return "{}".format(md5.hexdigest())


def ChainName(txns: List[Record], explicit_chains: Mapping[str, str]) -> str:
    """Generate a unique chain name. This assumes 'account', 'mindate' and
    'underlying' columns."""

    # Look for an explicit chain id.
    for txn in txns:
        explicit_chain_id = explicit_chains.get(txn.transaction_id, None)
        if explicit_chain_id is not None:
            return explicit_chain_id

    # Note: We don't know the max date, so we stick with the front date only in
    # the readable chain name.
    first_txn = next(iter(sorted(txns, key=lambda r: r.datetime)))
    return ".".join([first_txn.account,
                     "{:%y%m%d_%H%M%S}".format(first_txn.datetime),
                     first_txn.underlying.lstrip('/')])
