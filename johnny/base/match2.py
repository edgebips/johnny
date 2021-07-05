"""A new version of the matching code, which integrates state-based processing.

This code processes a transactions log in order and matches reductions of
positions against each other, in order to:

- Add missing expiration rows (thinkorswim suffers from some of these),
- Set the position effect where it is missing in the input,
- Creates a corresponding id column to save this in the table,
- Compute expiration quantities and signs,
- Add open rows for positions created before the transactions time wind,ow
- Add mark rows for positions still open and active,
- Splitting rows for futures crossing the null position boundary.

The purpose is to (a) relax the requirements on the particular importers and (b)
run through a single loop of this expensive accumulation (for performance
reasons). The result we seek is a log with the ability to be processed without
any state accumulation--all fields correctly rectified with proper
opening/closing effect and opening and marking entries. This makes any further
processing much easier and faster.

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

import datetime
import collections
import itertools
import logging
from decimal import Decimal
from typing import Any, Dict, Mapping, NamedTuple, Optional

from johnny.base.etl import petl, AssertColumns, Table, Record
from johnny.base import instrument
from johnny.base import inventories


class InstKey(NamedTuple):
    """Instrument key."""
    account: str
    symbol: str


def Process(transactions: Table,
            mark_time: Optional[datetime.datetime]=None) -> Table:
    """Run state-based processing over the transactions log.

    Args:
      transactions: The table of transactions, as normalized by each importer code.
      mark_time: The datetime to use for marking position.
    Returns:
      A fixed up table, as per the description of this module.
    """
    AssertColumns(transactions,
                  ('account', str),
                  ('transaction_id', str),
                  ('datetime', datetime.datetime),
                  ('rowtype', str),
                  ('order_id', str),
                  ('symbol', str),
                  ('effect', str),
                  ('instruction', str),
                  ('quantity', Decimal),
                  ('price', Decimal),
                  ('cost', Decimal),
                  ('commissions', Decimal),
                  ('fees', Decimal),
                  ('description', str))

    # Expand the instrument details.
    transactions = instrument.Expand(transactions, 'symbol')

    # Create a mapping of transaction ids to matches.
    invs, match_map, expire_map, effect_map = _CreateMatchMappings(transactions)

    # Shrink the instrument details.
    transactions = instrument.Shrink(transactions)

    if 0:
        print('invs')
        pp(invs)
        print()

        print('match_map')
        pp(match_map)
        print()

        print('expire_map')
        pp(expire_map)
        print()

        print('effect_map')
        pp(effect_map)
        print()

    return transactions


def _CreateMatchMappings(transactions: Table):
    """Create a mapping of transaction ids to matches.

    Args:
      transactions: The raw transactions table from the broker converter, with
        instrument expanded.
    Returns:
      A tuple of accumulated states:
        invs: A dict of of InstKey tuples to (position, basis, match-id) tuples.
        match_map: A mapping of transaction-id to match-id.
        expire_map: A mapping of transaction-id to expiration quantity.
        effect_map: A mapping of transaction-id to effect.
    """
    invs = collections.defaultdict(inventories.FifoInventory)

    # A mapping of transaction-id to match-id.
    match_map: Mapping[str, str] = {}

    # A mapping of transaction-id to expiration quantity.
    expire_map: Mapping[str, Decimal] = {}

    # A mapping of transaction-id to effect.
    effect_map: Mapping[str, str] = {}

    # A mapping of transaction-id to a list of (quantity, effect).
    split_map: Mapping[str, List[Tuple[str, str]]] = {}

    for rec in transactions.records():
        inv = invs[InstKey(rec.account, rec.symbol)]

        print(rec.effect)
        if rec.rowtype == 'Trade':
            sign = (1 if rec.instruction == 'BUY' else -1)
            basis = rec.multiplier * rec.price
            matched, __, match_id = inv.match(sign * rec.quantity, basis, rec.transaction_id)

            # We may have to split rows here.


            if matched and matched != rec.quantity:
                logging.warning("Partial matches across a flat position ideally "
                                "should split row: %s", rec)
            effect_map[rec.transaction_id] = 'CLOSING' if matched else 'OPENING'

        elif rec.rowtype == 'Expire':
            quantity, _, match_id = inv.expire(rec.transaction_id)
            expire_map[rec.transaction_id] = -quantity

        else:
            raise ValueError("Unknown row type: '{}'".format(rec.rowtype))

        if match_id:
            match_map[rec.transaction_id] = match_id

    # Clean up the residual inventories with zero positions (and cost).
    invs = {}
    for key, inv in invs.items():
        pos == inv.position()
        position, basis, match_id = pos
        if position == ZERO and basis == ZERO:
            continue
        invs[key] = pos

    return invs, match_map, expire_map, effect_map







"""

        if rec.rowtype == 'Open':
            raise ValueError
            sign = (1 if rec.instruction == 'BUY' else -1)
            # TODO(blais): Figure out how to compute a reasonable basis here.
            basis = ZERO
            _, __, match_id = inv.match(sign * rec.quantity, basis, rec.transaction_id)

"""


# TODO: Join NKE trade across accounts.
# TODO(blais): Make a page to ease creating
