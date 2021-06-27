"""Module to pair up opening and closing transactions.

This module will also process expiration directives, converting them into
closing transactions. This makes it easier later on to process chains ot events
without having to keep track of inventory state. Each 'Expire' row is replaced
by one corresponding 'Expire' row for each underlying position, with the
'quantity' field filled in and 'instruction' field replaced with 'BUY' or
'SELL'.
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


ZERO = Decimal(0)


class InstKey(NamedTuple):
    """Instrument key."""
    account: str
    symbol: str
    expiration: Optional[datetime.date]


def Match(transactions: Table, closing_time: Optional[datetime.datetime]=None) -> Dict[str, str]:
    """Compute a mapping of transaction ids to matches.

    This code will run through a normalized transaction log (see
    `transactions.md`) and match trades that reduce other ones. It will produce
    a mapping of (transaction-id, match-id). Each `match-id` will be a stable
    unique identifier across runs.

    It will also verify the 'effect' if set, or set it based on the matched
    positions if unset. This is where outright futures contracts are matched up.
    """
    AssertColumns(transactions,
                  ('account', str),
                  ('symbol', str),
                  ('rowtype', str),
                  ('instruction', str),
                  ('effect', str),
                  ('price', Decimal),
                  ('quantity', Decimal),
                  ('transaction_id', str))

    # TODO(blais): Parse the instrument, add in the multiplier and expiration
    # for necessary processing here.

    # Create a mapping of transaction ids to matches.
    invs, match_map, expire_map, effect_map = _CreateMatchMappings(transactions)

    # Insert Mark rows to close out the positions virtually.
    closing_transactions = _CreateClosingTransactions(invs, match_map, closing_time)

    def ExpiredQuantity(_, r: Record) -> Decimal:
        "Set quantity by expired quantity."
        if r.rowtype == 'Expire':
            quantity = expire_map.get(r.transaction_id, None)
            if quantity is not None:
                if r.quantity and r.quantity != abs(quantity):
                    message = ("Invalid expiration quantity for row: "
                               "record={} != matches={} from {}".format(
                                   r.quantity, abs(quantity), r))
                    raise ValueError(message)
                quantity = abs(quantity)
        else:
            quantity = r.quantity
        return quantity

    def ExpiredInstruction(_, r: Record) -> Decimal:
        "Set quantity by expired quantity."
        if not r.instruction and r.rowtype == 'Expire':
            quantity = expire_map.get(r.transaction_id, None)
            if quantity is not None:
                return 'SELL' if quantity < ZERO else 'BUY'
        return r.instruction

    def ValidateOrSetEffect(effect: str, r: Record) -> Decimal:
        "Set quantity by expired quantity."
        if effect == '?':
            return effect_map[r.transaction_id]
        if r.rowtype == 'Trade':
            if effect != effect_map[r.transaction_id]:
                logging.error("Invalid effect at: %s", r)
        return effect

    # Apply the mapping to the table.
    matched_transactions = (
        petl.cat(transactions,
                 # Note: We make sure the closing transactions are also expanded.
                 # Chains requires this.
                 instrument.Expand(closing_transactions, 'symbol'))
        .convert('quantity', ExpiredQuantity, pass_row=True)
        .convert('instruction', ExpiredInstruction, pass_row=True)
        .convert('effect', ValidateOrSetEffect, pass_row=True)
        .addfield('match_id', lambda r: match_map[r.transaction_id]))

    return matched_transactions


def _CreateMatchMappings(transactions: Table):
    """Create a mapping of transaction ids to matches."""
    invs = collections.defaultdict(inventories.FifoInventory)

    # A mapping of transaction-id to match-id.
    match_map: Mapping[str, str] = {}

    # A mapping of transaction-id to expiration quantity.
    expire_map: Mapping[str, Decimal] = {}

    # A mapping of transaction-id to effect.
    effect_map: Mapping[str, str] = {}

    for rec in transactions.records():
        instrument_key = InstKey(rec.account, rec.symbol, rec.expiration)
        inv = invs[instrument_key]

        if rec.rowtype == 'Open':
            sign = (1 if rec.instruction == 'BUY' else -1)
            # TODO(blais): Figure out how to compute a reasonable basis here.
            basis = ZERO
            _, __, match_id = inv.match(sign * rec.quantity, basis, rec.transaction_id)

        elif rec.rowtype == 'Trade':
            sign = (1 if rec.instruction == 'BUY' else -1)
            basis = rec.multiplier * rec.price
            matched, __, match_id = inv.match(sign * rec.quantity, basis, rec.transaction_id)
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

    invs = {key: inv.position() for key, inv in invs.items()}
    return invs, match_map, expire_map, effect_map


def _CreateClosingTransactions(invs: Mapping[str, Any],
                               match_map: Dict[str, str],
                               closing_time: Optional[datetime.datetime]=None) -> Table:
    """Create synthetic expiration and mark transactions to close matches."""
    closing_transactions = [(
        'account', 'transaction_id', 'rowtype', 'datetime',
        'symbol',
        'effect', 'instruction', 'quantity', 'price', 'cost', 'description',
        'commissions', 'fees'
    )]
    mark_ids = iter(itertools.count(start=1))
    dt_mark = datetime.datetime.now() if closing_time is None else closing_time
    dt_mark = dt_mark.replace(microsecond=0)

    # Allow for some margin in receiving the expiration message.
    for key, (quantity, basis, match_id) in invs.items():
        if quantity == ZERO:
            continue

        # This is an existing position; insert a mark.
        mark_id = next(mark_ids)
        transaction_id = "^mark{:06d}".format(mark_id)
        rowtype = 'Mark'
        description = f"Mark-to-market: {quantity} {key.symbol}"
        instruction = 'BUY' if quantity < ZERO else 'SELL'
        sign = -1 if quantity < ZERO else 1
        price = ZERO ## basis / quantity  TODO(blais): Get the multiplier.
        closing_transactions.append(
            (key.account, transaction_id, rowtype, dt_mark,
             key.symbol,
             'CLOSING', instruction, abs(quantity), price, sign * basis, description,
             ZERO, ZERO))
        match_map[transaction_id] = match_id

    return petl.wrap(closing_transactions)
