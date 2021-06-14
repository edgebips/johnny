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
from pprint import pprint
import collections
import itertools
import hashlib
import logging
from decimal import Decimal
from typing import Any, Dict, Tuple, Mapping, NamedTuple, Optional

from johnny.base.etl import petl, AssertColumns, Table, Record
from johnny.base import instrument


ZERO = Decimal(0)


class InstKey(NamedTuple):
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
    inventories, match_map, expire_map, effect_map = _CreateMatchMappings(transactions)

    # Insert Mark rows to close out the positions virtually.
    closing_transactions = _CreateClosingTransactions(inventories, match_map, closing_time)

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
        elif r.rowtype == 'Trade':
            assert effect == effect_map[r.transaction_id]
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
    invs = collections.defaultdict(FifoInventory)

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

    inventories = {key: inv.position() for key, inv in invs.items()}
    return inventories, match_map, expire_map, effect_map


def _CreateClosingTransactions(inventories: Mapping[str, Any],
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
    dt_today = dt_mark.date() - datetime.timedelta(days=2)
    for key, (quantity, basis, match_id) in inventories.items():
        if quantity == ZERO:
            continue
        (account, symbol, expiration) = key

        # This is an existing position; insert a mark.
        mark_id = next(mark_ids)
        transaction_id = "^mark{:06d}".format(mark_id)
        rowtype = 'Mark'
        description = f"Mark-to-market: {quantity} {symbol}"
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


def _CreateMatchId(transaction_id: str) -> str:
    """Create a unique match id from the given transaction id."""
    md5 = hashlib.blake2s(digest_size=4)
    md5.update(transaction_id.encode('ascii'))
    return "&{}".format(md5.hexdigest())


class MinInventory:
    """Minimal inventory matching on quantity, with open/close fields.

    This matching method leverages opening/closing information and only tracks
    quantity. This is used upstream in simpler parts of transactions processing.
    """
    def __init__(self):
        self.quantity = ZERO

    def trade(self, quantity: Decimal, effect: str) -> bool:
        """Update the inventory, possibly ignoring if closing against insufficient size.

        Args:
          quantity: A signed value.
          effect: Either `OPENING` or `CLOSING`.
        Returns:
          True if the update was applied; False if it was ignored.
        """
        if effect == 'OPENING':
            # Check consistency of signs.
            assert (self.quantity * quantity) >= 0, (self.quantity, quantity)
            self.quantity += quantity
            return True

        elif effect == 'CLOSING':
            if (self.quantity * quantity) >= 0:
                return False
            self.quantity += quantity
            return True

        else:
            raise ValueError("Invalid effect: '{}'".format(effect))

    def expire(self, quantity: Decimal) -> bool:
        if self.quantity != ZERO:
            self.quantity = ZERO
            return True
        return False


class MatchInventory:
    """Simple inventory object which implements matching of lots of a single instrument.

    The method we implement here avoids having to split rows for partial
    matches. It simplifies the process of partial matches by joining together
    partial reductions, e.g., the following sequences of changes are linked
    together:

      [+1, +1, -1, -1]
      [+1, +1, -2]
      [+2, -1, -1]
      [+1, -2, +1]
      [+2, -1, -2, +1]

    Basically as long as there is a reduction against an existing position, the
    same match id is used. The match id is derived from the opening position. An
    improvement in resolution would split some of these matches, e.g.,

      [+1, +1, -1, -1] --> [+1, -1], [-1, -1]
      [+2, -1, -2, +1] --> [+2, -1, -1], [-1, +1]

    but if you're fine with a bit more linkage, this will do.
    """

    create_id_fn = staticmethod(_CreateMatchId)

    def __init__(self):
        # The current quantity of the instrument.
        self.quantity = ZERO

        # The current match id being assigned.
        self.match_id: str = None

    def match(self, quantity: Decimal, transaction_id: str) -> Tuple[Decimal, str]:
        """Match the given change against the inventory state.
        Return the signed matched size and match id to apply.
        """
        # Add to the existing quantity; keep the same transaction id.
        if self.match_id is None:
            self.match_id = self.create_id_fn(transaction_id)

        if self.quantity * quantity >= ZERO:
            matched = ZERO
        elif abs(quantity) < abs(self.quantity):
            matched = quantity
        else:
            matched = -self.quantity

        self.quantity += quantity
        match_id = self.match_id

        if self.quantity == ZERO:
            self.match_id = None

        return (matched, match_id)

    def expire(self, transaction_id: str) -> Tuple[Decimal, str]:
        """Match the inventory state.
        Return the signed matched size and match id to apply.
        """
        if self.quantity == ZERO:
            return ZERO, None

        matched = -self.quantity
        self.quantity = ZERO

        match_id = (self.create_id_fn(transaction_id)
                    if self.match_id is None
                    else self.match_id)
        self.match_id = None
        return (matched, match_id)


class Lot(NamedTuple):
    """A single lot with basis."""
    quantity: Decimal
    basis: Decimal


class FifoInventory:
    """Simple inventory object which implements matching of lots of an instrument as FIFO.

    The method we implement here can split rows for partial matches, and
    maintains cost basis, so we can calculate the final cost of the remaining
    lots.
    """

    create_id_fn = staticmethod(_CreateMatchId)

    def __init__(self):
        # A list of insertion-ordered lots as (quantity, basis) pairs.
        self.lots: List[Lot] = []

        # The current match id being assigned.
        self.match_id: str = None

    def match(self,
              quantity: Decimal,
              cost: Decimal,
              transaction_id: str) -> Tuple[Decimal, Decimal, Optional[str]]:
        """Match the given change against the inventory state.
        Return the absolute matched size, absolute basis, and match id to apply.
        """
        # Note: You could calculate unrealized P/L on matches.

        # Add to the existing quantity; keep the same transaction id.
        if self.match_id is None:
            self.match_id = self.create_id_fn(transaction_id)

        # Notes: `basis` and `matched` are positive.
        basis = ZERO
        matched = ZERO
        if not self.lots:
            # Adding to an empty inventory.
            self.lots.append(Lot(quantity, cost))
        else:
            # Calculate the sign of the current position.
            sign = 1 if self.lots[0].quantity >= 0 else -1
            if sign * quantity >= ZERO:
                # Augmentation on existing position.
                self.lots.append(Lot(quantity, cost))
            else:
                # Reduction.
                # Notes: lot_matched` and `remaining` are positive.
                remaining = -sign * quantity
                while self.lots and remaining > ZERO:
                    lot = self.lots.pop(0)

                    lot_matched = min(sign * lot.quantity, remaining)
                    matched += lot_matched
                    basis += lot_matched * lot.basis
                    remaining -= lot_matched

                    if lot_matched < sign * lot.quantity:
                        # Partial lot matched; reinsert remainder.
                        self.lots.insert(0, Lot(lot.quantity - sign * lot_matched, lot.basis))
                        break

                # Remaining quantity to insert to cross.
                if remaining != ZERO:
                    self.lots.append(Lot(-sign * remaining, cost))

        match_id = self.match_id
        if not self.lots:
            self.match_id = None

        return (matched, basis, match_id)

    def expire(self, transaction_id: str) -> Tuple[Decimal, Decimal, Optional[str]]:
        """Match the inventory state.
        Return the signed matched size and match id to apply.
        """
        if not self.lots:
            return ZERO, ZERO, None

        sign = 1 if self.lots[0].quantity >= 0 else -1
        matched = sign * sum(lot.quantity for lot in self.lots)
        basis = sign * sum(lot.quantity * lot.basis for lot in self.lots)
        self.lots = []

        match_id = (self.create_id_fn(transaction_id)
                    if self.match_id is None
                    else self.match_id)
        self.match_id = None

        return (matched, basis, match_id)

    def position(self) -> Tuple[Decimal, Decimal, Optional[str]]:
        """Return the sum total quantity and cost basis."""
        if not self.lots:
            return ZERO, ZERO, None
        position = sum(lot.quantity for lot in self.lots)
        basis = sum(abs(lot.quantity) * lot.basis for lot in self.lots)
        return position, basis, self.match_id
