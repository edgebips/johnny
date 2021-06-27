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

import hashlib
from decimal import Decimal
from typing import List, Tuple, NamedTuple, Optional


ZERO = Decimal(0)


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

        if effect == 'CLOSING':
            if (self.quantity * quantity) >= 0:
                return False
            self.quantity += quantity
            return True

        raise ValueError("Invalid effect: '{}'".format(effect))

    def expire(self,quantity: Decimal) -> bool:
        """Expire a given quantity. If some expiration was carried out, True is returned."""
        del quantity
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
