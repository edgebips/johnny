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
import logging
from decimal import Decimal
from typing import Callable, List, Tuple, NamedTuple, Optional

from johnny.base.etl import AssertFields, Record


Instruction = str
Effect = str
Quantity = Decimal  # Unsigned quantity
Amount = Decimal
TransactionId = str
MatchId = str


ZERO = Decimal(0)
ONE = Decimal(1)


class MatchError(ValueError):
    """An exception raised on an irrecuperable match error."""


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

    def trade(self, quantity: Quantity, effect: Effect) -> bool:
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

        raise MatchError("Invalid effect: '{}'".format(effect))

    def expire(self, quantity: Quantity) -> bool:
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

    def match(self, quantity: Quantity,
              transaction_id: TransactionId) -> Tuple[Quantity, MatchId]:
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

    def expire(self, transaction_id: TransactionId) -> Tuple[Quantity, MatchId]:
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
    quantity: Quantity
    cost: Amount  # Unit cost, not total.


class FifoInventory:
    """Simple inventory object which implements matching of lots of an instrument as FIFO.

    This class maintains a list of lots to be matched against, and an active
    match id while the position isn't zero. When the position gets flat, the
    match-id gets reset. If a single transaction takes the position across the
    zero line, we preserve the match-id (this is an arbitrary decision). In
    addition, this class maintains cost basis. This inventory does not split
    rows for partial matches.
    """

    create_id_fn = staticmethod(_CreateMatchId)

    def __init__(self):
        # A list of insertion-ordered lots as (quantity, basis) pairs.
        self.lots: List[Lot] = []

        # The current match id being assigned.
        self.match_id: str = None

    def sign(self) -> Decimal:
        """Return the sign of the position."""
        if not self.lots:
            return ZERO
        return +ONE if self.lots[0].quantity > 0 else -ONE

    def quantity(self) -> Quantity:
        """Return the total quantity held in this inventory."""
        return sum(lot.quantity for lot in self.lots) if self.lots else ZERO

    def cost(self) -> Amount:
        """Return the total quantity held in this inventory."""
        return sum(lot.cost for lot in self.lots) if self.lots else ZERO

    def match(self,
              quantity: Quantity,
              cost: Amount,
              transaction_id: TransactionId) -> Tuple[Quantity, Amount, Optional[MatchId]]:
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
                # Reduction in FIFO order.
                # Notes: lot_matched` and `remaining` are positive.
                remaining = -sign * quantity
                while self.lots and remaining > ZERO:
                    lot = self.lots.pop(0)

                    lot_matched = min(sign * lot.quantity, remaining)
                    matched += lot_matched
                    basis += lot_matched * lot.cost
                    remaining -= lot_matched

                    if lot_matched < sign * lot.quantity:
                        # Partial lot matched; reinsert remainder.
                        self.lots.insert(0, Lot(lot.quantity - sign * lot_matched, lot.cost))
                        break

                # Remaining quantity to insert to cross.
                if remaining != ZERO:
                    self.lots.append(Lot(-sign * remaining, cost))

        match_id = self.match_id
        if not self.lots:
            self.match_id = None

        return (matched, basis, match_id)

    def expire(self, transaction_id: str) -> Tuple[Quantity, Amount, Optional[MatchId]]:
        """Match the inventory state.
        Return the signed matched size and match id to apply.
        """
        if not self.lots:
            return ZERO, ZERO, None

        sign = 1 if self.lots[0].quantity >= 0 else -1
        matched = sign * sum(lot.quantity for lot in self.lots)
        basis = sign * sum(lot.quantity * lot.cost for lot in self.lots)
        self.lots = []

        match_id = (self.create_id_fn(transaction_id)
                    if self.match_id is None
                    else self.match_id)
        self.match_id = None

        return (matched, basis, match_id)

    def position(self) -> Tuple[Quantity, Amount, Optional[MatchId]]:
        """Return the sum total (quantity, cost-basis, unique-match-id)."""
        if not self.lots:
            return ZERO, ZERO, None
        position = sum(lot.quantity for lot in self.lots)
        basis = sum(abs(lot.quantity) * lot.cost for lot in self.lots)
        return position, basis, self.match_id


# A function to produce a new row.
TxnAccumFn = Callable[[Record, str], None]


def SignedQuantity(rec: Record) -> Decimal:
    """Return the signed quantity."""
    sign = (1 if rec.instruction == 'BUY' else -1)
    return sign * rec.quantity



class OpenCloseFifoInventory:
    """Inventory object which implements explicit opening and closing of lots.

    This class maintains a list of lots to be matched against, and an active
    match id while the position isn't zero. When the position gets flat, the
    match-id gets reset. If a single transaction takes the position across the
    zero line, we preserve the match-id (this is an arbitrary decision). In
    addition, this class maintains cost basis.

    In constract to the simpler FifoInventory class above, this class will split
    matches when necessary. For example, if a position of -3 is matched against
    a position of +2, the result will be a closing of -2 and an opening of -1.
    The match id will be preserve across the flat line.

    Moreover, if the client knows that a transaction is opening or closing,
    dedicated methods are provided that will automatically synthetize
    transaction rows to correct for a missing initial position. This allows us
    to process partial transactions logs whereby the initial state of the log
    may not include existing positions.

    (More specifically: if the position is not of the right sign, we save the
    correction required at the front in order to make it right. For example, an
    opening position of +1 quantity assumes the current position is >= 0. If it
    is < 0, we bring it to 0. Ditto for closing with +1 quantity, opening with
    -1 quantity or closing with -1 quantity. We just fix it up and store the
    offset for later creation instead of returning an error.)

    In order for this class to be able to produce an arbitrary number of new
    rows, instead of requiring the client to process a return list, we call into
    an accumulator which the client provides to append new rows. This keeps the
    interface a little simpler.
    """

    create_id_fn = staticmethod(_CreateMatchId)

    def __init__(self, debug: bool=False):
        # A list of insertion-ordered lots as (quantity, cost) pairs.
        self.lots: List[Lot] = []

        # The current match id being processed.
        self._match_id: str = None

        # Enable debugging output.
        self.debug = debug

    def get_match_id(self, rec: Record) -> str:
        """Get a match id when we need one."""
        if self._match_id is None:
            self._match_id = self.create_id_fn(rec.transaction_id)
        return self._match_id

    def clear_match_id(self):
        """Clear the match id."""
        self._match_id = None

    def sign(self) -> Decimal:
        """Return the sign of the position."""
        return -ONE if self.lots and self.lots[0].quantity < 0 else +ONE

    def quantity(self) -> Quantity:
        """Return the total quantity held in this inventory."""
        return sum(lot.quantity for lot in self.lots) if self.lots else ZERO

    def cost(self) -> Amount:
        """Return the total quantity held in this inventory."""
        return sum(lot.cost for lot in self.lots) if self.lots else ZERO

    def match(self,
              rec: Record,
              accumfn: TxnAccumFn):
        """Match the given change against the inventory state.
        """
        AssertFields(rec,
                     ('transaction_id', TransactionId),
                     ('instruction', Instruction),
                     ('effect', Effect),
                     ('quantity', Quantity),
                     ('cost', Amount))

        # Notes: `basis` and `matched` are positive.
        matched_quantity = ZERO
        #matched_cost = ZERO
        squantity = SignedQuantity(rec)
        unit_cost = rec.cost / rec.quantity
        if self.debug: print('-----')
        if not self.lots:
            if self.debug: print('A')
            # Adding to an empty inventory.
            if rec.effect and rec.effect != 'OPENING':
                raise MatchError("New position not opening.")
            accumfn(rec._replace(match_id=self.get_match_id(rec),
                                 effect=rec.effect or 'OPENING'),
                    'NEW')
            self.lots.append(Lot(squantity, unit_cost))
        else:
            # Calculate the sign of the current position.
            position_sign = self.sign()
            if position_sign * squantity >= ZERO:
                if self.debug: print('B')

                # Augmentation on existing position.
                if rec.effect and rec.effect != 'OPENING':
                    raise MatchError("Augmenting position not opening.")
                accumfn(rec._replace(match_id=self.get_match_id(rec),
                                     effect=rec.effect or 'OPENING'),
                        'AUGMENT')
                self.lots.append(Lot(squantity, unit_cost))
            else:
                if self.debug: print('C')

                if rec.effect and rec.effect != 'CLOSING':
                    raise MatchError("Reducing position not closing.")

                # Reduction in FIFO order.
                # Notes: lot_matched` and `remaining` are positive.
                remaining = rec.quantity
                while self.lots and remaining > ZERO:
                    lot = self.lots.pop(0)

                    abs_lot_quantity = abs(lot.quantity)
                    matched = min(abs_lot_quantity, remaining)
                    matched_quantity += matched
                    #matched_cost += matched * lot.cost
                    remaining -= matched

                    if matched < abs_lot_quantity:
                        # Partial lot matched; insert remainder.
                        self.lots.insert(0, Lot(lot.quantity - position_sign * matched,
                                                lot.cost))
                        break

                # If after matching there is some remaining quantity, cross
                # beyond flat to the other side.
                assert matched_quantity > ZERO
                if remaining == ZERO:
                    if self.debug: print('D')
                    accumfn(rec._replace(quantity=matched_quantity,
                                         cost=matched_quantity * unit_cost,
                                         effect='CLOSING',
                                         match_id=self.get_match_id(rec)),
                            'REDUCE')
                else:
                    if self.debug: print('E')
                    accumfn(rec._replace(transaction_id=rec.transaction_id + '.1',
                                         quantity=matched_quantity,
                                         cost=matched_quantity * unit_cost,
                                         effect='CLOSING',
                                         match_id=self.get_match_id(rec)),
                            'REDUCE_CLOSING')
                    accumfn(rec._replace(transaction_id=rec.transaction_id + '.2',
                                         quantity=remaining,
                                         cost=remaining * unit_cost,
                                         effect='OPENING',
                                         match_id=self.get_match_id(rec)),
                            'REDUCE_OPENING')
                    self.lots.append(Lot(-position_sign * remaining, unit_cost))

        # If after matching the position has been cleared, we'll reset the match id.
        if not self.lots:
            self.clear_match_id()

    def opening(self,
                rec: Record,
                accumfn: TxnAccumFn):
        """Match an explicitly opening position, raise an error if incompatible.
        You will need to set the initial state of your books before matching the
        transactions log. We make no attempt to auto-correct the initial positions."""

        # If this is a reduction, it cannot be opening, it has to be closing.
        pquantity = self.quantity()
        squantity = SignedQuantity(rec)
        if pquantity * squantity < ZERO:
            raise MatchError(
                f"Invalid opening position matching {squantity} over {pquantity}.")

        # Match the new opening position.
        return self.match(rec, accumfn)

    def closing(self,
                rec: Record,
                accumfn: TxnAccumFn):
        """Match an explicitly closing position, raise an error if incompatible.
        You will need to set the initial state of your books before matching the
        transactions log. We make no attempt to auto-correct the initial positions."""

        # If this is an augmentation, it cannot be opening, it has to be closing.
        pquantity = self.quantity()
        squantity = SignedQuantity(rec)
        if pquantity * squantity >= ZERO:
            raise MatchError(
                f"Invalid closing position matching {squantity} over {pquantity}.")

        # Match the closing position against the inventory.
        return self.match(rec, accumfn)

    def expire(self,
               rec: Record,
               accumfn: TxnAccumFn):
        """Match the inventory state.
        Return the signed matched size and match id to apply.
        """
        if not self.lots:
            raise MatchError(f"Invalid expiration with no lots.")

        pquantity = self.quantity()
        instruction = 'SELL' if pquantity >= 0 else 'BUY'
        accumfn(rec._replace(rowtype='Expire',
                             instruction=instruction,
                             effect='CLOSING',
                             quantity=abs(pquantity),
                             cost=ZERO,
                             match_id=self.get_match_id(rec),),
                'EXPIRE')

        self.lots[:] = []
        self.clear_match_id()
