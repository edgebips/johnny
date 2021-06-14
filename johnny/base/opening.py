"""Module to synthesize opening balances.

This module accepts a list of transactions and a list of final positions in
their normalized formats and will run the transactions to synthesize opening
positions at the beginning of the transactions log.
"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import datetime
from pprint import pprint
import collections
import itertools
import hashlib
from decimal import Decimal
from typing import Any, Dict, Tuple, Mapping, NamedTuple, Optional

from johnny.base.etl import petl, Table, Record
from johnny.base import instrument


ZERO = Decimal(0)


PriceDB = Mapping[Tuple[str, datetime.date], Decimal]


def Open(transactions: Table, positions: Table, price_db: PriceDB) -> Table:
    """Synthesize opening positions.

    The resulting list of rows are transactions that create synthetic initial
    positions that correspond to the history of the log. `transactions` is the
    list of transactions at the tail of a log and `positions` is the list of
    positions at the END of the interval, and has to match the interval of the
    end of the transactions log in order to produce correct results.
    """

    # Apply all transactions forward to compute the final state.
    inventory = collections.defaultdict(Decimal)
    for rec in transactions.records():
        key = (rec.account, rec.symbol)
        if rec.rowtype == 'Expire':
            # Note: We ignore the sign and quantity and rely on the accumulated
            # inventory instead.
            inventory[key] = ZERO
        else:
            sign = 1 if rec.instruction == 'BUY' else -1
            inventory[key] += sign * rec.quantity

    # Remove inventory of final positions.
    for rec in positions.records():
        key = (rec.account, rec.symbol)
        inventory[key] -= rec.quantity

    # Get the earliest date.
    rec = next(iter(transactions.head(1).records()))
    first_dt = datetime.datetime.combine(
        rec.datetime.date() - datetime.timedelta(days=1),
        datetime.time(23, 59, 59))

    # Clean up zero positions.
    header = ('account', 'transaction_id', 'datetime', 'rowtype', 'order_id',
              'symbol', 'effect', 'instruction', 'quantity',
              'price', 'cost', 'commissions', 'fees', 'description')
    opening_rows = [header]
    for (account, symbol), quantity in inventory.items():
        if quantity == ZERO:
            continue
        instruction = 'BUY' if quantity < ZERO else 'SELL'
        transaction_id = GetTransactionId(account, symbol)

        # TODO(blais): Figure out original cost of the positions removing the
        # basis from the accumulated trades.
        price_key = (symbol, first_dt.date())
        price = price_db.get(price_key, ZERO)
        cost = quantity * price

        opening_rows.append(
            (account, transaction_id, first_dt, 'Open', None, symbol,
             'OPENING', instruction, abs(quantity), price, cost, ZERO, ZERO,
             f'Opening balance for {symbol}'))
    opening = instrument.Expand(petl.wrap(opening_rows), 'symbol')

    return petl.cat(opening, transactions)


def GetTransactionId(account: str, symbol: str) -> str:
    """Generate a unique transaction id."""
    h = hashlib.blake2s(digest_size=6)
    h.update(account.encode('ascii'))
    h.update(symbol.encode('ascii'))
    return h.hexdigest()
