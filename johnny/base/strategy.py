"""Strategy inference code."""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from functools import partial
from decimal import Decimal
from typing import Any, Iterator, List, Mapping, Optional, Tuple
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
from johnny.base import instrument
from johnny.base import mark
from johnny.base.etl import AssertColumns, Record, Table


ZERO = Decimal(0)


# Mapping of signatures to strategy names.
A, B, C, D, E, F = "abcdef"
_STRATEGIES = {
    ((A, +1, None),): "Long",
    ((A, -1, None),): "Short",
    ((A, +1, "C"),): "LongCall",
    ((A, -1, "C"),): "ShortCall",
    ((A, -1, "P"),): "ShortPut",
    ((A, +1, "P"),): "LongPut",
    ((A, -1, "P"), (B, -1, "C")): "Strangle",
    ((A, -2, "P"), (B, -1, "C")): "UnevenStrangle",
    ((A, -1, "P"), (B, -2, "C")): "UnevenStrangle",
    ((A, -3, "P"), (B, -1, "C")): "UnevenStrangle",
    ((A, -1, "P"), (B, -3, "C")): "UnevenStrangle",
    ((A, +1, "P"), (B, +1, "C")): "LongStrangle",
    ((A, -1, "C"), (A, -1, "P")): "Straddle",
    ((A, +1, "C"), (A, +1, "P")): "LongStraddle",
    ((A, +1, "P"), (B, -1, "P")): "PutSpread",
    ((A, -1, "C"), (B, +1, "C")): "CallSpread",
    ((A, -1, "P"), (B, +1, "P")): "BearSpread",
    ((A, +1, "C"), (B, -1, "C")): "BullSpread",
    ((A, +1, "P"), (B, -1, "P"), (C, -1, "C"), (D, +1, "C")): "IronCondor",
    ((A, -1, "P"), (B, +1, "P"), (C, +1, "C"), (D, -1, "C")): "LongIronCondor",
    ((A, +1, "P"), (B, -1, "P"), (B, -1, "C"), (C, +1, "C")): "IronFly",
    ((A, -1, "P"), (B, -1, "C"), (C, +1, "C")): "JadeLizard",
    ((A, +1, "P"), (B, -1, "P"), (C, -1, "C")): "ReverseJadeLizard",
    ((A, -2, "P"), (B, +1, "P")): "PutRatioSpread",
    ((A, +1, "C"), (B, -2, "C")): "CallRatioSpread",
    ((A, +1, "P"), (B, -2, "P"), (C, +1, "P")): "Butterfly",
    ((A, +1, "C"), (B, -2, "C"), (C, +1, "C")): "Butterfly",
    ((A, -1, "P"), (B, +2, "P"), (C, -1, "P")): "LongButterfly",
    ((A, -1, "C"), (B, +2, "C"), (C, -1, "C")): "LongButterfly",
    # TODO(blais): What about calendars?
    # TODO(blais): What about broken wing?
}


def InferStrategy(init_transactions: list[Record]) -> Tuple[str, Any]:
    """Infer strategies on chains. This produces a strategy string and a signature
    key for the initial position."""

    # Aggregate over the same contracts.
    quantities_map = collections.defaultdict(int)
    for txn in init_transactions:
        sign = -1 if txn.instruction == "SELL" else +1
        quantities_map[txn.symbol] += sign * txn.quantity

    # Calculate GCD to normalize sizes for signature.
    # Note: Requires Python 3.9.x or above.
    gcd = math.gcd(*map(int, quantities_map.values()))
    if gcd == 0:
        return None, None

    # Expand instruments.
    inst_map = {
        symbol: instrument.FromString(symbol) for symbol in quantities_map.keys()
    }

    # Compute symbolic ordered strikes.
    strikes = sorted(set(inst.strike or ZERO for inst in inst_map.values()))
    strike_map = dict(zip(strikes, string.ascii_lowercase))

    # Produce the final signature.
    signature = []
    underlyings = set()
    expirations = set()
    for symbol, quantity in quantities_map.items():
        inst = inst_map[symbol]
        underlyings.add(inst.underlying)
        expirations.add(inst.expiration)
        sstrike = strike_map[inst.strike or ZERO]
        signature.append((sstrike, int(quantity / gcd), inst.putcall))
    signature = tuple(sorted(signature))

    # Don't bother with positions with multiple underlyings.
    if len(underlyings) != 1:
        strategy = None
    # Don't bother with positions with multiple expirations.
    elif len(expirations) != 1:
        strategy = None
    else:
        strategy = _STRATEGIES.get(signature, None)

    return strategy, signature
