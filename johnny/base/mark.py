"""Convert transactions to chains.
"""
__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import functools
from functools import partial
import logging
import os
from decimal import Decimal
from typing import List, Optional, Mapping

from more_itertools import first, last
import click
import ameritrade as td

from johnny.base import config as configlib
from johnny.base import discovery
from johnny.base import consolidate
from johnny.base import instrument
from johnny.base import opening
from johnny.base import match
from johnny.base import match2
from johnny.base.etl import petl, Table, Record, WrapRecords
from johnny.sources.tastyworks_csv.positions import ReadPricesFromPositionsFile
from johnny.sources.thinkorswim_csv import symbols as tdsymbols
Instrument = instrument.Instrument


ZERO = Decimal(0)


# def FetchPricesForMark(transactions: Table) -> Table:
#     """Fetch live prices for all marks."""
#
#     config = td.config_from_dir(os.getenv("AMERITRADE_DIR"))
#     api = td.open(config)
#
#     marks = (transactions
#              .selecteq('rowtype', 'Mark'))
#
#     tdsyms = []
#     for row in marks.records():
#         inst = instrument.FromString(row.symbol)
#         tdsyms.append(tdsymbols.FromInstrument(inst))
#     tdsyms = [x for x in tdsyms if x.startswith('/')]
#
#     pp(tdsyms)
#     quotes = api.GetQuotes(symbol=",".join(tdsyms))
#     table = (petl.fromdicts(quotes.values()))
#              # .cut('symbol', 'description', 'bidPrice', 'askPrice', 'lastPrice', 'mark',
#              #      'volatility', 'delta', 'gamma', 'theta', 'vega', 'rho', 'underlyingPrice'))
#     print(table.lookallstr())

# Note: Fetching quotes for options on TD requires using the streaming client
# for futures.


def FetchPricesFromTransactionsLog(transactions: Table) -> Mapping[str, Decimal]:
    """Extract the latest price for each symbol from the transactions log. Return a
    dict of (symbol, mark). This is the "poor man's" way to produce some
    semi-reasonable prices (i.e., without accessing any network resource), just
    taking the most recent price you've seen in the log. A fallback of sorts.
    """
    def fn(symbol, group):
        rec = last(group)
        return symbol, rec.price, rec.datetime
    table = (transactions
             .sort('datetime')
             .select(lambda r: r.rowtype not in {'Open', 'Mark'})
             .rowreduce('symbol', fn, header=['symbol', 'price', 'datetime']))
    return table.lookupone('symbol', 'price')


def GetPriceMap(transactions: Table, config: configlib.Config) -> Mapping[str, Decimal]:
    """Produce a mapping of (symbol, mark-price)."""
    # Read prices from the transactions log itself. This is the baseline.
    price_map = FetchPricesFromTransactionsLog(transactions)

    # Read the positions files as a source of prices and override the price map
    # with those prices where present.
    logtables = discovery.ReadConfiguredInputs(
        config, configlib.Account.LogType.POSITIONS)
    positions = logtables[configlib.Account.LogType.POSITIONS]
    if positions:
        pos_price_map = (positions
                         .convert('mark', abs)
                         .lookupone('symbol', 'mark'))
        price_map.update(pos_price_map)

    return price_map


def Mark(transactions: Table, price_map: Mapping[str, Decimal]) -> Table:
    """Mark the live positions."""

    def set_mark(price: Decimal, row: Record) -> Decimal:
        "Set mark price from price database."
        return price if row.rowtype != 'Mark' else (price_map.get(row.symbol, price))

    def get_cost(cost: Decimal, rec: Record) -> Decimal:
        "Calculate cost from updated price."
        if rec.rowtype != 'Mark':
            return cost
        sign = -1 if rec.instruction == 'BUY' else +1
        return sign * rec.quantity * rec.price * rec.multiplier

    return (transactions
            .convert('price', set_mark, pass_row=True)
            .applyfn(instrument.Expand, 'symbol')
            .convert('cost', get_cost, pass_row=True)
            .applyfn(instrument.Shrink))
