#!/usr/bin/env python3
"""Pull all prices of underlyings over time from an API source to a local database.

The purpose is to attach to each transaction row the price of the corresponding
underlying or stock.
"""

import collections
import functools
import datetime
import time
from functools import partial
import logging
import shelve
import os
from decimal import Decimal
from typing import List, Optional, Mapping

from more_itertools import first, last
import click
import ameritrade
from ameritrade.utils import IsRateLimited
import numpy

from johnny.base import config as configlib
from johnny.base import chains as chainslib
from johnny.base import mark
from johnny.base import instrument
from johnny.base.etl import petl



Candle = collections.namedtuple('Candle', 'timestamp open low high close volume')
Q = Decimal('0.00001')


def price_history_to_arrays(history):
    """Convert response from GetPriceHistory to NumPy arrays."""
    candles = history['candles']
    num = len(candles)
    convert = lambda fname, dtype: numpy.fromiter((candle[fname] for candle in candles),
                                                  dtype=dtype, count=num)
    return Candle(convert('datetime', int)/1000,
                  convert('open', float),
                  convert('low', float),
                  convert('high', float),
                  convert('close', float),
                  convert('volume', int))


def interpolate_price(candles: Candle,
                      timestamp: int,
                      debug: Optional[bool] = False) -> Optional[float]:
    """Compute an interpolated price using the candles we happen to have."""

    # Find the closest two data points.
    ts_before, ts_after = -numpy.inf, +numpy.inf
    index_before, index_after = None, None
    for index, ts in enumerate(candles.timestamp):
        if ts <= timestamp:
            if ts > ts_before:
                ts_before = ts
                index_before = index
        else:
            if ts < ts_after:
                ts_after = ts
                index_after = index
    price_before = candles.close[index_before]
    price_after = candles.open[index_after]

    # Convex interpolation.
    fraction = (timestamp - ts_before) / (ts_after - ts_before)
    price = (1-fraction) * price_before + fraction * price_after
    if ts_before == -numpy.inf or ts_after == +numpy.inf:
        return None

    dt_before = datetime.datetime.fromtimestamp(ts_before)
    dt_after = datetime.datetime.fromtimestamp(ts_after)
    dtime = datetime.datetime.fromtimestamp(timestamp)
    if debug:
        print(dt_before, dtime, dt_after, " = ",
              fraction, "   ",
              price_before, price, price_after)
    return price


def clear_missing(database):
    """Remove all items which have a value unfound."""
    remove_keys = set()
    with shelve.open(database) as pricedb:
        for key, value in pricedb.items():
            if value is None:
                remove_keys.add(key)
        for key in remove_keys:
            logging.info(f"Clearing item for {key}")
            del pricedb[key]



@click.command()
@click.option('--config', '-c', type=click.Path(exists=True),
              help="Configuration filename. Default to $JOHNNY_CONFIG")
@click.option('--retry', is_flag=True, default=False,
              help="Clear missing values before starting")
@click.argument('database') # help="Location of DBM price database"
def main(config: Optional[str],
         retry: Optional[bool],
         database: str):
    logging.basicConfig(level=logging.INFO, format='%(levelname)-8s: %(message)s')

    if retry:
        clear_missing(database)

    filename = configlib.GetConfigFilenameWithDefaults(config)
    config = configlib.ParseFile(filename)
    transactions = (
        petl.frompickle(config.output.transactions)
        .applyfn(instrument.Expand, 'symbol')
        .selectin('instype', {'Equity', 'EquityOption'})
        .cut('underlying', 'datetime'))
    if 0:
        print(transactions.lookallstr())
        raise SystemExit

    tdapi = ameritrade.open(ameritrade.config_from_dir())

    with shelve.open(database) as pricedb:
        for row in transactions.records():
            dtime = row['datetime']
            symbol = row['underlying']
            key = "{}.{}".format(symbol, dtime)
            if key in pricedb:
                logging.info(f"Skipping for {symbol}, {dtime}")
                continue

            delta = datetime.timedelta(minutes=15)
            start = int((dtime - delta).timestamp() * 1000)
            end = int((dtime + delta).timestamp() * 1000)
            hist = tdapi.GetPriceHistory(symbol=symbol,
                                         frequency=5, frequencyType='minute',
                                         startDate=start, endDate=end)
            if IsRateLimited(hist):
                logging.info("Throttling for a few seconds; not retrying failed query.")
                time.sleep(2)
                continue
            if not ('candles' in hist and hist['candles']):
                logging.info(f"Empty for {symbol}, {dtime}: {hist}")
                pricedb[key] = None
                continue

            # Compute the price.
            candles = price_history_to_arrays(hist)
            price = interpolate_price(candles, dtime.timestamp())
            logging.info(f"Storing for {symbol}, {dtime}: {price}")
            pricedb[key] = (price, hist)

            # Address rate limitations in the API.
            time.sleep(0.1)



if __name__ == '__main__':
    main()
