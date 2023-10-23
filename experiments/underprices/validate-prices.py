#!/usr/bin/env python3
"""Validate parameters of the prices pulled from the database and produce a
table.

Problems:

- Stock name changes are problematic, as the upstream retail APIs simply aren't
  designed to handle this well.

- Stock splits are handled by pulling data from Yahoo and correcting the prices.

- The pricing VIX options requires treatment beyond that of BSM as intrinsic
  values don't respect the usual basic constraints.

- Chains containing stocks aren't handled for now.

"""

import collections
import functools
import datetime
import time
import re
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
import py_vollib.black_scholes.implied_volatility
from py_vollib import black_scholes
from py_lets_be_rational import exceptions

from johnny.base import config as configlib
from johnny.base import chains as chainslib
from johnny.base import mark
from johnny.base import instrument
from johnny.base.etl import petl
from johnny.base.etl import Record


STOCK_RENAMES = {"LB": ("BBWI", datetime.date(2021, 8, 3))}


Candle = collections.namedtuple("Candle", "timestamp open low high close volume")
Q = Decimal("0.00001")
Q2 = Decimal("0.01")


def price_history_to_arrays(history):
    """Convert response from GetPriceHistory to NumPy arrays."""
    candles = history["candles"]
    num = len(candles)
    convert = lambda fname, dtype: numpy.fromiter(
        (candle[fname] for candle in candles), dtype=dtype, count=num
    )
    return Candle(
        convert("datetime", int) / 1000,
        convert("open", float),
        convert("low", float),
        convert("high", float),
        convert("close", float),
        convert("volume", int),
    )


def interpolate_price(
    candles: Candle, timestamp: int, debug: Optional[bool] = False
) -> Optional[float]:
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
    price = (1 - fraction) * price_before + fraction * price_after
    if ts_before == -numpy.inf or ts_after == +numpy.inf:
        return None

    dt_before = datetime.datetime.fromtimestamp(ts_before)
    dt_after = datetime.datetime.fromtimestamp(ts_after)
    dtime = datetime.datetime.fromtimestamp(timestamp)
    if debug:
        print(
            dt_before,
            dtime,
            dt_after,
            " = ",
            fraction,
            "   ",
            price_before,
            price,
            price_after,
        )
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


ONE = Decimal(1)


def get_split_adjustment(rec: Record) -> Decimal:
    """Compute the split adjustment."""
    multiplier = 1
    if rec.db is not None:
        splits = rec.db[2]
        if splits:
            timestamp = int(rec.datetime.timestamp())
            for split_timestamp, split in splits:
                if split_timestamp > timestamp:
                    multiplier *= split
    return multiplier


def get_stock_price(rec: Record) -> Decimal:
    return (
        Decimal(rec.db[0] * rec.split_adj).quantize(Q2)
        if rec.db is not None and rec.db[0] is not None
        else Decimal(0)
    )


EXPIRATION_TIME = datetime.time(16, 0, 0)
DAY_SECS = 24 * 60 * 60
RISK_FREE_RATE = 0.0050


def get_days_to_expiration(rec: Record) -> Decimal:
    """Return the time to expiration in days."""
    if not rec.expiration:
        return rec.expiration
    dt_expiration = datetime.datetime.combine(rec.expiration, EXPIRATION_TIME)
    time_secs = (dt_expiration - rec.datetime).total_seconds()
    return time_secs / DAY_SECS


def get_implied_volatility(rec: Record) -> Decimal:
    """If possible, compute the IV."""
    if not re.match(".*Option$", rec.instype) or not (
        rec.expi_days and rec.stock_price
    ):
        return None

    time_annual = rec.expi_days / 365
    flag = rec.putcall[0].lower()
    option_price = float(rec.price)
    if option_price <= 0:
        vol = 0
    else:
        stock_price = float(rec.stock_price)
        strike_price = float(rec.strike)
        try:
            vol = black_scholes.implied_volatility.implied_volatility(
                option_price,
                stock_price,
                strike_price,
                time_annual,
                RISK_FREE_RATE,
                flag,
            )
        except exceptions.VolatilityValueException as exc:
            logging.debug(
                "Skip chain with invalid constraint: {} ({})".format(
                    rec.chain_id,
                    (
                        option_price,
                        stock_price,
                        strike_price,
                        "|",
                        time_annual,
                        RISK_FREE_RATE,
                        flag,
                        ":",
                        exc,
                    ),
                )
            )
            vol = None
    return vol


def print_group(giter):
    """Print a group iterator."""
    rows = list(giter)
    table = petl.wrap([rows[0].flds] + rows)
    print(table.lookallstr())


def process_matches(greeks_mapping, miter):
    rows = list(miter)
    table = petl.wrap([rows[0].flds] + rows)

    # Fail chains with positions other than equity options. We don't have
    # futures outrights pricing data, and don't support static deltas just yet.
    if set(table.values("rowtype")) != {"EquityOption"}:
        logging.debug("Invalid chain with non equity options")
        return None

    # Fail chains with missing IV.
    if table.select("iv", None).nrows() > 0:
        logging.debug("Invalid chain with missing implied volatilities")
        return None

    stack = []
    for rec in table.namedtuples():
        if rec.effect == "OPENING":
            stack.append(rec)
        elif rec.effect == "CLOSING":
            quantity = rec.quantity
            while quantity != 0:
                if not stack:
                    return None
                rec_last = stack.pop()
                match_quantity = min(quantity, rec_last.quantity)
                if match_quantity < rec_last.quantity:
                    stack.append(
                        rec_last._replace(quantity=rec_last.quantity - match_quantity)
                    )
                quantity -= match_quantity
                opening = rec_last._replace(quantity=match_quantity)
                closing = rec._replace(quantity=match_quantity)
                if 0:
                    print(match_quantity, quantity)
                    print(opening)
                    print(closing)
                    print()
                greeks = compute_greek_differentials(opening, closing)
                greeks_mapping[closing.transaction_id] = greeks

    assert quantity == float(0)


def process_chain(giter):
    """Process an entire chain."""
    rows = list(giter)
    table = petl.wrap([rows[0].flds] + rows)
    print("-" * 120)
    print(table.lookallstr())

    # greeks_mapping
    matches = table.aggregate("match_id", partial(process_matches, greeks_mapping))
    _ = list(matches)


@click.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    help="Configuration filename. Default to $JOHNNY_CONFIG",
)
@click.option(
    "--retry", is_flag=True, default=False, help="Clear missing values before starting"
)
@click.argument("database")  # help="Location of DBM price database"
def main(config: Optional[str], retry: Optional[bool], database: str):
    logging.basicConfig(level=logging.INFO, format="%(levelname)-8s: %(message)s")

    with shelve.open(database, "r") as pricedb:
        fields = (
            "chain_id",
            "rowtype",
            "datetime",
            "symbol",
            "underlying",
            "price",
            "strike",
            "expiration",
            "putcall",
        )

        filename = configlib.GetConfigFilenameWithDefaults(config)
        config = configlib.ParseFile(filename)
        transactions = (
            petl.frompickle(config.output.transactions_pickle)
            .applyfn(instrument.Expand, "symbol")
            # Fetch the database.
            .addfield(
                "db",
                lambda r: pricedb.get("{}.{}".format(r.underlying, r.datetime), None),
            )
            # Compute the split adjustment and split adjusted stock price.
            .addfield("split_adj", get_split_adjustment)
            .addfield("stock_price", get_stock_price)
            # Compute days to expiration.
            .addfield("expi_days", get_days_to_expiration)
            # Compute implied volatility.
            .addfield("iv", get_implied_volatility)
            .cutout("db")
        )

        # print(transactions.aggregate('match_id', print_group).lookallstr())

        chains = (
            transactions
            # Process entire chains.
            .aggregate("chain_id", process_chain)
        )

        # # Move these filters in the chain aggregator.
        # .selectin('instype', {'Equity', 'EquityOption'})
        # # TODO(blais): Replace this with 'IndexOption'.
        # .selectnotin('underlying', {'VIX'})

        # .cut(fields)
        # .aggregate('chain_id', process_chain))

        _ = list(chains.records())

        if 0:
            # def process_chain(giter):
            #     table = petl.wrap([fields] + list(giter))

            #     # Note: This is largely uncompromising; we still have to handle all
            #     # the corner cases: active positions, missing data, prices which
            #     # fail constraints implicit in BSM (e.g. intrinsic value), chains
            #     # with static deltas, etc.

            #     # Skip chains with marks, because we don't have live prices.
            #     first_row = next(iter(table.records()))
            #     if table.selecteq('rowtype', 'Mark').nrows() > 0:
            #         logging.info("Skip active chain: {}".format(first_row.chain_id))
            #         return

            #     # # Join table data. If some of the rows have some unavailable data,
            #     # # skip them.
            #     # table = (table
            #     #          # Fetch the database data.
            #     #          .addfield('price_key',
            #     #                    lambda r: "{}.{}".format(r.underlying, r.datetime))
            #     #          .addfield('db', lambda r: pricedb[r.price_key]))
            #     # if table.select(lambda r: r.db is None or r.db[0] is None).nrows() > 0:
            #     #     logging.info("Skip chain with no data: {}".format(first_row.chain_id))
            #     #     return

            #     # # Compute split-adjusted price.
            #     # table = (table
            #     #          .addfield('split_adj', split_adjustment)
            #     #          .addfield('stock_price',
            #     #                    lambda r: Decimal(r.db[0] * r.split_adj).quantize(Q2))
            #     #          .cutout('price_key', 'db'))

            #     # Skip chains containing stocks for now.
            #     if table.selectin('rowtype', {'Equity', 'Future'}).nrows() > 0:
            #         logging.info("Skip chain with static deltas: {}".format(first_row.chain_id))
            #         return

            #     # Process each of the options positions.

            #     ## TODO(blais): Add BSM implied vol as column

            #     invalid = False
            #     for rec in table.records():
            #         dt_expiration = datetime.datetime.combine(rec.expiration, expiration_time)
            #         time_secs = (dt_expiration - rec.datetime).total_seconds()
            #         time_annual = time_secs / year_secs
            #         flag = rec.putcall[0].lower()
            #         option_price = float(rec.price)
            #         stock_price = float(rec.stock_price)
            #         strike_price = float(rec.strike)

            #         if option_price <= 0:
            #             ivol = 0
            #         else:
            #             try:
            #                 ivol = black_scholes.implied_volatility.implied_volatility(
            #                     option_price, stock_price, strike_price,
            #                     time_annual, risk_free_rate, flag)
            #             except exceptions.VolatilityValueException as exc:
            #                 logging.info("Skip chain with invalid constraint: {} ({})".format(
            #                     first_row.chain_id, (
            #                         option_price, stock_price, strike_price, '|',
            #                         time_annual, risk_free_rate, flag, ':', exc)))
            #                 invalid = True
            #                 break
            #     if invalid:
            #         return

            filename = configlib.GetConfigFilenameWithDefaults(config)
            config = configlib.ParseFile(filename)
            transactions = (
                petl.frompickle(config.output.transactions_pickle)
                .applyfn(instrument.Expand, "symbol")
                # Move these filters in the chain aggregator.
                .selectin("instype", {"Equity", "EquityOption"})
                # TODO(blais): Replace this with 'IndexOption'.
                .selectnotin("underlying", {"VIX"})
                .cut(fields)
                .aggregate("chain_id", process_chain)
            )
            if 1:
                print(transactions.lookallstr())
                raise SystemExit

    if 0:
        for row in transactions.records():
            print(row)
            continue

            dtime = row["datetime"]
            symbol = row["underlying"]
            key = "{}.{}".format(symbol, dtime)
            if key in pricedb:
                logging.info(f"Skipping for {symbol}, {dtime}")
                continue

            delta = datetime.timedelta(minutes=15)
            start = int((dtime - delta).timestamp() * 1000)
            end = int((dtime + delta).timestamp() * 1000)
            hist = tdapi.GetPriceHistory(
                symbol=symbol,
                frequency=5,
                frequencyType="minute",
                startDate=start,
                endDate=end,
            )
            if IsRateLimited(hist):
                logging.info("Throttling for a few seconds; not retrying failed query.")
                time.sleep(5)
                continue
            if not ("candles" in hist and hist["candles"]):
                logging.info(f"Empty for {symbol}, {dtime}: {hist}")
                pricedb[key] = None
                continue

            # Compute the price.
            candles = price_history_to_arrays(hist)
            price = interpolate_price(candles, dtime.timestamp())
            logging.info(f"Storing for  {symbol}, {dtime}: {price}")
            pricedb[key] = (price, hist)

            # Address rate limitations in the API.
            time.sleep(0.1)


if __name__ == "__main__":
    main()
