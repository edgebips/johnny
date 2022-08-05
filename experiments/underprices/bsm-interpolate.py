#!/usr/bin/env python3
"""Back out any one of the missing parameters of BSM from the other parameters.

We run a simple root finding / optimization on top of the forward evaluation,
not using analyticals. Rates, volatility are supplied as annual fractions. Time
is supplied as days.
"""

import collections
import functools
import datetime
import time
import math
from math import sqrt, exp
from functools import partial
import logging
import shelve
import os
from decimal import Decimal
from typing import List, Optional, Mapping

import petl

petl.config.look_style = "minimal"
import click
import numpy
import seaborn as sns
from scipy.stats import norm
from matplotlib import pyplot
import matplotlib

matplotlib.use("Qt5Agg")


Q = Decimal("0.01")


def black_scholes(
    S: float, K: float, T: float, r: float, sigma: float, putcall: str
) -> float:
    """Evaluate BS'73 for a call or put option."""
    pc = 1 if putcall == "CALL" else -1
    sigma_dt = sigma * sqrt(T)
    d1 = (numpy.log(S / K) + (r + sigma**2 / 2) * T) / sigma_dt
    d2 = d1 - sigma_dt
    return pc * S * norm.cdf(pc * d1) - (pc * K * exp(-r * T)) * norm.cdf(pc * d2)


@click.command()
@click.argument("csv_filename", type=click.Path(exists=True))
def main(csv_filename: str):
    params = petl.fromcsv(csv_filename)
    # print(params.fieldnames())
    # print(params.head().lookallstr())

    header = ("description", "putcall", "K", "V", "Vtheo", "vol")
    rows = [header]
    for rec in params.records():
        V = float(rec.mark)
        S = float(rec.underlying)
        K = float(rec.strike)
        T = int(rec.days) / 365
        r = float(rec.interestRate) / 100
        vol = float(rec.volatility) / 100
        # print((V, S, K, T, r, vol))
        Vtheo = black_scholes(S, K, T, r, vol, rec.putcall)
        rows.append((rec.description, rec.putcall, K, V, Vtheo, max(0, vol)))
    table = petl.wrap(rows)
    pyplot.plot(table.values("K"), table.values("V"), ".-", label="market", alpha=0.5)
    pyplot.plot(table.values("K"), table.values("Vtheo"), ".-", label="theo", alpha=0.5)
    pyplot.legend()
    pyplot.show()

    calls = table.selecteq("putcall", "CALL")
    puts = table.selecteq("putcall", "PUT")
    pyplot.plot(
        calls.values("K"), calls.values("vol"), ".-", label="vol_calls", alpha=0.5
    )
    pyplot.plot(puts.values("K"), puts.values("vol"), ".-", label="vol_puts", alpha=0.5)
    pyplot.legend()
    pyplot.show()

    # print(.lookallstr())


if __name__ == "__main__":
    main()
