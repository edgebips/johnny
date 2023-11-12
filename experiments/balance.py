#!/usr/bin/env python3
"""Compute balance at time.
"""

from typing import Optional
from decimal import Decimal
import datetime as dt
import argparse
import logging
import click

from johnny.base import discovery
from johnny.base import mark
from johnny.base import chains as chainslib
from johnny.base import config as configlib
from johnny.base import instrument
from johnny.base.etl import petl, Table, Record


ZERO = Decimal(0)
Q2 = Decimal("0.01")


def _GetAmount(cur: Record) -> Decimal:
    # Note: Leave commissions and fees from this.
    return cur.cost + cur.cash


def _GetBalance(prv: Record, cur: Record, nxt: Record) -> Decimal:
    """Adds a balance column."""
    return (prv.balance if prv else ZERO) + cur.amount


# TODO(blais): Move this code to a library so we can call it to generate
# Beancount balance entries.
def BalanceTableForAccount(config: configlib.Config, account: str, date: dt.date):
    transactions = petl.frompickle(config.output.transactions_pickle)
    nontrades = petl.frompickle(config.output.nontrades_pickle)

    last_datetime = dt.datetime.combine(date, dt.time()) + dt.timedelta(days=1)
    t = (
        transactions.selectlt("datetime", last_datetime)
        .addfield("source", "Transactions")
        .selecteq("account", account)
        .cut(
            "datetime",
            "source",
            "rowtype",
            "description",
            "cost",
            "cash",
            "commissions",
            "fees",
        )
        .addfield("amount", _GetAmount)
    )
    # From the transactions table the 'amount' is the sum of 'cost' and 'cash'
    # only. 'commissions' and 'fees' are aggregated separate. This matches how
    # Tastytrade reports them.

    nt = (
        nontrades.selectlt("datetime", last_datetime)
        .addfield("source", "NonTrades")
        .selecteq("account", account)
        .cut("datetime", "source", "rowtype", "description", "amount")
    )
    # From nontrades, the 'amount' column is the balance affecting number.

    table = (
        petl.cat(t, nt)
        .replaceall(None, ZERO)
        .sort("datetime")
        # .addfield("change", GetChange)
        .addfieldusingcontext("balance", _GetBalance)
    )
    # When we compute the balance, we sum up the amounts. 'commissions' and
    # 'fees' should be tallied separately.

    aggtable = (
        table.aggregate(
            key=None,
            aggregation={
                "amount": ("amount", sum),
                "commissions": ("commissions", sum),
                "fees": ("fees", sum),
            },
        )
        .convertall(lambda v: v.quantize(Q2))
        .cut("commissions", "fees", "amount")
    )
    return table, aggtable


@click.command()
@click.option(
    "--config", "-c", help="Configuration filename. Default to $JOHNNY_CONFIG"
)
@click.option(
    "--date",
    "-d",
    default=str(dt.date.today()),
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="End of day balance date",
)
def main(config: Optional[str], date: dt.date):
    "General purpose command-line printer tool."
    logging.basicConfig(level=logging.INFO, format="%(levelname)-8s: %(message)s")
    filename = configlib.GetConfigFilenameWithDefaults(config)
    config = configlib.ParseFile(filename)
    account = "x20"

    consolidated, aggregates = BalanceTableForAccount(config, account, date)
    print(consolidated.lookallstr())
    print(aggregates.lookallstr())


if __name__ == "__main__":
    main()
