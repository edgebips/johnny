"""Routines to convert Johnny rows to Beancount directives.

For some limited set of chains, especially investments, I prefer to keep them at
the transactional level in Beancount, especially if they had transactions in
periods of inactive trading. Other Beancount integration used chain-level
materialization.
"""

from decimal import Decimal
from typing import List, Optional
import logging
import os
import sys
import io
from typing import Mapping, TextIO

import click

from johnny.base import discovery
from johnny.base import mark
from johnny.base import chains as chainslib
from johnny.base import config as configlib
from johnny.base import instrument
from johnny.base.etl import petl, Table, Record, WrapRecords

from beancount.core import data
from beancount.core import flags
from beancount.core.amount import Amount
from beancount.core.position import CostSpec
from beancount.parser import printer


CURRENCY = "USD"


def _CreatePosting(
    accounts: Mapping[str, str], account_id: str, number: Decimal
) -> data.Posting:
    return data.Posting(
        accounts[account_id], Amount(number, CURRENCY), None, None, None, None
    )


def RenderChainToBeancount(chain: Record, filename: str, outfile: TextIO):
    """Render a chain to Beancount syntax."""
    # print(WrapRecords([chain]))

    # TODO(blais): Make this configurable.
    accounts = {
        "gain_loss": "Income:US:Trade:PnL",
        "commissions": "Expenses:Financial:Commissions",
        "fees": "Expenses:Financial:Fees",
        "cash": "Assets:Trading:Cash",
    }

    narration = (
        f"Chain: {chain.chain_id} on {chain.underlyings} from "
        f"{chain.mindate} to {chain.maxdate} "
        f"({chain.days} days, {chain.init} init, {chain.adjust} adjustments)"
    )
    meta = data.new_metadata(filename, 1)

    for field in (
        "chain_id",
        "underlyings",
        "mindate",
        "maxdate",
        "group",
        "strategy",
        # "term",
    ):
        meta[field] = getattr(chain, field)

    txn = data.Transaction(
        meta,
        chain.maxdate,
        flags.FLAG_OKAY,
        "",
        narration,
        set(),
        {f"chain-{chain.chain_id}"},
        [],
    )

    net_cash = chain.pnl_chain + chain.commissions + chain.fees
    txn.postings.extend(
        [
            _CreatePosting(accounts, "gain_loss", -chain.pnl_chain),
            _CreatePosting(accounts, "commissions", -chain.commissions),
            _CreatePosting(accounts, "fees", -chain.fees),
            _CreatePosting(accounts, "cash", net_cash),
        ]
    )
    printer.print_entry(txn, file=outfile)


def RenderTransactionsToBeancount(
    chain: Record, txns: List[Record], filename: str, outfile: TextIO
):
    """Render a list of transactions to Beancount syntax."""

    # TODO(blais): add real account somewhere, a chain may cross accounts.

    # TODO(blais): Make this configurable.
    accounts = {
        "assets": "Assets:Trading:Main:{underlying}",
        "gain_loss": "Income:US:Trade:PnL",
        "commissions": "Expenses:Financial:Commissions",
        "fees": "Expenses:Financial:Fees",
        "cash": "Assets:Trading:Cash",
    }

    for txn in txns.records():
        narration = f"{txn.instruction} of {txn.symbol}"
        meta = data.new_metadata(filename, 1)

        print(txns)
        # for field in (
        #     "chain_id",
        #     "underlyings",
        #     "mindate",
        #     "maxdate",
        #     "group",
        #     "strategy",
        #     # "term",
        # ):
        #     meta[field] = getattr(chain, field)
        meta["chain"] = chain.chain_id
        beantxn = data.Transaction(
            meta,
            txn.datetime.date(),
            flags.FLAG_OKAY,
            "",
            narration,
            set(),
            {},
            [],
        )

        # net_cash = txn.cost + txn.commissions + txn.fees
        sign = -1 if txn.instruction == "SELL" else +1
        units = Amount(sign * txn.quantity, txn.symbol)
        adj_cost  = txn.cost + txn.commissions + txn.fees

        if txn.effect == "OPENING":
            cost = CostSpec(
                txn.price, None, CURRENCY, txn.datetime.date(), None, False
            )
            assets = data.Posting(
                accounts["assets"].format(underlying=txn.underlying),
                units,
                cost,
                None,
                None,
                None,
            )
            beantxn.postings.extend(
                [
                    assets,
                    _CreatePosting(accounts, "commissions", -txn.commissions),
                    _CreatePosting(accounts, "fees", -txn.fees),
                    _CreatePosting(accounts, "cash", adj_cost),
                ]
            )
        elif txn.effect == "CLOSING":
            cost = CostSpec(None, None, CURRENCY, None, None, False)
            price = Amount(txn.price, CURRENCY)
            assets = data.Posting(
                accounts["assets"].format(underlying=txn.underlying),
                units,
                cost,
                price,
                None,
                None,
            )
            beantxn.postings.extend(
                [
                    assets,
                    _CreatePosting(accounts, "commissions", -txn.commissions),
                    _CreatePosting(accounts, "fees", -txn.fees),
                    _CreatePosting(accounts, "cash", adj_cost),
                    _CreatePosting(accounts, "gain_loss", -chain.pnl_chain),
                ]
            )

        else:
            raise ValueError(f"Invalid effect: {txn.effect}")

        printer.print_entry(beantxn, file=outfile)
