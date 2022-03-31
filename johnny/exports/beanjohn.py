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
from johnny.base.config import Config, BeancountAccounts
from johnny.base import instrument
from johnny.base.etl import petl, Table, Record, WrapRecords

from beancount.core import data
from beancount.core import flags
from beancount.core.amount import Amount
from beancount.core.account import Account
from beancount.core.number import MISSING
from beancount.core.position import CostSpec
from beancount.parser import printer


CURRENCY = "USD"
ZERO = Decimal(0)
Q2 = Decimal("0.01")


def _GetAccounts(config: Config, account: str) -> BeancountAccounts:
    for accobj in config.input.accounts:
        if accobj.nickname == account:
            return accobj.beancount


def _CreatePosting(account: Account, number: Decimal) -> Optional[data.Posting]:
    if isinstance(number, Decimal):
        if number == ZERO:
            return None
        number = number.quantize(Q2)
    return data.Posting(account, Amount(number, CURRENCY), None, None, None, None)


def RenderChainToBeancount(
    config: Config, chain: Record, filename: str, outfile: TextIO
):
    """Render a chain to Beancount syntax."""
    # print(WrapRecords([chain]))

    baccounts = _GetAccounts(config, chain.account)

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
        filter(
            None,
            [
                _CreatePosting(baccounts.account_pnl, -chain.pnl_chain),
                _CreatePosting(baccounts.account_commissions, -chain.commissions),
                _CreatePosting(baccounts.account_fees, -chain.fees),
                _CreatePosting(baccounts.account_cash, net_cash),
            ],
        )
    )
    printer.print_entry(txn, file=outfile)


def RenderTransactionsToBeancount(
    config: Config, chain: Record, txns: List[Record], filename: str, outfile: TextIO
):
    """Render a list of transactions to Beancount syntax."""

    baccounts = _GetAccounts(config, chain.account)

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

        links = {
            "jtxn-{}".format(txn.transaction_id) if txn.transaction_id else None,
            "jorder-{}".format(txn.order_id.lstrip("&")) if txn.order_id else None,
            "jmatch-{}".format(txn.match_id.lstrip("&") if txn.match_id else None),
        }
        links.discard(None)

        meta["chain"] = chain.chain_id
        beantxn = data.Transaction(
            meta,
            txn.datetime.date(),
            flags.FLAG_OKAY,
            "",
            narration,
            set(),
            links,
            [],
        )

        # net_cash = txn.cost + txn.commissions + txn.fees
        sign = -1 if txn.instruction == "SELL" else +1
        units = Amount(sign * txn.quantity, txn.symbol)
        adj_cost = txn.cost + txn.commissions + txn.fees

        if txn.effect == "OPENING":
            cost = CostSpec(txn.price, None, CURRENCY, txn.datetime.date(), None, False)
            assets = data.Posting(
                baccounts.account_assets.format(underlying=txn.underlying),
                units,
                cost,
                None,
                None,
                None,
            )
            beantxn.postings.extend(
                filter(
                    None,
                    [
                        assets,
                        _CreatePosting(baccounts.account_commissions, -txn.commissions),
                        _CreatePosting(baccounts.account_fees, -txn.fees),
                        _CreatePosting(baccounts.account_cash, adj_cost),
                    ],
                )
            )
        elif txn.effect == "CLOSING":
            cost = CostSpec(None, None, CURRENCY, None, None, False)
            price = Amount(txn.price, CURRENCY)
            assets = data.Posting(
                baccounts.account_assets.format(underlying=txn.underlying),
                units,
                cost,
                price,
                None,
                None,
            )
            beantxn.postings.extend(
                filter(
                    None,
                    [
                        assets,
                        _CreatePosting(baccounts.account_commissions, -txn.commissions),
                        _CreatePosting(baccounts.account_fees, -txn.fees),
                        _CreatePosting(baccounts.account_cash, adj_cost),
                        _CreatePosting(baccounts.account_pnl, MISSING),
                    ],
                )
            )

        else:
            raise ValueError(f"Invalid effect: {txn.effect}")

        printer.print_entry(beantxn, file=outfile)
