"""Web application for all the files.
"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from functools import partial
from typing import Optional, Tuple
import datetime
import logging

import seaborn as sns
sns.set()

from johnny.base import config as configlib
from johnny.base import chains as chainslib
from johnny.base.etl import Table, Record
Chain = chainslib.Chain


ACTIONS = {
    'Closing': 1,
    'Daytrade': 2,
    'Adjusting': 3,
    'Opening': 4,
    'Opening_Earnings': 5,
    'Closing_Earnings': 6,
    'Adjusting_Earnings': 7,
}

def get_chains_at_date(transactions: Table,
                       chains: Table,
                       chains_map: dict[str, Chain],
                       date: datetime.date) -> Tuple[Table, Table]:
    """Filter and identify chains active on a given date."""

    # A set of chain ids with transactions on the date.
    # This is used to figure out if an adjustment took place on a chain.
    traded_chains = set(
        transactions
        .selectne('rowtype', 'Mark')
        .select(lambda r: r.datetime.date() == date)
        .values('chain_id'))

    # Infer the action of the chain based on the status and date extents. Note
    # that this may return None, in which case the chain is to be excluded.
    def infer_action(r: Record) -> Optional[str]:
        if date == r.maxdate and r.status in {'FINAL', 'CLOSED'}:
            if date == r.mindate:
                action = 'Daytrade'
            # Break out 'Earnings' to its own table.
            else:
                action = 'Closing'
        elif date == r.mindate:
            action = 'Opening'
        elif r.chain_id in traded_chains:
            action = 'Adjusting'
        else:
            return None
        if r.group == 'Earnings':
            action += '_Earnings'
        return action

    # Join in the comments from the chain.
    def get_comment(r: Record) -> str:
        chain = chains_map.get(r.chain_id)
        if not chain:
            logging.error(f"Missing chain {r.chain_id}")
        return chain.comment if chain else ''

    # Filter commissions & fees per day.
    commfees = (transactions
                .select(lambda r: r.datetime.date() == date)
                .aggregate('chain_id', {'commissions': ('commissions', sum),
                                        'fees': ('fees', sum)}))

    # Process the chains.
    chains = (chains
              .select(lambda r: r.mindate <= date <= r.maxdate)
              .addfield('action', infer_action, index=0)
              .selecttrue('action')
              .addfield('k', lambda r: ACTIONS.get(r.action), index=0)
              .sort(['k', 'chain_id'])
              .cutout('k')
              .addfield('comment', get_comment)
              .leftjoin(commfees, key='chain_id', rprefix='day_')
              .cutout('account', 'init_legs',
                      'net_liq', 'net_win', 'net_loss',
                      'commissions', 'fees'))

    return chains


def get_summary(chains: Table) -> Table:
    """Filter and identify chains active on a given date."""

    # Calculate a sensible summary table. Note that we clear the adjusting and
    # opening P/L, as they are not relevant to the day's action.
    agg = {
        'pnl_chain': ('pnl_chain', sum),
        'credits': ('init', sum),
        'day_commissions': ('day_commissions', sum),
        'day_fees': ('day_fees', sum),
    }

    pnl_actions = {'Closing', 'Closing_Earnings', 'Daytrade'}
    def clean_pnl(value, r: Record) -> str:
        return value if r.action in pnl_actions else 0

    init_actions = {'Opening'}
    def clean_init(value, r: Record) -> str:
        return value if r.action in init_actions else 0

    summary = (chains
               .aggregate('action', agg)
               .convert('pnl_chain', clean_pnl, pass_row=True)
               .convert('credits', clean_init, pass_row=True)
               .addfield('k', lambda r: ACTIONS.get(r.action))
               .sort('k')
               .cutout('k'))

    return summary
