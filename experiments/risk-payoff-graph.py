#!/usr/bin/env python3
"""Fetch a list of chains to be annotated, run an editor with a list, and then
integrate that data in the chains.
"""
__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import logging
import tempfile
import subprocess
import os
import datetime
import os
import sys
from decimal import Decimal
from typing import List, Optional

import click

from johnny.base import discovery
from johnny.base import mark
from johnny.base import chains_pb2
from johnny.base import chains as chainslib
from johnny.base import config as configlib
from johnny.base import instrument
from johnny.base import recap as recaplib
from johnny.base.etl import petl, Table

from goodkids import session as sesslib


def get_market_metrics(session: sesslib.Session, symbols: list[str]) -> Table:
    """Get the market metrics for the full list of symbols."""
    resp = session.relget(f'/market-metrics', params={'symbols': ','.join(symbols)})
    items = resp.json()['data']['items']
    pp(items)
    raise SystemExit
    header = ['symbol', 'beta', 'corr-spy-3month']
    rows = [header]
    for item in items:
        rows.append([item['symbol'],
                     item.get('beta', None),
                     item.get('corr-spy-3month', None)])
    return (petl.wrap(rows)
            .convert('beta', lambda v: Decimal(v) if v else None)
            .convert('corr-spy-3month', lambda v: Decimal(v) if v else None))


 # get near 30-day IV, or front one.
 # 'option-expiration-implied-volatilities': [{'expiration-date': '2021-08-20',
 #                                             'option-chain-type': 'Standard',
 #                                             'settlement-type': 'PM'},
 #                                            {'expiration-date': '2021-09-17',
 #                                             'implied-volatility': '0.405535342',
 #                                             'option-chain-type': 'Standard',
 #                                             'settlement-type': 'PM'},
 #                                            {'expiration-date': '2021-10-15',
 #                                             'implied-volatility': '0.354183901',
 #                                             'option-chain-type': 'Standard',
 #                                             'settlement-type': 'PM'},
 #                                            {'expiration-date': '2021-12-17',
 #                                             'implied-volatility': '0.326761689',
 #                                             'option-chain-type': 'Standard',
 #                                             'settlement-type': 'PM'},
 #                                            {'expiration-date': '2022-01-21',
 #                                             'implied-volatility': '0.307666698',
 #                                             'option-chain-type': 'Standard',
 #                                             'settlement-type': 'PM'},
 #                                            {'expiration-date': '2022-03-18',
 #                                             'implied-volatility': '0.316100506',
 #                                             'option-chain-type': 'Standard',
 #                                             'settlement-type': 'PM'},
 #                                            {'expiration-date': '2022-12-16',
 #                                             'implied-volatility': '0.29648466',
 #                                             'option-chain-type': 'Standard',
 #                                             'settlement-type': 'PM'}],



def get_product(underlying: str) -> str:
    return underlying[:-3] if underlying.startswith('/') else underlying


@click.command()
@click.option('--config', '-c', type=click.Path(exists=True),
              help="Configuration filename. Default to $JOHNNY_CONFIG")
@click.option('--username', '-u', help="Tastyworks username.")
@click.option('--password') # Tastyworks password.
def main(config: Optional[str], username: Optional[str], password: Optional[str]):
    # Load the database.
    filename = configlib.GetConfigFilenameWithDefaults(config)
    config = configlib.ParseFile(filename)
    transactions = petl.frompickle(config.output.transactions)

    positions = (transactions
                 .selecteq('account', 'x18')
                 .applyfn(instrument.Expand, 'symbol')
                 .selecteq('rowtype', 'Mark')
                 .addfield('product', lambda r: get_product(r.underlying)))

    #print(positions.lookallstr())
    symbols = set(positions.values('product'))
    session = sesslib.get_session(username, password)
    metrics = get_market_metrics(session, symbols)

    table = (petl.leftjoin(positions, metrics, lkey='product', rkey='symbol'))

    print(table.lookallstr())




if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)-8s: %(message)s')
    main(obj={})
