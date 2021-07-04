#!/usr/bin/env python3
"""Identify unique transaction types tripled from a Tastyworks database.
"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import shelve
from decimal import Decimal

import click
import petl
petl.config.look_style = 'minimal'
petl.compat.numeric_types = petl.compat.numeric_types + (Decimal,)
petl.config.failonerror = True


KNOWN = {
    # From blais@
    ('Money Movement', 'Balance Adjustment', None),
    ('Money Movement', 'Credit Interest', None),
    ('Money Movement', 'Mark to Market', None),
    ('Money Movement', 'Transfer', None),
    ('Money Movement', 'Withdrawal', None),
    ('Receive Deliver', 'Expiration', None),
    ('Receive Deliver', 'Symbol Change', 'Buy to Close'),
    ('Receive Deliver', 'Symbol Change', 'Sell to Open'),
    ('Trade', 'Buy', 'Buy'),
    ('Trade', 'Buy to Close', 'Buy to Close'),
    ('Trade', 'Buy to Open', 'Buy to Open'),
    ('Trade', 'Sell', 'Sell'),
    ('Trade', 'Sell to Close', 'Sell to Close'),
    ('Trade', 'Sell to Open', 'Sell to Open'),

    # From hg@
    # ('Money Movement', 'Deposit', None),
    # ('Money Movement', 'Dividend', None),
    # ('Money Movement', 'Fee', None),
    # ('Receive Deliver', 'ACAT', 'Buy to Open'),
    # ('Receive Deliver', 'ACAT', 'Sell to Open'),
    # ('Receive Deliver', 'Assignment', None),
    # ('Receive Deliver', 'Cash Settled Assignment', None),
    # ('Receive Deliver', 'Cash Settled Exercise', None),
    # ('Receive Deliver', 'Exercise', None),
    # ('Receive Deliver', 'Expiration', None),
}

@click.command()
@click.argument('database', type=click.Path(resolve_path=True, exists=True))
def main(database: str):
    db = shelve.open(database, 'r')

    def Filter(rec: petl.Record) -> bool:
        return (rec['transaction-type'],
                rec['transaction-sub-type'],
                rec['action']) not in KNOWN

    table = (petl.fromdicts(value for key, value in db.items() if not key.startswith('__'))
             #.cut('transaction-type', 'transaction-sub-type', 'action')
             #.distinct()
             #.complement(known)

             .select(Filter)

             # Anonymize content.
             .convert('account-number', lambda _: '<HIDDEN>')
             .convert('id', lambda _: '12345678')

             # Make all numbers opaque.
             .convert('net-value', lambda _: '123.45')
             .convert('value', lambda _: '123.45')
             .convert('commission', lambda _: '123.45')
             .convert('regulatory-fees', lambda _: '123.45')
             .convert('clearing-fees', lambda _: '123.45')
             .convert('proprietary-index-option-fees', lambda _: '123.45')
             .convert('quantity', lambda _: '1.0')
             )
    print(table.lookallstr())


if __name__ == '__main__':
    main()
