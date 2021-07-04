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
             .convert('account-number', lambda _: '<HIDDEN>')
             .convert('id', lambda _: '12345678')
             )
    print(table.lookallstr())





if __name__ == '__main__':
    main()
