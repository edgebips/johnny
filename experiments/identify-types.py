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


@click.command()
@click.argument('database', type=click.Path(resolve_path=True, exists=True))
def main(database: str):
    db = shelve.open(database, 'r')

    table = (petl.fromdicts(value for key, value in db.items() if not key.startswith('__'))

             #.cut('underlying-symbol', 'exchange')
             .cut('transaction-type', 'transaction-sub-type', 'action')
             .distinct()
             )
    print(table.lookallstr())


if __name__ == '__main__':
    main()
