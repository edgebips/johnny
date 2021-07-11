#!/usr/bin/env python3
"""Search a TW db (shelve with original API Json) for an order by order-id and print it.
"""

import pprint
import shelve

import click

@click.command()
@click.argument('database', type=click.Path(resolve_path=True, exists=True))
@click.argument('order_id', type=int)
def main(database: str, order_id: int):
    db = shelve.open(database, 'r')

    # Unfortunately there is no index so we do a linear scan for all matching entries.
    for value in db.values():
        value_order_id = value.get('order-id', None)
        if value_order_id == order_id:
            pprint.pprint(value)
            print()


if __name__ == '__main__':
    main()
