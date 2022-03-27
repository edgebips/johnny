#!/usr/bin/env python3
"""Add an 'Order #' column from a user's example file.

For this file:
https://raw.githubusercontent.com/Graeme22/tastyworks-cli/main/tests/data/transactions.csv
"""

import argparse
import logging

import click

from johnny.base.etl import petl


@click.command()
@click.argument("input_filename", type=click.Path(exists=True))
@click.argument("output_filename")
def main(input_filename: str, output_filename: str):
    def genid():
        i = 0
        while True:
            yield "{:09d}".format(i)
            i += 1

    newid = iter(genid())
    (
        petl.fromcsv(input_filename)
        .addfield("Order #", lambda x: next(newid))
        .tocsv(output_filename)
    )


if __name__ == "__main__":
    main()
