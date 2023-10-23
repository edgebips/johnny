#!/usr/bin/env python3
"""Convert our CSV file formats to Parquet."""

import argparse
import petl

from johnny.base import config as configlib
from johnny.base import transactions as txnlib


def main():
    parser = argparse.ArgumentParser(description=__doc__.strip())
    args = parser.parse_args()

    filename = configlib.GetConfigFilenameWithDefaults(None)
    config = configlib.ParseFile(filename)
    transactions = petl.frompickle(config.output.transactions_pickle)

    txnlib.to_parquet(transactions, "/tmp/transactions.parquet")


if __name__ == "__main__":
    main()
