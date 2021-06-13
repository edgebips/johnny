"""Helper functions to parser and deal with configuration matters."""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from typing import Mapping

from johnny.base.config_pb2 import Config
from johnny.base import config_pb2
from johnny.base.etl import petl, Table

from google.protobuf import text_format


def ParseFile(filename: str) -> config_pb2.Config:
    """Parse a text-formatted proto configuration file."""
    with open(filename) as infile:
        return text_format.Parse(infile.read(), config_pb2.Config())


def MapAccount(config: config_pb2.Config, table: Table, field: str) -> Table:
    """Convert a table's raw account id to an alias from the configuration."""
    accounts_map = {acc.number: acc.nickname
                    for acc in config.accounts}
    return petl.convert(table, field, accounts_map)


def GetExplicitChains(config: Config) -> Mapping[str, str]:
    """Extract a mapping of transaction-id to some unique chain-id."""
    return {tid: "chain-{}".format(chain.chain_id)
            for chain in config.chain
            for tid in chain.transaction_ids}
