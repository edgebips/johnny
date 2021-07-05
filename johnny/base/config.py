"""Helper functions to parser and deal with configuration matters."""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from typing import Mapping, Tuple

from johnny.base.config_pb2 import Config
from johnny.base.config_pb2 import Chain
from johnny.base.config_pb2 import Price
from johnny.base import config_pb2
from johnny.base.etl import petl, Table

from google.protobuf import text_format


def ToText(message) -> str:
    """Convert a proto message to pretty-printed text."""
    return text_format.MessageToString(message)


def ParseFile(filename: str) -> config_pb2.Config:
    """Parse a text-formatted proto configuration file."""
    with open(filename) as infile:
        return text_format.Parse(infile.read(), config_pb2.Config())


def MapAccount(config: config_pb2.Config, table: Table, field: str) -> Table:
    """Convert a table's raw account id to an alias from the configuration."""
    accounts_map = {acc.number: acc.nickname
                    for acc in config.accounts}
    return petl.convert(table, field, accounts_map)


def GetExplicitChains(config: Config) -> Tuple[Mapping[str, str], Mapping[str, str]]:
    """Extract a mapping of transaction-id to some unique chain-id."""
    transactions_map = {tid: chain.chain_id
                        for chain in config.chains
                        for tid in chain.transaction_ids}
    orders_map = {oid: chain.chain_id
                  for chain in config.chains
                  for oid in chain.order_ids}
    return (transactions_map, orders_map)
