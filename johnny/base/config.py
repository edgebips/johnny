"""Helper functions to parser and deal with configuration matters."""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from typing import Mapping, Tuple

from johnny.base.config_pb2 import Config, Chain, Account, FutOptMonthMapping
from johnny.base import config_pb2
from johnny.base.etl import petl, Table

from google.protobuf import text_format


def ToText(message) -> str:
    """Convert a proto message to pretty-printed text."""
    return text_format.MessageToString(message)


def ParseFile(filename: str) -> config_pb2.Config:
    """Parse a text-formatted proto configuration file."""
    with open(filename) as infile:
        config = text_format.Parse(infile.read(), config_pb2.Config())
    Validate(config)
    return config


class ConfigError(ValueError):
    """Error raised for invalid configurations."""


def Validate(config: config_pb2.Config):
    """Validate the configuration."""

    # Check the account nicknames are unique.
    nicknames = [a.nickname for a in config.input.accounts]
    if len(nicknames) != len(set(nicknames)):
        raise ConfigError("Nicknames are not unique")

    # Ensure required fields are set.
    for a in config.input.accounts:
        if not a.HasField('logtype'):
            raise ConfigError("Log type is not set")


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
