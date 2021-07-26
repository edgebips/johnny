"""Helper functions to parser and deal with configuration matters."""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import os
from typing import Mapping, Optional, Tuple

# pylint: disable=unused-import
from johnny.base.config_pb2 import Config, Chain, ChainStatus, Account
from johnny.base.config_pb2 import FutOptMonthMapping
from johnny.base import config_pb2
from johnny.base.etl import petl, Table

from google.protobuf import text_format


def ToText(message) -> str:
    """Convert a proto message to pretty-printed text."""
    return text_format.MessageToString(message)


def GetConfigFilenameWithDefaults(filename: Optional[str]) -> str:
    """Get the configuration filename, handling defaults."""
    if not filename:
        filename = os.getenv("JOHNNY_CONFIG")
    if not filename:
        raise RuntimeError("No value for JOHNNY_CONFIG is provided.")
    return filename


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


def GetExplicitChains(config: Config) -> Mapping[str, str]:
    """Extract a mapping of transaction-id to some unique chain-id."""
    transactions_map = {tid: chain.chain_id
                        for chain in config.chains
                        for tid in chain.transaction_ids}
    return transactions_map
