"""Helper functions to parser and deal with configuration matters."""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from os import path
from typing import Mapping, Optional, Tuple
import copy
import logging
import os
import re

# pylint: disable=unused-import
from johnny.base.config_pb2 import Config, Account
from johnny.base.chains_pb2 import Chains, Chain, ChainStatus
from johnny.base.config_pb2 import FutOptMonthMapping
from johnny.base.config_pb2 import InstrumentType
from johnny.base.config_pb2 import BeancountAccounts
from johnny.base.config_pb2 import DatedValue
from johnny.base.etl import petl, Table

from google.protobuf import text_format


LogType = Account.LogType


def ToText(message) -> str:
    """Convert a proto message to pretty-printed text."""
    string = text_format.MessageToString(message)
    return re.sub("^}$", "}\n", string, flags=re.MULTILINE)


def GetConfigFilenameWithDefaults(filename: Optional[str]) -> str:
    """Get the configuration filename, handling defaults."""
    if not filename:
        filename = os.getenv("JOHNNY_CONFIG")
    if not filename:
        raise RuntimeError("No value for JOHNNY_CONFIG is provided.")
    return filename


def PerformReplacements(config: Config) -> Config:
    """Replace environment variables in the config filenames."""
    config = copy.deepcopy(config)

    for account in config.input.accounts:
        account.source = path.expandvars(account.source)
        account.initial = path.expandvars(account.initial)
    config.input.chains_db = path.expandvars(config.input.chains_db)

    config.output.chains_db = path.expandvars(config.output.chains_db)
    config.output.transactions_pickle = path.expandvars(
        config.output.transactions_pickle
    )
    config.output.transactions_csv = path.expandvars(config.output.transactions_csv)
    config.output.transactions_parquet = path.expandvars(
        config.output.transactions_parquet
    )
    config.output.chains_pickle = path.expandvars(config.output.chains_pickle)
    config.output.chains_csv = path.expandvars(config.output.chains_csv)
    config.output.chains_parquet = path.expandvars(config.output.chains_parquet)

    return config


def ParseFile(filename: str) -> Config:
    """Parse a text-formatted proto configuration file."""
    with open(filename) as infile:
        config = text_format.Parse(infile.read(), Config())
    config = PerformReplacements(config)
    Validate(config)
    return config


class ConfigError(ValueError):
    """Error raised for invalid configurations."""


def Validate(config: Config):
    """Validate the configuration."""

    # Check the account nicknames are unique.
    nicknames = [a.nickname for a in config.input.accounts]
    if len(nicknames) != len(set(nicknames)):
        raise ConfigError("Nicknames are not unique")

    # Ensure required fields are set.
    for a in config.input.accounts:
        if not a.logtype:
            raise ConfigError("Log type is not set")


def ReadChains(filename: str) -> Chains:
    """Parse a text-formatted chains poor man's db file."""
    chains = Chains()
    if path.exists(filename):
        with open(filename) as infile:
            text_format.Parse(infile.read(), chains)
    return chains
