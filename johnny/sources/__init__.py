"""API for all sources.
"""

__copyright__ = "Copyright (C) 2023  Martin Blais"
__license__ = "GNU GPLv2"


from johnny.base.etl import petl

from typing import Any
import abc


class Source(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def ImportTransactions(config: Any) -> petl.Table:
        """Import the list of transactions from the source-specific configuration.

        See `transactions.proto` for the schema of the returned table.
        """

    @abc.abstractmethod
    def ImportNonTrades(config: Any) -> petl.Table:
        """Import the list of non-trade rows from the source-specific configuration.

        See `nontrades.proto` for the schema of the returned table.
        """

    @abc.abstractmethod
    def ImportPositions(config: Any) -> petl.Table:
        """Import the list of positions/marks from the source-specific configuration.

        See `positions.proto` for the schema of the returned table.
        """
