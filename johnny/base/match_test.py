"""Tests for matching code."""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import datetime
from decimal import Decimal
import unittest
from unittest import mock

from johnny.base import match
from johnny.base.etl import petl
from johnny.base.etl import petl, AssertTableEqual


ZERO = Decimal(0)


# pylint: disable=line-too-long
class TestMatch2(unittest.TestCase):
    @mock.patch.object(
        match, "_GetMarkTime", return_value=datetime.datetime(2021, 7, 1, 0, 0, 0)
    )
    def test_add_missing_expirations(self, _):
        header = (
            "account",
            "datetime",
            "transaction_id",
            "order_id",
            "rowtype",
            "symbol",
            "instruction",
            "effect",
            "quantity",
            "cost",
            "description",
            "price",
            "commissions",
            "fees",
        )
        transactions = petl.wrap(
            [
                header,
                (
                    "A",
                    datetime.datetime(2021, 6, 1, 12, 10, 0),
                    "00000001",
                    None,
                    txnlib.Type.Trade,
                    "AAPL_210625_C150",
                    "BUY",
                    "OPENING",
                    Decimal("2"),
                    Decimal("123.00"),
                    "Desc",
                    ZERO,
                    ZERO,
                    ZERO,
                ),
            ]
        )
        expected_output = petl.wrap(
            [
                header,
                (
                    "A",
                    datetime.datetime(2021, 6, 1, 12, 10, 0),
                    "00000001",
                    None,
                    txnlib.Type.Trade,
                    "AAPL_210625_C150",
                    "BUY",
                    "OPENING",
                    Decimal("2"),
                    Decimal("123.00"),
                    "Desc",
                    ZERO,
                    ZERO,
                    ZERO,
                    "&00000001",
                ),
                (
                    "A",
                    datetime.datetime(2021, 6, 26, 0, 0, 0),
                    "62535baf0310",
                    "e60a324c",
                    txnlib.Type.Expire,
                    "AAPL_210625_C150",
                    "SELL",
                    "CLOSING",
                    Decimal("2"),
                    Decimal(0),
                    "Synthetic expiration for AAPL_210625_C150",
                    ZERO,
                    ZERO,
                    ZERO,
                    "&00000001",
                ),
            ]
        )
        AssertTableEqual(expected_output, match.Process(transactions))

    @mock.patch.object(
        match, "_GetMarkTime", return_value=datetime.datetime(2021, 7, 10, 0, 0, 0)
    )
    def test_add_mark_transactions(self, _):
        header = (
            "account",
            "datetime",
            "transaction_id",
            "order_id",
            "rowtype",
            "symbol",
            "instruction",
            "effect",
            "quantity",
            "cost",
            "description",
            "price",
            "commissions",
            "fees",
        )
        transactions = petl.wrap(
            [
                header,
                (
                    "A",
                    datetime.datetime(2021, 7, 1, 12, 10, 0),
                    "00000001",
                    None,
                    txnlib.Type.Trade,
                    "AAPL_210925_C150",
                    "BUY",
                    "OPENING",
                    Decimal("2"),
                    Decimal("123.00"),
                    "Desc",
                    ZERO,
                    ZERO,
                    ZERO,
                ),
            ]
        )
        expected_output = petl.wrap(
            [
                header,
                (
                    "A",
                    datetime.datetime(2021, 7, 1, 12, 10, 0),
                    "00000001",
                    None,
                    txnlib.Type.Trade,
                    "AAPL_210925_C150",
                    "BUY",
                    "OPENING",
                    Decimal("2"),
                    Decimal("123.00"),
                    "Desc",
                    ZERO,
                    ZERO,
                    ZERO,
                    "&00000001",
                ),
                (
                    "A",
                    datetime.datetime(2021, 7, 10, 0, 0, 0),
                    "mark-d7183c",
                    None,
                    txnlib.Type.Mark,
                    "AAPL_210925_C150",
                    "SELL",
                    "CLOSING",
                    Decimal("2"),
                    ZERO,
                    "Mark for AAPL_210925_C150",
                    ZERO,
                    ZERO,
                    ZERO,
                    "&00000001",
                ),
            ]
        )
        print(expected_output.lookallstr())
        print(match.Process(transactions).lookallstr())
        AssertTableEqual(expected_output, match.Process(transactions))


if __name__ == "__main__":
    unittest.main()
