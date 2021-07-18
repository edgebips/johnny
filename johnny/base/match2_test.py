"""Tests for matching code."""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import datetime
from decimal import Decimal
import unittest
from unittest import mock

from johnny.base import match2
from johnny.base import instrument
from johnny.base.etl import petl
from johnny.base.etl import petl, Table, AssertTableEqual


ZERO = Decimal(0)


# pylint: disable=line-too-long
class TestMatch2(unittest.TestCase):

    @mock.patch.object(match2, '_GetExpirationDate', return_value=datetime.date(2021, 7, 1))
    def test_match_simple(self, _):

        header = ('account', 'datetime', 'transaction_id', 'rowtype', 'symbol', 'instruction', 'effect', 'quantity', 'cost', 'description', 'price', 'commissions', 'fees')
        transactions = petl.wrap([
            header,
            ('A', datetime.datetime(2021, 6, 1, 12, 10, 0), '00000001', 'Trade', 'AAPL_210625_C150', 'BUY', 'OPENING', Decimal('2'), Decimal('123.00'), 'Desc', ZERO, ZERO, ZERO),
        ])
        expected_output = petl.wrap([
            header,
            ('A', datetime.datetime(2021, 6, 1, 12, 10, 0), '00000001', 'Trade', 'AAPL_210625_C150', 'BUY', 'OPENING', Decimal('2'), Decimal('123.00'), 'Desc', ZERO, ZERO, ZERO, '&c5529d3c'),
            ('A', datetime.datetime(2021, 6, 26, 0, 0, 0), None, 'Expire', 'AAPL_210625_C150', 'SELL', 'CLOSING', Decimal('2'), Decimal(0), 'Synthetic expiration for AAPL_210625_C150', ZERO, ZERO, ZERO, '&c5529d3c'),
        ])
        AssertTableEqual(expected_output, match2.Process(transactions))


if __name__ == '__main__':
    unittest.main()
