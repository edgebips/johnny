"""Tests for matching code."""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import datetime
from decimal import Decimal
import unittest

from johnny.base import match
from johnny.base import instrument
from johnny.base.etl import petl


# pylint: disable=line-too-long
class TestMatch(unittest.TestCase):

    def test_match_simple(self):
        table = petl.wrap([
            ('account', 'transaction_id', 'rowtype', 'symbol', 'instruction', 'effect', 'quantity', 'price'),
            ('x1234', '^141438597.1', 'Trade', '/GEZ21', 'SELL', '?', Decimal('2'), Decimal('99.72')),
            ('x1234', '^142574538.1', 'Trade', '/GEZ21', 'SELL', '?', Decimal('1'), Decimal('99.735')),
            ('x1234', '^142574538.2', 'Trade', '/GEZ21', 'SELL', '?', Decimal('2'), Decimal('99.735')),
            ('x1234', '^151675620.1', 'Trade', '/GEZ21', 'BUY', '?', Decimal('5'), Decimal('99.825')),
            ('x1234', '^152683832.1', 'Trade', '/VXM22', 'BUY', '?', Decimal('3'), Decimal('19.2')),
            ('x1234', '^152683833.1', 'Trade', '/VXN22', 'SELL', '?', Decimal('3'), Decimal('20.85')),
            ('x1234', '^152737267.1', 'Trade', '/ZTU21', 'SELL', '?', Decimal('2'), Decimal('110.35546875')),
            ('x1234', '^153075747.1', 'Trade', '/VXM22', 'SELL', '?', Decimal('3'), Decimal('19.05')),
            ('x1234', '^153075753.1', 'Trade', '/VXN22', 'BUY', '?', Decimal('1'), Decimal('21.15')),
            ('x1234', '^153075753.2', 'Trade', '/VXN22', 'BUY', '?', Decimal('1'), Decimal('21.15')),
            ('x1234', '^153075753.3', 'Trade', '/VXN22', 'BUY', '?', Decimal('1'), Decimal('21.15')),
        ])
        table = (table
                 .applyfn(instrument.Expand, 'symbol')
                 .applyfn(match.Match, closing_time=datetime.datetime(2021, 6, 1, 0, 0, 0))
                 .applyfn(instrument.Shrink))

        expected = petl.wrap([
            ('account', 'transaction_id', 'rowtype', 'symbol', 'instruction', 'effect', 'quantity', 'price', 'datetime', 'cost', 'description', 'commissions', 'fees', 'match_id'),
            ('x1234', '^141438597.1', 'Trade', '/GEZ21', 'SELL', 'OPENING', Decimal('2'), Decimal('99.72'), None, None, None, None, None, '&e2542452'),
            ('x1234', '^142574538.1', 'Trade', '/GEZ21', 'SELL', 'OPENING', Decimal('1'), Decimal('99.735'), None, None, None, None, None, '&e2542452'),
            ('x1234', '^142574538.2', 'Trade', '/GEZ21', 'SELL', 'OPENING', Decimal('2'), Decimal('99.735'), None, None, None, None, None, '&e2542452'),
            ('x1234', '^151675620.1', 'Trade', '/GEZ21', 'BUY', 'CLOSING', Decimal('5'), Decimal('99.825'), None, None, None, None, None, '&e2542452'),
            ('x1234', '^152683832.1', 'Trade', '/VXM22', 'BUY', 'OPENING', Decimal('3'), Decimal('19.2'), None, None, None, None, None, '&c6310c96'),
            ('x1234', '^152683833.1', 'Trade', '/VXN22', 'SELL', 'OPENING', Decimal('3'), Decimal('20.85'), None, None, None, None, None, '&08e4c104'),
            ('x1234', '^152737267.1', 'Trade', '/ZTU21', 'SELL', 'OPENING', Decimal('2'), Decimal('110.35546875'), None, None, None, None, None, '&94464231'),
            ('x1234', '^153075747.1', 'Trade', '/VXM22', 'SELL', 'CLOSING', Decimal('3'), Decimal('19.05'), None, None, None, None, None, '&c6310c96'),
            ('x1234', '^153075753.1', 'Trade', '/VXN22', 'BUY', 'CLOSING', Decimal('1'), Decimal('21.15'), None, None, None, None, None, '&08e4c104'),
            ('x1234', '^153075753.2', 'Trade', '/VXN22', 'BUY', 'CLOSING', Decimal('1'), Decimal('21.15'), None, None, None, None, None, '&08e4c104'),
            ('x1234', '^153075753.3', 'Trade', '/VXN22', 'BUY', 'CLOSING', Decimal('1'), Decimal('21.15'), None, None, None, None, None, '&08e4c104'),
            ('x1234', '^mark000001', 'Mark', '/ZTU21', 'BUY', 'CLOSING', Decimal('2'), Decimal('0'), datetime.datetime(2021, 6, 1, 0, 0, 0), Decimal('-441421.87500000'), 'Mark-to-market: -2 /ZTU21', Decimal('0'), Decimal('0'), '&94464231'),
        ])
        for r1, r2 in zip(table.records(), expected.records()):
            self.assertTupleEqual(r1, r2)

    def test_cross_flat_position(self):
        table = petl.wrap([
            ('account', 'transaction_id', 'rowtype', 'symbol', 'instruction', 'effect', 'quantity', 'price'),
            ('x1234', '^00001', 'Trade', '/NQH21', 'BUY', '?', Decimal('1'), Decimal('100.00')),
            ('x1234', '^00002', 'Trade', '/NQH21', 'SELL', '?', Decimal('2'), Decimal('101.00')),
            ('x1234', '^00003', 'Trade', '/NQH21', 'BUY', '?', Decimal('2'), Decimal('102.00')),
            ('x1234', '^00004', 'Trade', '/NQH21', 'SELL', '?', Decimal('1'), Decimal('103.00')),
        ])
        table = (table
                 .applyfn(instrument.Expand, 'symbol')
                 .applyfn(match.Match, closing_time=datetime.datetime(2021, 6, 1, 0, 0, 0))
                 .applyfn(instrument.Shrink))

        expected = petl.wrap([
            ('account', 'transaction_id', 'rowtype', 'symbol', 'instruction', 'effect', 'quantity', 'price', 'datetime', 'cost', 'description', 'commissions', 'fees', 'match_id'),
            ('x1234', '^00001', 'Trade', '/NQH21', 'BUY', 'OPENING', Decimal('1'), Decimal('100.00'), None, None, None, None, None, '&c3552ac7'),
            ('x1234', '^00002', 'Trade', '/NQH21', 'SELL', 'CLOSING', Decimal('2'), Decimal('101.00'), None, None, None, None, None, '&c3552ac7'),
            # This row really ought to have been split.
            ('x1234', '^00003', 'Trade', '/NQH21', 'BUY', 'CLOSING', Decimal('2'), Decimal('102.00'), None, None, None, None, None, '&c3552ac7'),
            ('x1234', '^00004', 'Trade', '/NQH21', 'SELL', 'CLOSING', Decimal('1'), Decimal('103.00'), None, None, None, None, None, '&c3552ac7'),
        ])
        for r1, r2 in zip(table.records(), expected.records()):
            self.assertTupleEqual(r1, r2)

if __name__ == '__main__':
    unittest.main()
