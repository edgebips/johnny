"""Tests for matching code."""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import datetime
from decimal import Decimal
import unittest

from johnny.base import match2
from johnny.base import instrument
from johnny.base.etl import petl


# pylint: disable=line-too-long
class TestMatch2(unittest.TestCase):

    def test_match_simple(self):

        table = petl.wrap([
            ('account', 'transaction_id', 'rowtype', 'symbol', 'instruction', 'effect', 'quantity', 'price'),
            ('A', '^01', 'Trade', '/GEZ21', 'SELL', '?', Decimal('2'), Decimal('99.72')),
            ('A', '^02', 'Trade', '/GEZ21', 'SELL', '?', Decimal('1'), Decimal('99.735')),
            ('A', '^03', 'Trade', '/GEZ21', 'SELL', '?', Decimal('2'), Decimal('99.735')),
            ('A', '^04', 'Trade', '/GEZ21', 'BUY', '?', Decimal('5'), Decimal('99.825')),
            ('A', '^05', 'Trade', '/VXM22', 'BUY', '?', Decimal('3'), Decimal('19.2')),
            ('A', '^06', 'Trade', '/VXN22', 'SELL', '?', Decimal('3'), Decimal('20.85')),
            ('A', '^07', 'Trade', '/ZTU21', 'SELL', '?', Decimal('2'), Decimal('110.35546875')),
            ('A', '^08', 'Trade', '/VXM22', 'SELL', '?', Decimal('3'), Decimal('19.05')),
            ('A', '^09', 'Trade', '/VXN22', 'BUY', '?', Decimal('1'), Decimal('21.15')),
            ('A', '^10', 'Trade', '/VXN22', 'BUY', '?', Decimal('1'), Decimal('21.15')),
            ('A', '^11', 'Trade', '/VXN22', 'BUY', '?', Decimal('1'), Decimal('21.15')),
        ])

        match2._CreateMatchMappings(

        # table = petl.wrap([
        #     ('account', 'transaction_id', 'rowtype', 'symbol', 'instruction', 'effect', 'quantity', 'price'),
        #     ('A', '^01', 'Trade', '/GEZ21', 'SELL', '?', Decimal('2'), Decimal('99.72')),
        #     ('A', '^02', 'Trade', '/GEZ21', 'SELL', '?', Decimal('1'), Decimal('99.735')),
        #     ('A', '^03', 'Trade', '/GEZ21', 'SELL', '?', Decimal('2'), Decimal('99.735')),
        #     ('A', '^04', 'Trade', '/GEZ21', 'BUY', '?', Decimal('5'), Decimal('99.825')),
        #     ('A', '^05', 'Trade', '/VXM22', 'BUY', '?', Decimal('3'), Decimal('19.2')),
        #     ('A', '^06', 'Trade', '/VXN22', 'SELL', '?', Decimal('3'), Decimal('20.85')),
        #     ('A', '^07', 'Trade', '/ZTU21', 'SELL', '?', Decimal('2'), Decimal('110.35546875')),
        #     ('A', '^08', 'Trade', '/VXM22', 'SELL', '?', Decimal('3'), Decimal('19.05')),
        #     ('A', '^09', 'Trade', '/VXN22', 'BUY', '?', Decimal('1'), Decimal('21.15')),
        #     ('A', '^10', 'Trade', '/VXN22', 'BUY', '?', Decimal('1'), Decimal('21.15')),
        #     ('A', '^11', 'Trade', '/VXN22', 'BUY', '?', Decimal('1'), Decimal('21.15')),
        # ])
        # table = (table
        #          .applyfn(instrument.Expand, 'symbol')
        #          .applyfn(match.Match, closing_time=datetime.datetime(2021, 6, 1, 0, 0, 0))
        #          .applyfn(instrument.Shrink))

        # expected = petl.wrap([
        #     ('account', 'transaction_id', 'rowtype', 'symbol', 'instruction', 'effect', 'quantity', 'price', 'datetime', 'cost', 'description', 'commissions', 'fees', 'match_id'),
        #     ('A', '^01', 'Trade', '/GEZ21', 'SELL', 'OPENING', Decimal('2'), Decimal('99.72'), None, None, None, None, None, '&e2542452'),
        #     ('A', '^02', 'Trade', '/GEZ21', 'SELL', 'OPENING', Decimal('1'), Decimal('99.735'), None, None, None, None, None, '&e2542452'),
        #     ('A', '^03', 'Trade', '/GEZ21', 'SELL', 'OPENING', Decimal('2'), Decimal('99.735'), None, None, None, None, None, '&e2542452'),
        #     ('A', '^04', 'Trade', '/GEZ21', 'BUY', 'CLOSING', Decimal('5'), Decimal('99.825'), None, None, None, None, None, '&e2542452'),
        #     ('A', '^05', 'Trade', '/VXM22', 'BUY', 'OPENING', Decimal('3'), Decimal('19.2'), None, None, None, None, None, '&c6310c96'),
        #     ('A', '^06', 'Trade', '/VXN22', 'SELL', 'OPENING', Decimal('3'), Decimal('20.85'), None, None, None, None, None, '&08e4c104'),
        #     ('A', '^07', 'Trade', '/ZTU21', 'SELL', 'OPENING', Decimal('2'), Decimal('110.35546875'), None, None, None, None, None, '&94464231'),
        #     ('A', '^08', 'Trade', '/VXM22', 'SELL', 'CLOSING', Decimal('3'), Decimal('19.05'), None, None, None, None, None, '&c6310c96'),
        #     ('A', '^09', 'Trade', '/VXN22', 'BUY', 'CLOSING', Decimal('1'), Decimal('21.15'), None, None, None, None, None, '&08e4c104'),
        #     ('A', '^10', 'Trade', '/VXN22', 'BUY', 'CLOSING', Decimal('1'), Decimal('21.15'), None, None, None, None, None, '&08e4c104'),
        #     ('A', '^11', 'Trade', '/VXN22', 'BUY', 'CLOSING', Decimal('1'), Decimal('21.15'), None, None, None, None, None, '&08e4c104'),
        #     ('A', '^mark000001', 'Mark', '/ZTU21', 'BUY', 'CLOSING', Decimal('2'), Decimal('0'), datetime.datetime(2021, 6, 1, 0, 0, 0), Decimal('-441421.87500000'), 'Mark-to-market: -2 /ZTU21', Decimal('0'), Decimal('0'), '&94464231'),
        # ])
        # for r1, r2 in zip(table.records(), expected.records()):
        #     self.assertTupleEqual(r1, r2)


if __name__ == '__main__':
    unittest.main()
