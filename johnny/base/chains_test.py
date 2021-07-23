__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from datetime import datetime, timedelta
from decimal import Decimal as D
from typing import Any, List, Tuple
import unittest

from johnny.base import chains
from johnny.base import match
from johnny.base import instrument
from johnny.base.etl import petl, Table


ZERO = D(0)


def addtime(prv, cur, nxt):
    if prv is None:
        return datetime(2021, 7, 1, 0, 0, 1)
    else:
        return prv.datetime + timedelta(seconds=1)


def process(rows: List[Any]) -> Tuple[Table, List[str]]:
    table = petl.wrap([
        ('expected_id',
         'rowtype', 'instruction', 'effect', 'quantity', 'symbol', 'cost'),
    ] + rows)
    expected = list(
        table
        .convert('expected_id', lambda v: 'tos.210701_{:06d}.NKE'.format(v))
        .values('expected_id'))
    input_ = (table
              .addfield('account', 'tos')
              .addfieldusingcontext('datetime', addtime)
              .addfield('transaction_id', lambda r: 'T{}'.format(r.datetime.second))
              .addfield('order_id', lambda r: 'O{}'.format(r.datetime.second))
              .cutout('expected_id'))

    actual = (input_

              .addfield('description', '')
              .addfield('price', None)
              .addfield('commissions', ZERO)
              .addfield('fees', ZERO)

              .applyfn(match.Process)

              .cutout('description', 'price', 'commissions', 'fees')

              .applyfn(instrument.Expand, 'symbol')
              .applyfn(chains.Group)
              .applyfn(instrument.Shrink))
    return actual, expected


class TestChains(unittest.TestCase):

    def check_chain_ids(self, rows):
        actual, expected = process(rows)
        self.assertEqual(expected, list(actual.values('chain_id')))

    def test_single_underlying(self):
        self.check_chain_ids([
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE', D('92.10')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE', D('92.15')),
        ])

    def test_single_option(self):
        self.check_chain_ids([
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE_210815_C195', D('2.10')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE_210815_C195', D('2.15')),
        ])

    def test_multi_underlying(self):
        self.check_chain_ids([
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE', D('92.10')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE', D('92.15')),
            (3, 'Trade', 'SELL', 'OPENING', D(2), 'NKE', D('92.20')),
            (3, 'Trade', 'BUY', 'CLOSING', D(2), 'NKE', D('92.25')),
        ])

    def test_multi_option(self):
        self.check_chain_ids([
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE_210815_C195', D('2.10')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE_210815_C195', D('2.15')),
            (3, 'Trade', 'BUY', 'OPENING', D(1), 'NKE_210815_C200', D('2.20')),
            (3, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE_210815_C200', D('2.25')),
        ])

    def test_multi_both(self):
        self.check_chain_ids([
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE', D('92.10')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE', D('92.15')),
            (3, 'Trade', 'BUY', 'OPENING', D(1), 'NKE_210815_C200', D('2.20')),
            (3, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE_210815_C200', D('2.25')),
            (5, 'Trade', 'BUY', 'OPENING', D(1), 'NKE', D('92.30')),
            (5, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE', D('92.35'))])

    def test_overlapping_underlying(self):
        self.check_chain_ids([
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE', D('92.10')),
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE', D('92.15')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE', D('92.20')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE', D('92.20')),
        ])

    def test_overlapping_same_expiration(self):
        self.check_chain_ids([
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE_210815_C195', D('2.10')),
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE_210815_C200', D('2.15')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE_210815_C195', D('2.20')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE_210815_C200', D('2.25')),
        ])

    def test_overlapping_diff_expirations(self):
        self.check_chain_ids([
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE_210815_C195', D('2.10')),
            (2, 'Trade', 'BUY', 'OPENING', D(1), 'NKE_210915_C195', D('2.15')),
            (3, 'Trade', 'BUY', 'OPENING', D(1), 'NKE_211015_C195', D('2.20')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE_210815_C195', D('2.25')),
            (2, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE_210915_C195', D('2.30')),
            (3, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE_211015_C195', D('2.35')),
        ])

    def test_overlapping_over_underlying_only_outer(self):
        self.check_chain_ids([
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE', D('92.10')),
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE_210815_C195', D('2.10')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE_210815_C195', D('2.15')),
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE_210915_C195', D('2.20')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE_210915_C195', D('2.25')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE', D('92.15')),
        ])

    def test_overlapping_over_underlying_only_inner(self):
        self.check_chain_ids([
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE_210815_C195', D('2.10')),
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE', D('92.10')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE_210815_C195', D('2.15')),
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE_210915_C195', D('2.20')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE', D('92.15')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE_210915_C195', D('2.25')),
        ])

    def test_non_overlapping(self):
        self.check_chain_ids([
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE_210815_C195', D('2.10')),
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE', D('92.10')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE_210815_C195', D('2.15')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE', D('92.15')),
            #
            (5, 'Trade', 'BUY', 'OPENING', D(1), 'NKE_210915_C195', D('2.20')),
            (5, 'Trade', 'BUY', 'OPENING', D(1), 'NKE', D('92.20')),
            (5, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE_210915_C195', D('2.25')),
            (5, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE', D('92.25')),
        ])

    def test_overlapping_via_options(self):
        self.check_chain_ids([
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE_210815_C195', D('2.10')),
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE', D('92.10')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE', D('92.15')),
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE', D('92.20')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE', D('92.25')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE_210815_C195', D('2.15')),
        ])

    def test_overlapping_options_but_without_underlying(self):
        self.check_chain_ids([
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE_210815_C195', D('2.10')),
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE', D('92.10')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE', D('92.15')),
            (4, 'Trade', 'BUY', 'OPENING', D(1), 'NKE_210915_C195', D('2.20')),
            (4, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE_210915_C195', D('2.25')),
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE', D('92.20')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE', D('92.25')),
            (1, 'Trade', 'SELL', 'CLOSING', D(1), 'NKE_210815_C195', D('2.15')),
        ])

class TestOpenChains(unittest.TestCase):

    def test_open_position(self):
        actual, expected = process([
            (1, 'Trade', 'BUY', 'OPENING', D(1), 'NKE_210815_C195', D('2.10')),
        ])
        print(actual.lookallstr())




if __name__ == '__main__':
    unittest.main()
