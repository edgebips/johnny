"""Tests for inventories."""

# pylint: disable=line-too-long,missing-function-docstring

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from decimal import Decimal
from typing import List
from unittest.mock import patch
import itertools
import unittest

from parameterized import parameterized

from johnny.base import inventories
from johnny.base.etl import petl, Table


Lot = inventories.Lot
OpenCloseFifoInventory = inventories.OpenCloseFifoInventory
MatchError = inventories.MatchError


ZERO = Decimal(0)


def _CreateTestMatchId(transaction_id: str) -> str:
    return "m-{}".format(transaction_id)


class TestMatchInventory(unittest.TestCase):

    def _create_inventory(self):
        inv = inventories.MatchInventory()
        inv.create_id_fn = _CreateTestMatchId
        return inv

    def test_buy_sell(self):
        inv = self._create_inventory()
        self.assertEqual((Decimal(0), 'm-A'), inv.match(Decimal(+2), 'A'))
        self.assertEqual((Decimal(0), 'm-A'), inv.match(Decimal(+1), 'B'))
        self.assertEqual((Decimal(-1), 'm-A'), inv.match(Decimal(-1), 'C'))
        self.assertEqual((Decimal(-1), 'm-A'), inv.match(Decimal(-1), 'D'))
        self.assertEqual((Decimal(-1), 'm-A'), inv.match(Decimal(-1), 'E'))
        self.assertEqual((Decimal(0), 'm-F'), inv.match(Decimal(-1), 'F'))

    def test_sell_buy(self):
        inv = self._create_inventory()
        self.assertEqual((Decimal(0), 'm-A'), inv.match(Decimal(-2), 'A'))
        self.assertEqual((Decimal(0), 'm-A'), inv.match(Decimal(-1), 'B'))
        self.assertEqual((Decimal(+1), 'm-A'), inv.match(Decimal(+1), 'C'))
        self.assertEqual((Decimal(+1), 'm-A'), inv.match(Decimal(+1), 'D'))
        self.assertEqual((Decimal(+1), 'm-A'), inv.match(Decimal(+1), 'E'))
        self.assertEqual((Decimal(0), 'm-F'), inv.match(Decimal(+1), 'F'))

    def test_crossover(self):
        inv = self._create_inventory()
        self.assertEqual((Decimal(0), 'm-A'), inv.match(Decimal(+1), 'A'))
        self.assertEqual((Decimal(-1), 'm-A'), inv.match(Decimal(-2), 'B'))
        self.assertEqual((Decimal(+1), 'm-A'), inv.match(Decimal(+2), 'C'))
        self.assertEqual((Decimal(-1), 'm-A'), inv.match(Decimal(-1), 'D'))
        self.assertEqual((Decimal(0), 'm-E'), inv.match(Decimal(-3), 'E'))

    def test_multiple(self):
        inv = self._create_inventory()
        self.assertEqual((Decimal(0), 'm-A'), inv.match(Decimal(+1), 'A'))
        self.assertEqual((Decimal(0), 'm-A'), inv.match(Decimal(+1), 'B'))
        self.assertEqual((Decimal(0), 'm-A'), inv.match(Decimal(+1), 'C'))
        self.assertEqual((Decimal(0), 'm-A'), inv.match(Decimal(+1), 'D'))
        self.assertEqual((Decimal(-4), 'm-A'), inv.match(Decimal(-5), 'E'))
        self.assertEqual((Decimal(1), 'm-A'), inv.match(Decimal(+1), 'F'))

    def test_expire_zero(self):
        inv = self._create_inventory()
        self.assertEqual((Decimal(0), None), inv.expire('A'))
        self.assertEqual((Decimal(0), None), inv.expire('B'))

    def test_expire_nonzero(self):
        inv = self._create_inventory()
        self.assertEqual((Decimal(0), 'm-A'), inv.match(Decimal(+1), 'A'))
        self.assertEqual((Decimal(-1), 'm-A'), inv.expire('A'))

        self.assertEqual((Decimal(0), 'm-B'), inv.match(Decimal(-1), 'B'))
        self.assertEqual((Decimal(0), 'm-B'), inv.match(Decimal(-1), 'C'))
        self.assertEqual((Decimal(2), 'm-B'), inv.expire('B'))


class TestFifoInventory(unittest.TestCase):

    def _create_inventory(self):
        inv = inventories.FifoInventory()
        inv.create_id_fn = _CreateTestMatchId
        return inv

    def test_buy_sell(self):
        inv = self._create_inventory()
        self.assertEqual((Decimal(0), Decimal(0), 'm-A'), inv.match(Decimal(+2), Decimal(100), 'A'))
        self.assertEqual((Decimal(0), Decimal(0), 'm-A'), inv.match(Decimal(+2), Decimal(110), 'B'))
        self.assertEqual((Decimal(1), Decimal(100), 'm-A'), inv.match(Decimal(-1), Decimal(120), 'C'))
        self.assertEqual((Decimal(0), Decimal(0), 'm-A'), inv.match(Decimal(+2), Decimal(130), 'D'))
        self.assertEqual((Decimal(1), Decimal(100), 'm-A'), inv.match(Decimal(-1), Decimal(140), 'E'))
        self.assertEqual((Decimal(3), Decimal(2*110 + 130), 'm-A'), inv.match(Decimal(-3), Decimal(150), 'E'))

    def test_sell_buy(self):
        inv = self._create_inventory()
        self.assertEqual((Decimal(0), Decimal(0), 'm-A'), inv.match(Decimal(-2), Decimal(100), 'A'))
        self.assertEqual((Decimal(0), Decimal(0), 'm-A'), inv.match(Decimal(-2), Decimal(110), 'B'))
        self.assertEqual((Decimal(1), Decimal(100), 'm-A'), inv.match(Decimal(+1), Decimal(120), 'C'))
        self.assertEqual((Decimal(0), Decimal(0), 'm-A'), inv.match(Decimal(-2), Decimal(130), 'D'))
        self.assertEqual((Decimal(1), Decimal(100), 'm-A'), inv.match(Decimal(+1), Decimal(140), 'E'))
        self.assertEqual((Decimal(3), Decimal(2*110 + 130), 'm-A'), inv.match(Decimal(+3), Decimal(150), 'E'))

    def test_cross_zero(self):
        inv = self._create_inventory()
        self.assertEqual((Decimal(0),   Decimal(0), 'm-A'), inv.match(Decimal(+1), Decimal(100), 'A'))
        self.assertEqual((Decimal(1), Decimal(100), 'm-A'), inv.match(Decimal(-2), Decimal(110), 'B'))
        self.assertEqual((Decimal(1), Decimal(110), 'm-A'), inv.match(Decimal(+2), Decimal(120), 'C'))
        self.assertEqual((Decimal(1), Decimal(120), 'm-A'), inv.match(Decimal(-2), Decimal(130), 'D'))
        self.assertEqual((Decimal(1), Decimal(130), 'm-A'), inv.match(Decimal(+2), Decimal(140), 'E'))
        self.assertEqual((Decimal(1), Decimal(140), 'm-A'), inv.match(Decimal(-2), Decimal(150), 'F'))
        self.assertEqual((Decimal(1), Decimal(150), 'm-A'), inv.match(Decimal(+2), Decimal(160), 'G'))
        self.assertEqual((Decimal(1), Decimal(160), 'm-A'), inv.match(Decimal(-1), Decimal(170), 'H'))

        # Check that it resets on zero.
        self.assertEqual((Decimal(0),   Decimal(0), 'm-I'), inv.match(Decimal(-1), Decimal(180), 'I'))

    def test_expire_zero(self):
        inv = self._create_inventory()
        self.assertEqual((Decimal(0), Decimal(0), None), inv.expire('A'))
        self.assertEqual((Decimal(0), Decimal(0), None), inv.expire('B'))

    def test_expire_nonzero(self):
        inv = self._create_inventory()
        self.assertEqual((Decimal(0), Decimal(0), 'm-A'), inv.match(Decimal(+1), Decimal(100), 'A'))
        self.assertEqual((Decimal(1), Decimal(100), 'm-A'), inv.expire('B'))
        self.assertEqual((Decimal(0), Decimal(0), None), inv.expire('C'))



HEADER = ('transaction_id', 'rowtype', 'effect', 'instruction', 'quantity', 'cost', 'match_id')


def TestTable(records: List[tuple]):
    return petl.wrap(itertools.chain([HEADER], records))


def MatchTable(table: Table,
               use_effect: bool=True,
               debug: bool=False) -> tuple[OpenCloseFifoInventory, Table]:
    # Create the inventory.
    inv = inventories.OpenCloseFifoInventory()
    inv.create_id_fn = _CreateTestMatchId

    # Accumulator for new rows.
    new_rows = []
    def accum(rec):
        new_rows.append(rec)

    # Run through the input table.
    nomatch_table = table.convert('match_id', lambda _: '')
    for rec in nomatch_table.namedtuples():
        if rec.rowtype == 'Trade':
            if use_effect:
                if rec.effect == 'OPENING':
                    inv.opening(rec, accum)
                    continue
                elif rec.effect == 'CLOSING':
                    inv.closing(rec, accum)
                    continue
                assert not rec.effect
            inv.match(rec, accum, debug=debug)
        elif rec.rowtype == 'Expire':
            inv.expire(rec, accum)
        else:
            raise ValueError(f"Invalid row type: {rec.rowtype}")

    return inv, TestTable(new_rows)


def AssertTableEqual(table1: Table, table2: Table):
    for rec1, rec2 in zip(table1.records(), table2.records()):
        assert rec1 == rec2, (rec1, rec2)


def SplitSignedQuantity(quantity: Decimal) -> tuple[str, Decimal]:
    return ('SELL' if quantity < 0 else 'BUY'), Decimal(abs(quantity))


def OtherEffect(effect: str):
    if not effect:
        return effect
    return 'BUY' if effect == 'SELL' else 'SELL'


class TestOpenCloseFifoInventory(unittest.TestCase):

    @parameterized.expand([(+1,), (-1,), (+4,), (-4,)])
    def test_opening_from_empty(self, quantity):
        instruction, uq = SplitSignedQuantity(quantity)
        rows = [('a', 'Trade', 'OPENING', instruction, uq, uq * Decimal(100), 'm-a')]
        inv, table = MatchTable(TestTable(rows))
        AssertTableEqual(TestTable(rows), table)
        self.assertEqual(1, len(inv.lots))
        self.assertEqual(Lot(Decimal(quantity), Decimal(100)), inv.lots[0])

    @parameterized.expand([(+1,), (-1,)])
    def test_opening_and_opening(self, quantity):
        instruction, uq = SplitSignedQuantity(quantity)
        rows = [('a', 'Trade', 'OPENING', instruction, uq, uq * Decimal(100), 'm-a'),
                ('b', 'Trade', 'OPENING', instruction, 2 * uq, 2 * uq * Decimal(110), 'm-a')]
        inv, table = MatchTable(TestTable(rows))
        AssertTableEqual(TestTable(rows), table)
        self.assertEqual(2, len(inv.lots))
        self.assertEqual(Lot(Decimal(quantity), Decimal(100)), inv.lots[0])
        self.assertEqual(Lot(Decimal(2 * quantity), Decimal(110)), inv.lots[1])

    @parameterized.expand([(+1,), (-1,), (+4,), (-4,)])
    def test_opening_and_closing_equal_amounts(self, q):
        #print('XXX', q)
        oi, uq = SplitSignedQuantity(q)
        ci = 'BUY' if oi == 'SELL' else 'SELL'
        rows = [('a', 'Trade', 'OPENING', oi, uq, uq * Decimal(100), 'm-a'),
                ('b', 'Trade', 'CLOSING', ci, uq, uq * Decimal(110), 'm-a')]
        inv, table = MatchTable(TestTable(rows))
        #print(table.lookallstr())
        AssertTableEqual(TestTable(rows), table)
        self.assertEqual(0, len(inv.lots))

    @parameterized.expand([(+1,), (-1,)])
    def test_opening_and_closing_less(self, sign):
        oi, oq = SplitSignedQuantity(sign * 4)
        ci, cq = SplitSignedQuantity(-sign * 3)
        rows = [('a', 'Trade', 'OPENING', oi, oq, oq * Decimal(100), 'm-a'),
                ('b', 'Trade', 'CLOSING', ci, cq, cq * Decimal(110), 'm-a')]
        inv, table = MatchTable(TestTable(rows))
        AssertTableEqual(TestTable(rows), table)
        self.assertEqual(1, len(inv.lots))
        self.assertEqual(Lot(Decimal(sign * (oq-cq)), Decimal(100)), inv.lots[0])

    def test_opening_multi_and_closing_one(self):
        rows = [
            ('a', 'Trade', 'OPENING', 'BUY', Decimal(2), 2 * Decimal(100), 'm-a'),
            ('b', 'Trade', 'OPENING', 'BUY', Decimal(2), 2 * Decimal(101), 'm-a'),
            ('c', 'Trade', 'OPENING', 'BUY', Decimal(2), 2 * Decimal(102), 'm-a'),
            ('d', 'Trade', 'CLOSING', 'SELL', Decimal(5), 5 * Decimal(103), 'm-a'),
        ]
        inv, table = MatchTable(TestTable(rows))
        AssertTableEqual(TestTable(rows), table)
        self.assertEqual(1, len(inv.lots))
        self.assertEqual(Lot(Decimal(1), Decimal(102)), inv.lots[0])

    def test_opening_one_and_closing_multi(self):
        rows = [
            ('a', 'Trade', 'OPENING', 'BUY', Decimal(6), 6 * Decimal(100), 'm-a'),
            ('b', 'Trade', 'CLOSING', 'SELL', Decimal(1), 1 * Decimal(101), 'm-a'),
            ('c', 'Trade', 'CLOSING', 'SELL', Decimal(2), 2 * Decimal(102), 'm-a'),
            ('d', 'Trade', 'CLOSING', 'SELL', Decimal(2), 2 * Decimal(103), 'm-a'),
        ]
        inv, table = MatchTable(TestTable(rows))
        AssertTableEqual(TestTable(rows), table)
        self.assertEqual(1, len(inv.lots))
        self.assertEqual(Lot(Decimal(1), Decimal(100)), inv.lots[0])

    @parameterized.expand([(+1,), (-1,)])
    def test_opening_and_closing_through(self, sign):
        oi = 'BUY' if sign > 0 else 'SELL'
        ci = 'BUY' if not sign > 0 else 'SELL'
        rows = [
            ('a', 'Trade', 'OPENING', oi, Decimal(2), 2 * Decimal(100), 'm-a'),
            ('b', 'Trade', 'CLOSING', ci, Decimal(3), 3 * Decimal(101), 'm-a'),
        ]
        inv, table = MatchTable(TestTable(rows))

        expected_rows = [
            ('a', 'Trade', 'OPENING', oi, Decimal(2), 2 * Decimal(100), 'm-a'),
            ('b.1', 'Trade', 'CLOSING', ci, Decimal(2), 2 * Decimal(101), 'm-a'),
            ('b.2', 'Trade', 'OPENING', ci, Decimal(1), 1 * Decimal(101), 'm-a'),
        ]
        AssertTableEqual(TestTable(expected_rows), table)
        self.assertEqual(1, len(inv.lots))
        self.assertEqual(Lot(Decimal(-sign), Decimal(101)), inv.lots[0])

    def test_opening_separate_ones(self):
        rows = [
            ('a', 'Trade', 'OPENING', 'BUY', Decimal(2), 2 * Decimal(100), 'm-a'),
            ('b', 'Trade', 'CLOSING', 'SELL', Decimal(2), 2 * Decimal(101), 'm-a'),
            ('c', 'Trade', 'OPENING', 'BUY', Decimal(1), 1 * Decimal(100), 'm-c'),
            ('d', 'Trade', 'CLOSING', 'SELL', Decimal(1), 1 * Decimal(101), 'm-c'),
            ('f', 'Trade', 'OPENING', 'SELL', Decimal(1), 1 * Decimal(101), 'm-f'),
            ('e', 'Trade', 'CLOSING', 'BUY', Decimal(1), 1 * Decimal(100), 'm-f'),
        ]
        inv, table = MatchTable(TestTable(rows))
        AssertTableEqual(TestTable(rows), table)
        self.assertEqual(0, len(inv.lots))

    def test_error_new_closing(self):
        rows = [
            ('a', 'Trade', 'CLOSING', 'SELL', Decimal(1), Decimal(101), 'm-a'),
        ]
        with self.assertRaises(MatchError) as econtext:
            MatchTable(TestTable(rows), use_effect=False)
        self.assertRegex(str(econtext.exception), 'New position not opening')

    def test_error_augmenting_closing(self):
        rows = [
            ('a', 'Trade', 'OPENING', 'BUY', Decimal(1), Decimal(101), 'm-a'),
            ('b', 'Trade', 'CLOSING', 'BUY', Decimal(1), Decimal(102), 'm-b'),
        ]
        with self.assertRaises(MatchError) as econtext:
            MatchTable(TestTable(rows), use_effect=False)
        self.assertRegex(str(econtext.exception), 'Augmenting position not opening')

    def test_error_reducing_opening(self):
        rows = [
            ('a', 'Trade', 'OPENING', 'BUY', Decimal(2), Decimal(101), 'm-a'),
            ('b', 'Trade', 'OPENING', 'SELL', Decimal(1), Decimal(101), 'm-b'),
        ]
        with self.assertRaises(MatchError) as econtext:
            MatchTable(TestTable(rows), use_effect=False)
        self.assertRegex(str(econtext.exception), 'Reducing position not closing')

    @parameterized.expand([('BUY',), ('SELL',)])
    def test_auto_effect(self, effect):
        rows = [
            ('a', 'Trade', 'OPENING', effect, Decimal(1), Decimal(101), 'm-a'),
            ('b', 'Trade', 'CLOSING', OtherEffect(effect), Decimal(1), Decimal(102), 'm-a'),
        ]
        inv, table = MatchTable(TestTable(rows).convert('effect', lambda _: ''))
        AssertTableEqual(TestTable(rows), table)
        self.assertEqual(0, len(inv.lots))

    def test_auto_effect_crossing(self):
        rows = [
            ('a', 'Trade', 'OPENING', 'BUY', Decimal(1), Decimal(101), 'm-a'),
            ('b', 'Trade', 'CLOSING', 'SELL', Decimal(2), 2 * Decimal(102), 'm-a'),
            ('c', 'Trade', 'OPENING', 'SELL', Decimal(2), 2 * Decimal(103), 'm-a'),
            ('d', 'Trade', 'CLOSING', 'BUY', Decimal(4), 4 * Decimal(104), 'm-a'),
            ('e', 'Trade', 'CLOSING', 'SELL', Decimal(1), Decimal(105), 'm-a'),
            ('f', 'Trade', 'OPENING', 'BUY', Decimal(1), Decimal(106), 'm-f'),
        ]
        inv, table = MatchTable(TestTable(rows).convert('effect', lambda _: ''))
        expected_rows = [
            ('a', 'Trade', 'OPENING', 'BUY', Decimal(1), Decimal(101), 'm-a'),
            ('b.1', 'Trade', 'CLOSING', 'SELL', Decimal(1), Decimal(102), 'm-a'),
            ('b.2', 'Trade', 'OPENING', 'SELL', Decimal(1), Decimal(102), 'm-a'),
            ('c', 'Trade', 'OPENING', 'SELL', Decimal(2), 2 * Decimal(103), 'm-a'),
            ('d.1', 'Trade', 'CLOSING', 'BUY', Decimal(3), 3 * Decimal(104), 'm-a'),
            ('d.2', 'Trade', 'OPENING', 'BUY', Decimal(1), Decimal(104), 'm-a'),
            ('e', 'Trade', 'CLOSING', 'SELL', Decimal(1), Decimal(105), 'm-a'),
            ('f', 'Trade', 'OPENING', 'BUY', Decimal(1), Decimal(106), 'm-f'),
        ]
        AssertTableEqual(TestTable(expected_rows), table)

    def test_auto_effect_and_reset_match_id(self):
        rows = [
            ('a', 'Trade', 'OPENING', 'BUY', Decimal(1), Decimal(101), 'm-a'),
            ('b', 'Trade', 'CLOSING', 'SELL', Decimal(1), Decimal(102), 'm-a'),
            ('c', 'Trade', 'OPENING', 'SELL', Decimal(1), Decimal(103), 'm-c'),
            ('d', 'Trade', 'CLOSING', 'BUY', Decimal(1), Decimal(104), 'm-c'),
            ('e', 'Trade', 'OPENING', 'BUY', Decimal(1), Decimal(105), 'm-e'),
            ('f', 'Trade', 'CLOSING', 'SELL', Decimal(2), 2 * Decimal(106), 'm-e'),
            ('g', 'Trade', 'CLOSING', 'BUY', Decimal(1), Decimal(107), 'm-e'),
            ('h', 'Trade', 'OPENING', 'SELL', Decimal(1), Decimal(108), 'm-h'),
            ('i', 'Trade', 'CLOSING', 'BUY', Decimal(1), Decimal(109), 'm-h'),
        ]
        inv, table = MatchTable(TestTable(rows).convert('effect', lambda _: ''))
        expected_rows = [
            ('a', 'Trade', 'OPENING', 'BUY', Decimal(1), Decimal(101), 'm-a'),
            ('b', 'Trade', 'CLOSING', 'SELL', Decimal(1), Decimal(102), 'm-a'),
            ('c', 'Trade', 'OPENING', 'SELL', Decimal(1), Decimal(103), 'm-c'),
            ('d', 'Trade', 'CLOSING', 'BUY', Decimal(1), Decimal(104), 'm-c'),
            ('e', 'Trade', 'OPENING', 'BUY', Decimal(1), Decimal(105), 'm-e'),
            ('f.1', 'Trade', 'CLOSING', 'SELL', Decimal(1), Decimal(106), 'm-e'),
            ('f.2', 'Trade', 'OPENING', 'SELL', Decimal(1), Decimal(106), 'm-e'),
            ('g', 'Trade', 'CLOSING', 'BUY', Decimal(1), Decimal(107), 'm-e'),
            ('h', 'Trade', 'OPENING', 'SELL', Decimal(1), Decimal(108), 'm-h'),
            ('i', 'Trade', 'CLOSING', 'BUY', Decimal(1), Decimal(109), 'm-h'),
        ]
        AssertTableEqual(TestTable(expected_rows), table)

    def test_add_invalid_opening(self):
        rows = [
            ('a', 'Trade', 'OPENING', 'SELL', Decimal(3), 3 * Decimal(100), 'm-a'),
            ('b', 'Trade', 'OPENING', 'BUY', Decimal(2), 2 * Decimal(101), 'm-a'),
        ]
        with self.assertRaises(MatchError) as econtext:
            MatchTable(TestTable(rows))
        self.assertRegex(str(econtext.exception), 'Invalid opening position closing')

    def test_add_invalid_closing(self):
        rows = [
            ('a', 'Trade', 'CLOSING', 'SELL', Decimal(3), 3 * Decimal(100), 'm-a'),
        ]
        with self.assertRaises(MatchError) as econtext:
            MatchTable(TestTable(rows))
        self.assertRegex(str(econtext.exception), 'Invalid closing position opening')

    def test_expire_zero(self):
        rows = [
            ('a', 'Expire', '', '', Decimal(1), Decimal(100), 'm-a'),
        ]
        with self.assertRaises(MatchError) as econtext:
            MatchTable(TestTable(rows))
        self.assertRegex(str(econtext.exception), 'Invalid expiration with no lots')

    @parameterized.expand([('BUY',), ('SELL',)])
    def test_expire_nonzero(self, effect):
        rows = [
            ('a', 'Trade', 'OPENING', effect, Decimal(1), Decimal(100), 'm-a'),
            ('b', 'Expire', '', '', Decimal(1), Decimal(0), 'm-b'),
        ]
        inv, table = MatchTable(TestTable(rows))
        expected_rows = [
            ('a', 'Trade', 'OPENING', effect, Decimal(1), Decimal(100), 'm-a'),
            ('b', 'Expire', 'CLOSING', OtherEffect(effect), Decimal(1), Decimal(0), 'm-a'),
        ]
        AssertTableEqual(TestTable(expected_rows), table)


if __name__ == '__main__':
    unittest.main()
