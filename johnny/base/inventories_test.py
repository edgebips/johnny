__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import datetime
from decimal import Decimal
import unittest
from unittest.mock import patch

from johnny.base import match
from johnny.base import instrument
from johnny.base import inventories
from johnny.base.etl import petl, Table, Record, PrintToPython


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


if __name__ == '__main__':
    unittest.main()
