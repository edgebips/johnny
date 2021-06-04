__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from decimal import Decimal as D
import unittest

from johnny.base import match


def _CreateTestMatchId(transaction_id: str) -> str:
    return "m-{}".format(transaction_id)


match.MatchInventory.create_id_fn = staticmethod(_CreateTestMatchId)
match.FifoInventory.create_id_fn = staticmethod(_CreateTestMatchId)


class TestMatchInventory(unittest.TestCase):

    def test_buy_sell(self):
        inv = match.MatchInventory()
        self.assertEqual((D(0), 'm-A'), inv.match(D(+2), 'A'))
        self.assertEqual((D(0), 'm-A'), inv.match(D(+1), 'B'))
        self.assertEqual((D(-1), 'm-A'), inv.match(D(-1), 'C'))
        self.assertEqual((D(-1), 'm-A'), inv.match(D(-1), 'D'))
        self.assertEqual((D(-1), 'm-A'), inv.match(D(-1), 'E'))
        self.assertEqual((D(0), 'm-F'), inv.match(D(-1), 'F'))

    def test_sell_buy(self):
        inv = match.MatchInventory()
        self.assertEqual((D(0), 'm-A'), inv.match(D(-2), 'A'))
        self.assertEqual((D(0), 'm-A'), inv.match(D(-1), 'B'))
        self.assertEqual((D(+1), 'm-A'), inv.match(D(+1), 'C'))
        self.assertEqual((D(+1), 'm-A'), inv.match(D(+1), 'D'))
        self.assertEqual((D(+1), 'm-A'), inv.match(D(+1), 'E'))
        self.assertEqual((D(0), 'm-F'), inv.match(D(+1), 'F'))

    def test_crossover(self):
        inv = match.MatchInventory()
        self.assertEqual((D(0), 'm-A'), inv.match(D(+1), 'A'))
        self.assertEqual((D(-1), 'm-A'), inv.match(D(-2), 'B'))
        self.assertEqual((D(+1), 'm-A'), inv.match(D(+2), 'C'))
        self.assertEqual((D(-1), 'm-A'), inv.match(D(-1), 'D'))
        self.assertEqual((D(0), 'm-E'), inv.match(D(-3), 'E'))

    def test_multiple(self):
        inv = match.MatchInventory()
        self.assertEqual((D(0), 'm-A'), inv.match(D(+1), 'A'))
        self.assertEqual((D(0), 'm-A'), inv.match(D(+1), 'B'))
        self.assertEqual((D(0), 'm-A'), inv.match(D(+1), 'C'))
        self.assertEqual((D(0), 'm-A'), inv.match(D(+1), 'D'))
        self.assertEqual((D(-4), 'm-A'), inv.match(D(-5), 'E'))
        self.assertEqual((D(1), 'm-A'), inv.match(D(+1), 'F'))

    def test_expire_zero(self):
        inv = match.MatchInventory()
        self.assertEqual((D(0), None), inv.expire('A'))
        self.assertEqual((D(0), None), inv.expire('B'))

    def test_expire_nonzero(self):
        inv = match.MatchInventory()
        self.assertEqual((D(0), 'm-A'), inv.match(D(+1), 'A'))
        self.assertEqual((D(-1), 'm-A'), inv.expire('A'))

        self.assertEqual((D(0), 'm-B'), inv.match(D(-1), 'B'))
        self.assertEqual((D(0), 'm-B'), inv.match(D(-1), 'C'))
        self.assertEqual((D(2), 'm-B'), inv.expire('B'))


class TestFifoInventory(unittest.TestCase):

    def test_buy_sell(self):
        inv = match.FifoInventory()
        self.assertEqual((D(0), D(0), 'm-A'), inv.match(D(+2), D(100), 'A'))
        self.assertEqual((D(0), D(0), 'm-A'), inv.match(D(+2), D(110), 'B'))
        self.assertEqual((D(1), D(100), 'm-A'), inv.match(D(-1), D(120), 'C'))
        self.assertEqual((D(0), D(0), 'm-A'), inv.match(D(+2), D(130), 'D'))
        self.assertEqual((D(1), D(100), 'm-A'), inv.match(D(-1), D(140), 'E'))
        self.assertEqual((D(3), D(2*110 + 130), 'm-A'), inv.match(D(-3), D(150), 'E'))

    def test_sell_buy(self):
        inv = match.FifoInventory()
        self.assertEqual((D(0), D(0), 'm-A'), inv.match(D(-2), D(100), 'A'))
        self.assertEqual((D(0), D(0), 'm-A'), inv.match(D(-2), D(110), 'B'))
        self.assertEqual((D(1), D(100), 'm-A'), inv.match(D(+1), D(120), 'C'))
        self.assertEqual((D(0), D(0), 'm-A'), inv.match(D(-2), D(130), 'D'))
        self.assertEqual((D(1), D(100), 'm-A'), inv.match(D(+1), D(140), 'E'))
        self.assertEqual((D(3), D(2*110 + 130), 'm-A'), inv.match(D(+3), D(150), 'E'))

    def test_cross_zero(self):
        inv = match.FifoInventory()
        self.assertEqual((D(0),   D(0), 'm-A'), inv.match(D(+1), D(100), 'A'))
        self.assertEqual((D(1), D(100), 'm-A'), inv.match(D(-2), D(110), 'B'))
        self.assertEqual((D(1), D(110), 'm-A'), inv.match(D(+2), D(120), 'C'))
        self.assertEqual((D(1), D(120), 'm-A'), inv.match(D(-2), D(130), 'D'))
        self.assertEqual((D(1), D(130), 'm-A'), inv.match(D(+2), D(140), 'E'))
        self.assertEqual((D(1), D(140), 'm-A'), inv.match(D(-2), D(150), 'F'))
        self.assertEqual((D(1), D(150), 'm-A'), inv.match(D(+2), D(160), 'G'))
        self.assertEqual((D(1), D(160), 'm-A'), inv.match(D(-1), D(170), 'H'))

        # Check that it resets on zero.
        self.assertEqual((D(0),   D(0), 'm-I'), inv.match(D(-1), D(180), 'I'))

    def test_expire_zero(self):
        inv = match.FifoInventory()
        self.assertEqual((D(0), D(0), None), inv.expire('A'))
        self.assertEqual((D(0), D(0), None), inv.expire('B'))

    def test_expire_nonzero(self):
        inv = match.FifoInventory()
        self.assertEqual((D(0), D(0), 'm-A'), inv.match(D(+1), D(100), 'A'))
        self.assertEqual((D(1), D(100), 'm-A'), inv.expire('B'))
        self.assertEqual((D(0), D(0), None), inv.expire('C'))


if __name__ == '__main__':
    unittest.main()
