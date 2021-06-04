__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from decimal import Decimal
import unittest

from johnny.base import number


ZERO = Decimal(0)


class DecimalTest(unittest.TestCase):

    def test_todecimal_na_values(self):
        self.assertEqual(ZERO, number.ToDecimal("--"))
        self.assertEqual(ZERO, number.ToDecimal("N/A"))
        self.assertEqual(ZERO, number.ToDecimal("N/A (Split Position)"))
        self.assertEqual(ZERO, number.ToDecimal(""))

    def test_todecimal_bond_32th(self):
        self.assertEqual(Decimal('100.'), number.ToDecimal("100'00"))
        self.assertEqual(Decimal('100.03125'), number.ToDecimal("100'01"))
        self.assertEqual(Decimal('100.5'), number.ToDecimal("100'16"))
        self.assertEqual(Decimal('100.96875'), number.ToDecimal("100'31"))
        self.assertEqual(Decimal('101.'), number.ToDecimal("100'32"))

        self.assertEqual(Decimal('100.'), number.ToDecimal("100'000"))
        self.assertEqual(Decimal('100.00390625'), number.ToDecimal("100'001"))
        self.assertEqual(Decimal('100.0078125'), number.ToDecimal("100'002"))

        with self.assertRaises(ValueError):
            number.ToDecimal("100'0001")

    def test_todecimal_bond_64th(self):
        self.assertEqual(Decimal('100.'), number.ToDecimal('100"00'))
        self.assertEqual(Decimal('100.015625'), number.ToDecimal('100"01'))
        self.assertEqual(Decimal('100.484375'), number.ToDecimal('100"31'))
        self.assertEqual(Decimal('100.5'), number.ToDecimal('100"32'))
        self.assertEqual(Decimal('100.984375'), number.ToDecimal('100"63'))
        self.assertEqual(Decimal('101.'), number.ToDecimal('100"64'))

        self.assertEqual(Decimal('100.'), number.ToDecimal('100"000'))
        self.assertEqual(Decimal('100.001953125'), number.ToDecimal('100"001'))
        self.assertEqual(Decimal('100.00390625'), number.ToDecimal('100"002'))

        with self.assertRaises(ValueError):
            number.ToDecimal('100"0001')


if __name__ == '__main__':
    unittest.main()
