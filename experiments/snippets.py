# Inventories

    # @parameterized.expand([(+1,), (-1,)])
    # def test_valid_opening(self, quantity):
    #     """Tests trivial valid openings."""
    #     inv = self._create_inventory()
    #     self.assertEqual((Decimal(0), Decimal(0), 'm-A'), inv.opening(Decimal(quantity), Decimal(100), 'A'))
    #     self.assertEqual(Decimal(quantity), inv.quantity())
    #     self.assertEqual(Decimal(0), inv.initial)

    # @parameterized.expand([(+1,), (-1,)])
    # def test_invalid_opening(self, sgn):
    #     """Test invalid openings which require correction."""
    #     inv = self._create_inventory()
    #     self.assertEqual((Decimal(0), Decimal(0), 'm-A'), inv.opening(Decimal(-2 * sgn), Decimal(100), 'A'))
    #     self.assertEqual((Decimal(0), Decimal(0), 'm-A'), inv.opening(Decimal(+1 * sgn), Decimal(110), 'A'))
    #     self.assertEqual(Decimal(+1 * sgn), inv.quantity())
    #     self.assertEqual(Decimal(+2 * sgn), inv.initial)
    #     self.assertEqual([Lot(quantity=Decimal('1') * sgn, basis=Decimal('110'))], inv.lots)

    # @parameterized.expand([
    #     # Positive cases.
    #     (+2, -3, +1), # Overflowing reduction.
    #     (+2, -2, 0),  # Valid case.
    #     (+2, -1, 0),  # Valid case (partial).
    #     (+2, 0, 0),   # Trivial case (no quantity).
    #     (+2, +1, -3), # Underflow requiring correction.
    #     (+2, +2, -4), # Underflow requiring correction.
    #     (+2, +3, -5), # Underflow requiring correction.
    #     # Negative cases.
    #     (-2, +3, -1), # Overflowing reduction.
    #     (-2, +2, 0),  # Valid case.
    #     (-2, +1, 0),  # Valid case (partial).
    #     (-2, 0, 0),   # Trivial case (no quantity).
    #     (-2, -1, +3), # Underflow requiring correction.
    #     (-2, -2, +4), # Underflow requiring correction.
    #     (-2, -3, +5), # Underflow requiring correction.
    # ])
    # def test_valid_closing(self, base, quantity, expected_correction):
    #     """Tests valid and invalid (correcting) closings."""
    #     inv = self._create_inventory()
    #     self.assertEqual((Decimal(0), Decimal(0), 'm-A'), inv.opening(Decimal(base), Decimal(100), 'A'))
    #     matched, _, __ = inv.closing(Decimal(quantity), Decimal(110), 'A')
    #     self.assertEqual(abs(quantity), matched)
    #     self.assertEqual(expected_correction, inv.initial)
