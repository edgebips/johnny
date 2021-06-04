# PositionsTable

Here is the definition of the fields we're expecting from the positions file;

- `account: str`: A unique identifier for the account number. This implicitly defines
  the brokerage. This can be used if a file contains information about multiple
  accounts.

- `group: Optionnal[str]`: A group name that can be created by the application, if
  possible. thinkorswim has the ability to create position groups. Tastyworks
  does not. This is a kind of user label common to multiple positions.

- `symbol: str`: The symbol normalized in unambiguous, cross-platform, readable
  symbology. The symbol for the underlying instrument. This may be an equity,
  equity option, futures, futures option or spot currency pair.

  The individual fields of the instrument's symbol may be expanded into their
  individual components. See the file "instrument.md" for details.

- `quantity: Decimal`: The quantity helpd. This number is signed, positive if
  long, negative if short. Note that this is different than the `transactions`
  table.

- `price: Decimal`: The per-unit average traded price of the instrument. This is
  how much we paid or received per share of this position. This is a signed
  value; for a debit trade, the number will be negative; for a credit trade, the
  number will be positive.

- `mark: Decimal`: The per-unit value on the market of the instrument. The mark
  can be either the midpoint of the market, or the price to exit immediately,
  that's up to you. Typically it's the midpoint. The sign is typically the
  inverse that of the `price` field.

- `cost: Decimal`: The total cost for acquiring this position. This is a signed
  value as well, following the `price` field.

- `net_liq: Decimal`: The total value of this position, as mark by the `mark`
  field. This is a signed value as well, following the `mark` field.

Note that pairs of `price` and `cost`, `mark` and `net_liq` are redundant. We
could simplify the requirements on this file and derive some of these quantities
ourselves.

Also note that the relationship between `quantity` and the other numbers involve
the instrument `multiplier`, which can be derived from the symbol.


## Example:

Here's an example input:

    account  group        symbol                 quantity  price       mark      cost      net_liq
    x1234    FX           /6AM21_ADUM21_C0.795   -1        0.00240     -0.00130  240.00    -130.00
    x1234    FX           /6AM21_ADUM21_C0.84    1         -0.00030    0.00007   -30.00    6.76
    x1234    FX           /6AM21_ADUM21_P0.7     1         -0.00050    0.00020   -50.00    20.00
    x1234    FX           /6AM21_ADUM21_P0.745   -1        0.00250     -0.00155  250.00    -155.00
    x1234    FX           /6CM21_CAUM21_C0.825   -2        0.00110     -0.00230  220.00    -460.00


## Greeks

Greeks are often provided by the platform and parsed by the library code, but
for now left unspecified. **We will be adding their spec later, as we want to
have them in order to compute ratios over the portfolio and groups.***
