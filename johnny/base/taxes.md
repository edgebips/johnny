# Tax Reconciliation

We define a format for files provided by brokers. There are two types of files:

1. Worksheets for computing taxes with short and long term components
2. Form 8949 for everything but futures, with wash sales adjustments and
   adjusted gain.

We parse these and attempt to match the positions we imported in Johnny. This is
the schema for those two tables.


## Tax Worksheets

These can be downloaded from brokers, contain the full set of matches (trades)
with short-term and long-term categorization and no wash sales adjustments. This
is where brokers select which closing transactions match which open transaction
(and you can use lot identification to change it usually).

Fields, non-optional:

- `instype: str`: The instrument type.
- `symbol: str`: Our normalized Johnny symbol.
- `cost: Decimal`: Cost of the trade.
- `proceeds: Decimal`: Proceeds of the trade.
- `st_gain_loss: Decimal`: Short-term gain-loss
- `lt_gain_loss: Decimal`: Long-term gain-loss
- `gain_loss: Decimal`: Total gain-loss (short-term + long-term)

Note that the gain adjustment columns in the worksheets should always be set to
zero.


## Form 8949

- `instype: str`: The instrument type.
- `symbol: str`: Our normalized Johnny symbol.
- `cost: Decimal`: Cost of the trade.
- `proceeds: Decimal`: Proceeds of the trade.
- `gain_adj: Decimal`: Gain adjustment for wash sales.
- `gain_loss: Decimal`: Total gain-loss (short-term + long-term)
- `term: str`: Long-term vs. short-term category, either `ST` or `LT`.
- `box: str`: The 1099 box the trade belongs to, `A`, `B`, `D`, `E`.

Note that brokers wash away inadmissible sales but seem to not add the washed
cost basis back in. That's for you to do.

## A Note About Cost & Proceedings

The cost and proceedings values are arranged in two possible ways:

* *inverted*: Cost and proceeds are swapped and the signs inverted in order to
  make both amounts positive. This is used in the worksheets and Form 8949.

* *nullified*: Cost and proceeds are offset to make the cost to zero. This is
  used in the 1099 reporting.
