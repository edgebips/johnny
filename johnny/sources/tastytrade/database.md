# Tastytrade Internal Transactions Log
## Introduction

Here is a spec describing the format of the JSON documents that can be
downloaded for each of the transactions in the log, the field names and their
possible values. Note that this is possibly incomplete, and only derived by
observing the values appearing in my personal user streams of transactions. As
per Tastytrade, there is no official specification document describing the
format of the log (despite its availability in the History tab).

You can use the `tastytrade-update` script in order to maintain a local mirror
of your transactions log for processing.


## Identifier Fields

`id`: A unique integer id for each transactions. These are distinct even within
linked orders for spreads. You can rely on those for uniqueness. However, they
do not appear to be strictly increasing; in other words, if you sort by
`executed-at`, the corresponding `id` fields are only approximately in
increasing order.

`account-number`: Is your full, non-anonymized account number with eight
characters (typically three alpha-numerical ones and 5 numerical digits).


## Categorization Fields

Each row has a particular type and sub-type categorization. The corresponding
fields are:

* `transaction-type`: The top-level category for rows.
* `transaction-sub-type`: The subcategory.
* `action`: An instruction. It's called action now because it's also used for
  expirations and such.

There are possible values for `transaction-type` in the log:

* `Money Movement`: These are for transfers, deposits, withdrawals, dividends
  and all other forms of adjustments. This is an effect on the cash account, and
  has no effect on the positions. The available subtypes are:

  * `Balance Adjustment`:
  * `Credit Interest`:
  * `Mark to Market`:
  * `Transfer`:
  * `Withdrawal`:
  * `Deposit`:
  * `Cash`:
  * `Fee`:

  The `action` field is always unset.

* `Trade`: A trade carried out as a result of instruction to the broker. There
  are four sub-types for instrument types `Equity`, `Equity Option`, and `Future
  Option`:

  * `Buy to Open`: A purchase of an instrument opening a new position.
  * `Buy to Close`: A purchase of an instrument closing an existing position.
  * `Sell to Open`: A sale of an instrument opening a new position.
  * `Sell to Close`: A sale of an instrument closing an existing position.

  For `Future` instruments, there is no tracking of opening and closing state
  (we will fix this upstream ourselves):

  * `Buy`: A purchase of a futures contract, opening or closing (or both).
  * `Sell`: A purchase of a futures contract, opening or closing (or both).

  In post-processing, we will compute the inventory state of the account and
  insert a opening/closing field, potentially splitting a few if necessary
  (i.e., if the position crosses the zero line).

  The `action` field is always set to the same value as the subtype.

* `Receive Deliver`: An action changing the positions ("receiving" or
  "delivering" a product), but not as a direct result of an instruction. This is
  used for reporitng expirations, assignments, exercises, and more. Basically,
  if it's not a trade and it affects the positions, the row is of this
  transaction type. Possible subtypes are:

  * `Expiration`: Options contracts expiring OTM. (Unfortunately the `quantity`
    field is set but does not provide an indication as to whether we're expiring
    a long or short position, so in order to process this, you still have to
    accumulate state. We will fix this ourselves in post-processing.)

  * `Assignment`: Options contracts being assigned, expiring ITM and resulting
    in a position in the underlying.

  * `Exercise`: An options exercise, as per instruction.

  * `Cash Settled Assignment`: Options contracts expiring ITM and settled in cash.

  * `Cash Settled Exercise`: (?)

  * `Symbol Change`: When an underlying changes name, a simulated closing and
    opening is performed. Two distinct rows will be found, one with `action`
    field `X to Close` and the other with `X to Open`.

  * `ACAT`: When instruments are transfered in-kind from another account as a
    result of a direct (ACAT) transfer, transactions will appear and the
    `action` field wither `Buy to Open` or `Sell to Open`.


## Order & Trade Fields

For trades, some details about the order are provided:

`order-id`: A unique integer identifier for each order, for example,
`153026413`. These are shared by all transactions that were placed jointly, and
are not unique for each row. You can use this to reconstruct how an order was
placed.

`leg-count`: An integer, the number of other legs in the order, e.g., `2`. I've
only seen values from 1 to 4.

`price`: A decimal number, the per-unique price of the execution, e.g. `0.37`.
The precision is complete and dependent on the underlying.

`quantity`: A decimal, The number of units of the `symbol` bought or sold, e.g.
`8.0`


## Date/time Fields

`executed-at`:  This field is the main date/time field for each entry, in
ISO8601 UTC format (including the final `Z`, like
`2021-06-02T18:49:45.075+00:00`). Note that it is not unique; placing an order
for a spread that will be filled by a market-market will result in multiple rows
with identical `executed-at` times, so you cannot use this as the key. Also,
adjustments and mark-to-market events will occur on the hour and overlap with
other ones.

`transaction-date`: This is the transaction date, also in ISO8601 format, e.g.
`2021-06-02`. WARNING: The date appears incorrect at least some of the time
though, and it's not a timezone conversion issue: I'm located in the EST
timezone (New York) and I have a transaction with an `executed-at` value of
`2021-06-02T22:17:25.873+00:00` which is an 18:17 time on the same time, the
Java app shows it as such, and the `transaction-date` value is incorrectly
`2021-06-03`. I recommend ignoring this field.


## Instrument Fields

`instrument-type`: The type of instrument. Valid values include

    Equity Option
    Future Option
    Equity
    Future
    Cryptocurrency

`symbol`: An unpadded, fixed-length representation of the instrument being
traded. These are parsed distinctly for each instrument type.

Here are some examples of symbols for `Equity`:

    'NKE'
    'PLTR'

Examples for `Equity Option`, which equivalently includes ETFs:

    'AAPL  210716P00110000'
    'ADBE  210618P00520000'
    'ARKK  210618P00093000'
    'BIDU  210604P00185000'
    'BLOK  210618C00050000'
    'BYND  210716P00100000'
    'COIN  210618P00250000'
    'CRWD  210416P00160000'
    'DOCU  210604P00212500'
    'EEM   210716C00057000'
    'FCX   210521P00035000'
    'FSLR  210820P00082500'
    'GM    210730C00061000'
    'GPS   210618C00041000'
    'HRB   210618C00026000'
    'LAZR  210618C00025000'
    'MRVL  210618C00055000'
    'NVDA  210716C00855000'
    'QQQ   210716P00337000'
    'SKLZ  210521P00012500'
    'SPCE  210618P00029000'
    'SPY   210618C00437000'
    'STX   210716C00140000'
    'TSLA  210430C00900000'
    'VFC   210521P00080000'
    'X     210521P00020000'
    'XLE   210730P00049000'

Examples for `Future Option`:

    './6AM1 ADUK1 210507P0.745'
    './6CM1 CAUM1 210604P0.79'
    './6CU1 CAUQ1 210806P0.785'
    './6EU1 EUUN1 210709C1.225'
    './6JM1 JPUM1 210604C0.009225'
    './6JU1 JPUQ1 210806C0.0092'
    './CLN1 LON1  210617C72.5'
    './CLN1 LON1  210617C82.5'
    './CLU1 LOU1  210817C77.5'
    './ESM1 ESM1  210618P3900'
    './GCQ1 OGQ1  210727P1800'
    './NGM1 LNEM1 210525C3.06'
    './NGQ1 LNEQ1 210727P2.9'
    './NQM1 NQM1  210618P11940'
    './RTYM1RTOM1 210618C2460'
    './RTYU1R3EN1 210716C2370'
    './ZCN1 OZCM1 210521C625'
    './ZCN1 OZCN1 210625C840'
    './ZNU1 OZNN1 210625C138'

Examples for `Future`:

    '/GEZ1'
    '/VXM21'
    '/VXN21'
    '/ZTU1'

Note that futures symbols aren't annualized (these need to be normalized for
longer-term logs).

`underlying-symbol`: For options, a separate field for just the name of the
corresponding underlying is provided. For example, `NTAP` or `/6JU1`. Note that
for `Future Option`, the underlyings will include the contract month:

    '/6AM1'
    '/6CM1'
    '/6CU1'
    '/6EM1'
    '/6EU1'
    '/6JM1'
    '/6JU1'
    '/CLM1'
    '/CLN1'
    '/CLQ1'
    '/CLU1'
    '/ESM1'
    '/ESU1'
    '/GCQ1'

For future `Future` trades on the outrights, the `underlying-symbol` includes
only the uncalendared product:

    '/GE'
    '/VX'
    '/ZT'


## Balance Affecting Numerical Fields

The following fields are provided that affect the balance:

    commission
    clearing-fees
    proprietary-index-option-fees
    regulatory-fees
    value

There all have positive decimal values. Each of those fields is accompanied by a
corresponding `*-effect` field which provides the effective sign, whose value is
either `Credit`, `Debit` or `None`. All numerical values are unsigned and
`Debit` is for a negative value. When the effect is `None`, the corresponding
numerical field is always `0.0`. Here's an example:

    'commission': '8.0'
    'commission-effect': 'Debit'
    'clearing-fees': '0.8'
    'clearing-fees-effect': 'Debit'
    'proprietary-index-option-fees': '0.0'
    'proprietary-index-option-fees-effect': 'None'
    'regulatory-fees': '0.306'
    'regulatory-fees-effect': 'Debit'
    'value': '296.0'
    'value-effect': 'Credit'
    'net-value': '286.894'
    'net-value-effect': 'Credit'


The fees are broken down into three buckets; you can calculate the total fees
like this:

  `fees` = `clearing-fees` + `regulatory-fees` + `proprietary-index-option-fees`

Furthermore, th following invariant always hold:

  `net-value` = `value` + `commission` + `fees`

So accumulating `net-value` over the transaction rows provides the ongoing cash
balance of your account. In addition, there is one more related field:

`is-estimated-fee`: A boolean field is provided to mark whether the fees are
estimated or precise. As far as I can tell, this is always `True`.


## Exchange/Venue Fields

`exchange`: Exchange and venue related codes. These are INET style single-letter
codes for exchanges that have been extended to up to three-letter codes, and
also includes digits. Sometimes the value is empty. Here are some of the ones
I've seen, with a guess as to the venue each might represent:

        : Chicago Mercantile Exchange (CME)
    1   :
    64  :
    8   :
    84  :
    A   :
    AME :
    B   :
    BAT : BATS Global Markets
    BOX : Boston Options Exchange
    BXO :
    C   :
    C20 :
    C2O :
    D   :
    E   :
    EDG : Direct Edge (CBOE US Equities)
    EML :
    GMN :
    H   :
    ISE : Nasdaq ISE
    M   :
    MCR :
    MIO :
    MPR :
    NSD : Nasdaq
    P   :
    PEA :
    PHL : Philadelphia Stock Exchange
    PSE :
    Q   :
    R   :
    T   :
    W   :
    X   :
    XAS :
    XC  : CBOE /VX Options
    XCB :
    XIS :
    XMI :
    XPH :
    XPS :
    Y   :
    Z   :

`exchange-affiliation-identifier`: This is always empty as far as I can tell,
and probably an internal category detail of the relationship of TW to their
broker.

Various exchange and venue-specific identifiers. These probably need not be
exported as they are quite unlikely to get exposed through any user-level UI,
but possibly the support desk needs to look these up on a support call so
they're likely in the same schema. These show up like this:

    'exec-id': '4100O1500900725'
    'ext-exchange-order-number': '64655265300938605'
    'ext-exec-id': '900725'
    'ext-global-order-number': 15053727
    'ext-group-fill-id': '177692073'
    'ext-group-id': '0'


## Other Fields

`description`: There is a generate text description field that is provided as
well. You could synthesize this yourself, but it's what is rendered in the log,
e.g.. 'Sold 8 NTAP 06/04/21 Put 70.50 @ 0.37'. This is intended to be read by
humans. The level of detail in that field varies across instrument types types.
