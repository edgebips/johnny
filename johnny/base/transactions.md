# Transactions Log Table

## Opening and Closing

The transactions log is not assumed to include opening transactions, nor closing
(mark-to-market) transactions. These are synthesizes in coordinations with a
positions table.


## Field Names

A normalized transactions table contains the following columns and data types.

### Information about the Event

- `account: str`: A unique identifier for the account number. This implicitly defines
  the brokerage. This can be used if a file contains information about multiple
  accounts. The configuration can map this to a public nickname that may or may
  not reflect the source.

- `transaction_id: str`: A unique transaction id by which we can identify this
  transaction. This is essential in order to deduplicate previously imported
  transactions. This can be given from the system or synthesized from a stable
  hash from the rows of an input file.

- `datetime: datetime.datetime`: A date/time converted to local time and naive
  (no timezone, not "timezone aware"). This is the date and time at which the
  transaction occurred. This is distinct from the settlement date (which is not
  provided by this data structure).

- `rowtype: str`: An enum string for the row, whether this is a trade, an
    expiration, or a mark-to-market synthetic close (used to value currency
    positions). The value will be one of

  * `Trade` (a purchase or sale),
  * `Expire` (an expiration, a receive and deliver type of event),
  * `Mark` (a virtual sale).
  * `Open` (a virtual transaction to create an opening balance)

  `Mark` is never inserted by the normalization code, that's something that is
  inserted by further processing code.

- `order_id: str`: The order id used for the transaction, if there was an order.
  May include alphabetical characters. This is used to join together multiple
  transactions that were issued jointly, e.g. a spread, an iron condor, a pairs
  trade, etc. Note that orders issued as "blast all" will usually have distinct
  order ids (and our solution typically involves adding another type/id column
  to join together pairs trades). Expirations don't have orders, so will remain
  normally unset. This will be set to None if not available.


### Information about the Financial Instrument

- `symbol: str`: The symbol normalized in unambiguous, cross-platform, readable
  symbology. The symbol for the underlying instrument. This may be an equity,
  equity option, futures, futures option or spot currency pair.

  The individual fields of the instrument's symbol may be expanded into their
  individual components. See the file "instrument.md" for details.


### Information Affecting the Balance

- `effect: str`: The effect on the position, either `OPENING` or `CLOSING`, or
  `?`. For futures contracts, the value is not usually known, it will be
  inferred later, from processing the entire file from the initial positions.

  If it is not known, state-based code providing and updating the state of
  inventories is required to sort out whether this will cause an increase or
  decrease of the position automatically. Ideally include the state if you have
  it available. If not, set the value to `?`.

- `instruction: Optional[str]`: An enum, `BUY`, `SELL`. If this is an
  expiration, this can be left unset and inferred automatically by the matching
  code.

- `quantity: Decimal`: The quantity bought or sold. This number should always be
  positive; the 'instruction' field will provide the sign.

- `price: Decimal`: The per-contract price for the instrument. Multiply this by
  the `quantity` and the `multiplier` to get the `cost`.

- `cost: Decimal`: The dollar amount of the position change minus commissions
  and fees. This is a signed number.

- `commissions: Decimal`: The dollar amount of commissions charged. This is a
  signed number, usually negative. These are the fees paid to the broker for
  service.

- `fees: Decimal`: The total dollar amount paid for exchange and other
  regulatory institution fees. This is a signed number, usually negative. Note
  that this could be broken down into multiple components, but we do not bother
  with this here, as this is not something we have control over, will not affect
  our trading, and is often just not available from CSV downloads.


### Descriptive information

- `description: str`: An optional free-form description string describing the
  transaction, if one is available. This is used for rendering debugging outputs
  and for rendering transactions in accounting systems. If not set, leave an
  empty string (not `None`).


### Chaining Information

- `match_id: str`: A unique random id which links together transactions reducing
  each other. A buy of a particular option, followed by a partial sell, and a
  final sell, would all share the same match id. This maps closing transactions
  to their corresponding opening ones.

  This is normally not provided by the importers, and is filled in later by
  analysis code. The field is always set, even if there's only an opening
  transaction.

  (Note the inherent conflict in the 1:1 relationship here: a single transaction
  may close multiple opening ones and vice-versa. In order to make this a 1:1
  match. In theory we would have to split one or both of the opening/closing
  sides.)

- `chain_id`: A unique random id which links together transactions grouped
  together in a sequence of events. A "trade", or "chain" ofrelated transactions
  over time. For instance, selling a strangle, then closing one side, and
  rolling the other side, and then closing, could be considered a single chain
  of events.

  Similar to the match id, this is left empty by the converters and filled in
  later on by analysis code.
