# Non-Trades Table

This is a table intended to support a normalized format for all the non-trade
rows to be converted into some other accounting system, like Beancount.

## Row Types

- `rowtype: str`: An enum string for the row type.

  * `CashBalance`: A balance of the cash account
  * `FuturesBalance`: A balance of the futures account.
  * `Adjustment`: A balance adjustment (of many types)
  * `FuturesMTM`: A mark-to-mark futures transfer.
  * `BalanceInterest`: Interest on balance.
  * `MarginInterest`: Interest on margin.
  * `Dividend`: A dividend received.
  * `Distribution`: A capital gains distribution, typically from an ETF.
  * `TransferIn`: An incoming transfer.
  * `TransferOut`: An outgoing transfer.
  * `TransferInternal`: An internal transfer.
  * `MonthlyFee`: A monthly fee.
  * `TransferFee`: A wire transfer fee.
  * `HTBFee`: A hard-to-borrow fee.
  * `Sweep`: A cash sweep (typically to be ignored).

## Information about the Event

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
  row was logged, in local time. Where only a date is included, the time is set
  to midnight that day.

## Descriptive information

- `description: str`: An optional free-form description string describing the
  transaction, if one is available. This is used for rendering debugging outputs
  and for rendering transactions in accounting systems. If not set, leave an
  empty string (not `None`).

- `type: Optional[str]`: The original type fields of the imported data.

- `symbol: Optional[str]`: If applicable, the symbol normalized in unambiguous,
  cross-platform, readable symbology. The individual fields of the instrument's
  symbol may be expanded into their individual components. See the file
  "instrument.md" for details.

- `ref: Optional[str]`: An optional reference number.

## Balance-Affecting Fields

- `amount: Decimal`: The signed amount being applied to the account balance.

- `balance: Optional[Decimal]`: An optional resulting balance field of the
  corresponding account after the amount is applied.
