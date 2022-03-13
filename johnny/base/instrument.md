# Canonical Instrument Representation

We define a symbology for all instruments that is both independent of all the
ones used by various brokers and which is also much easier to parse and read.


## Terminology

We call an "underlying" the financial product that we trade or that we trade a
derivative upon. For futures contracts, an underlying is defined to include its
expiration month, for example, `/ZCN21`. We call the contract type without its
month the "product", e.g., `/ZC`.


## Component Fields

The following fields are optional, and can be entirely inferred from the
`symbol` field alone. These can be expanded from the symbol when specific
processing of its components is required, e.g., when the `strike` field is
needed on its own.

- `instype: str`: The instrument type, an enum with the following possible
  values:

  * `Equity`
  * `EquityOption`
  * `Future`
  * `FutureOption`
  * `Index` (not tradeable)
  * `IndexOption`
  * `Crypto`

- `underlying: str`: The underlying instrument, with normalized name. If this is
  a futures instrument, includes the calendar month (and is normalized to
  include the decade as well).

- `expcode: Optional[str]`: The expiration date of an option. If this is an
  option on a future, the corresponding option expiration code, e.g. `LOM21` for
  `/CLM21`.

- `expiration: Optional[datetime.date]`: The expiration date of an option. If
  this is an option on a future, this may not be present and need be inferred
  separately (insert it if you have it).

- `putcall: Optional[str]`: If an option, `CALL` or `PUT`

- `strike: Optional[Decimal]`: The strike price of an option (Decimal).

- `multiplier: Decimal`: A multiplier for the contract, i.e., the contract size.
  Some products require a fractional one unfortunately, e.g. `/BTC`, `/MYM`.

  This is a multiplier for the quantity. For equities, this is 1. For equity
  options, it should be set to 100. For futures contracts, set to whatever the
  multiplier for the contract is. (These values are static and technically are
  inferred automatically from the underlying and instrument type.)

The currency that the instrument is quoted in is not included; we assume the US
dollar is the quoting currency so far.


## Examples

Equity:

    TSLA

Future:

    /CLM21

EquityOption:

    SLV_210319_C25.5

FutureOption:

    /CLM21_LOM21_C65
