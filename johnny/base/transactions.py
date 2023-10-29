"""Common code to process and validate transactions logs."""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from decimal import Decimal
from typing import Callable, Tuple
import collections
import datetime
import functools

import pyarrow as pa
import pyarrow.parquet as pq

from johnny.base.etl import Record, Table
from johnny.base.transactions_pb2 import Transaction


GetFn = Callable[[str], Tuple[Table, Table]]
ParserFn = Callable[[str], Table]
ZERO = Decimal(0)


# Transaction table field names.
FIELDS = [
    # Event info
    "account",
    "transaction_id",
    "datetime",
    "rowtype",
    "order_id",
    # Instrument info
    "symbol",
    # Balance info
    "effect",
    "instruction",
    "quantity",
    "price",
    "cost",
    "cash",
    "commissions",
    "fees",
    # Descriptive info
    "description",
    # Extra info.
    "init",
]


class ValidationError(Exception):
    """Conformance for transactions table. Check your importer."""


def IsZoneAware(d: datetime.datetime) -> bool:
    """Return true if the time is timezone aware."""
    return d.tzinfo is not None and d.tzinfo.utcoffset(d) is not None


# Valid effect types. The empty string is used to indicate "unknown".
EFFECT = {"OPENING", "CLOSING", ""}

# Valid instructions.
INSTRUCTION = {"BUY", "SELL", ""}


def ValidateFieldNames(table: Table):
    """Validate the field names and their order."""
    if list(table.header())[: len(FIELDS)] != FIELDS:
        raise ValidationError("Invalid field names on table:\n{}".format(table))


def ValidateTransactionRecord(r: Record):
    """Validate the transactions log from a source for datatypes and conformance.
    See `transactions.md` file for details on the specification and expectations
    from the converters."""

    assert r.account and isinstance(r.account, str)

    assert r.transaction_id and isinstance(r.transaction_id, str), r
    assert r.order_id and isinstance(r.order_id, str) or r.order_id in {"", None}, r

    # We could tighten the constraints further and require unique transactions
    # vs. order ids mutually, but it's probably not necessary.
    # assert r.transaction_id != r.order_id, r

    assert isinstance(r.datetime, datetime.datetime)
    assert not IsZoneAware(r.datetime)
    assert r.rowtype in Transaction.RowType.keys()
    assert r.effect in EFFECT or r.effect == "", r
    assert r.instruction in INSTRUCTION or r.instruction == "", r

    # Check the normalized symbol.
    assert r.symbol and isinstance(r.symbol, str)
    # TODO(blais): Parse the symbol to ensure it's right.
    ## assert instrument.Parse(r.symbol)

    # A quantity of 'None' is allowed if the logs don't include the expiration
    # quantity, and is filled in automatically by matching code.
    assert r.quantity is None or (
        isinstance(r.quantity, Decimal) and r.quantity >= ZERO
    ), r
    assert isinstance(r.price, Decimal)
    assert isinstance(r.cost, Decimal)
    assert isinstance(r.cash, Decimal)
    assert isinstance(r.commissions, Decimal)
    assert isinstance(r.fees, Decimal)

    assert isinstance(r.description, str)

    if r.rowtype == "Dividend":
        assert not r.quantity
        assert not r.price
        assert not r.cost


def ValidateTransactions(transactions: Table):
    """Check that the imports are sound before we process them and ensure that
    the transaction ids are unique.
    """
    unique_ids = collections.defaultdict(int)
    num_txns = 0
    try:
        for rec in transactions.records():
            unique_ids[rec.transaction_id] += 1
            num_txns += 1
            ValidateTransactionRecord(rec)
    except Exception as exc:
        if force:
            traceback.print_last()
        else:
            raise
    if num_txns != len(unique_ids):
        for key, value in unique_ids.items():
            if value > 1:
                print("Duplicate id '{}', {} times".format(key, value))
        raise AssertionError(
            "Transaction ids aren't unique: {} txns != {} txns".format(
                num_txns, len(unique_ids)
            )
        )


def ToParquet(transactions: Table, filename: str):
    """Write a transactions table to Parquet.

    This is used because we have to convert all the data types.
    """

    # Note: We use pyarrow directly instead of polars/duckdb/pandas in order to
    # avoid conversions of the type systems.
    def number(name, *args):
        # return pa.field(name, pa.decimal128(*args), nullable=True)
        return pa.field(name, pa.float64(), nullable=True)
        # binary?

    def string(name):
        return pa.field(name, pa.string(), nullable=False)

    def opt_string(name):
        return pa.field(name, pa.string(), nullable=True)

    def datetime(name):
        return pa.field(name, pa.timestamp("s"), nullable=False)

    def bool_(name):
        return pa.field(name, pa.bool_(), nullable=False)

    def enum(name):
        return pa.field(name, pa.dictionary(pa.int8(), pa.string()), nullable=False)

    fields = [
        string("account"),
        string("transaction_id"),
        datetime("datetime"),
        enum("rowtype"),
        opt_string("order_id"),
        string("symbol"),
        enum("effect"),
        enum("instruction"),
        number("quantity", 16, 2),
        number("price", 16, 8),
        number("cost", 16, 8),
        number("cash", 16, 2),
        number("commissions", 16, 8),
        number("fees", 16, 6),
        string("description"),
        string("match_id"),
        string("chain_id"),
        bool_("init"),
    ]
    schema = pa.schema(fields)
    num_fields = [c.name for c in fields if pa.types.is_floating(c.type)]

    # Note: Problems:
    # - Empty values for order_id if you don't make the column nullable doesn't
    #   fail to store but creates an invalid file.
    # - Storing as Decimal doesn't keep the variable precision information.
    if num_fields:
        transactions = transactions.convert(num_fields, float)
    data = transactions.cut(*schema.names).dicts()

    table = pa.Table.from_pylist(data, schema=schema)
    with pq.ParquetWriter(filename, schema) as writer:
        writer.write_table(table)

    # df = transactions.todataframe()
    # df.to_parquet("/tmp/transactions2.parquet", index=False)
