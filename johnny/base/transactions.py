"""Common code to process and validate transactions logs."""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from decimal import Decimal
from typing import Callable, Tuple
import collections
import datetime
import functools
import enum
import traceback

import pyarrow as pa
import pyarrow.parquet as pq

from johnny.base import checks
from johnny.base.etl import Record, Table
from johnny.base.transactions_pb2 import Transaction


Type = enum.StrEnum("Type", {k: k for k in Transaction.RowType.keys()})


GetFn = Callable[[str], Tuple[Table, Table]]
ParserFn = Callable[[str], Table]
ZERO = Decimal(0)


# TODO(blais): Remove this and replace by inspection of the proto.
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


# Valid effect types. The empty string is used to indicate "unknown".
EFFECT = {"OPENING", "CLOSING", ""}

# Valid instructions.
INSTRUCTION = {"BUY", "SELL", ""}


def ValidateFieldNames(table: Table):
    """Validate the field names and their order."""
    if list(table.header())[: len(FIELDS)] != FIELDS:
        raise ValidationError("Invalid field names on table:\n{}".format(table))


def ValidateRecord(r: Record):
    """Validate the transactions log from a source for datatypes and conformance.
    See `transactions.md` file for details on the specification and expectations
    from the converters."""

    checks.AssertEnum(Transaction.RowType, r.rowtype)
    checks.AssertString(r.account)
    checks.AssertDateTime(r.datetime)
    checks.AssertString(r.description)
    checks.AssertString(r.symbol)
    checks.AssertValidSymbol(r.symbol)
    checks.AssertString(r.transaction_id)
    checks.AssertOptionalString(r.order_id)

    checks.AssertOptionalEnum(Transaction.PositionEffect, r.effect)
    checks.AssertOptionalEnum(Transaction.Instruction, r.instruction)

    # A quantity of 'None' is allowed if the logs don't include the expiration
    # quantity, and is filled in automatically by matching code.
    checks.AssertOptionalPositiveDecimal(r.quantity)
    checks.AssertDecimal(r.price)
    checks.AssertDecimal(r.cost)
    checks.AssertDecimal(r.cash)
    checks.AssertDecimal(r.commissions)
    checks.AssertDecimal(r.fees)

    # Check that dividends don't have trade information.
    if r.rowtype == Type.Cash:
        assert r.quantity == ZERO
        assert r.price == ZERO
        assert r.cost == ZERO

    # We could tighten the constraints further and require unique transactions
    # vs. order ids mutually, but it's probably not necessary.
    assert (
        r.transaction_id != r.order_id
    ), f"Transaction vs. Order ids not mutually excluive: {r.transaction_id} = {r.order_id}"


def Validate(transactions: Table):
    """Check that the imports are sound before we process them and ensure that
    the transaction ids are unique.
    """
    unique_ids = collections.defaultdict(int)
    num_txns = 0
    try:
        for rec in transactions.records():
            unique_ids[rec.transaction_id] += 1
            num_txns += 1
            ValidateRecord(rec)
    except Exception as exc:
        traceback.print_exc()
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
