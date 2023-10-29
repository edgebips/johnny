"""Processing of non-trades for Tastytrade."""

import shelve
from os import path

from johnny.base.etl import petl
from johnny.sources.tastytrade import config_pb2
from johnny.sources.tastytrade import transactions
import johnny.base.nontrades as nontradeslib


NONTRADE_TYPES = {
    ("Money Movement", "Balance Adjustment"): nontradeslib.Type.Adjustment,
    ("Money Movement", "Credit Interest"): nontradeslib.Type.BalanceInterest,
    # Note: This contains amounts affecting balance for futures.
    ("Money Movement", "Mark to Market"): nontradeslib.Type.FuturesMarkToMarket,
    ("Money Movement", "Transfer"): nontradeslib.Type.InternalTransfer,
    ("Money Movement", "Withdrawal"): nontradeslib.Type.ExternalTransfer,
    ("Money Movement", "Deposit"): nontradeslib.Type.ExternalTransfer,
    ("Money Movement", "Fee"): nontradeslib.Type.TransferFee,
}


def ImportNonTrades(config: config_pb2.Config) -> petl.Table:
    # Convert numerical fields to decimals.
    filename = path.expandvars(config.dbm_filename)
    db = shelve.open(filename, "r")
    items = transactions.PreprocessTransactions(db.items())

    # Filter rows that we care about. Note that this removes mark-to-market
    # entries.
    table = (
        petl.fromdicts(items)
        # Add row type and filter out the row types we're not interested
        # in.
        .addfield("rowtype", transactions.GetRowType).selectin(
            "rowtype", set(NONTRADE_TYPES.values())
        )
    )

    # Remove some useless rows.
    remove_cols = [
        "account-number",
        "action",
        "regulatory-fees",
        "clearing-fees",
        "commission",
        "proprietary-index-option-fees",
        "is-estimated-fee",
        "ext-exchange-order-number",
        "ext-global-order-number",
        "ext-group-id",
        "ext-group-fill-id",
        "ext-exec-id",
        "exec-id",
        "exchange",
        "order-id",
        "exchange-affiliation-identifier",
        "leg-count",
        "destination-venue",
    ]
    removed = table.cut(*remove_cols).distinct()
    assert removed.nrows() == 1, "Rows removed aren't all vacuous."
    table = table.cutout(*remove_cols)

    # Add datetime.
    table = (
        table.addfield("datetime", transactions.ParseTime)
        .rename("transaction-sub-type", "nativetype")
        .addfield("account", None)
        .rename("id", "transaction_id")
        .convert("transaction_id", str)
        .addfield("ref", None)
        .rename("value", "amount")
        .addfield("balance", None)
        .cut(nontradeslib.FIELDS)
    )

    return table.sort("rowtype")
