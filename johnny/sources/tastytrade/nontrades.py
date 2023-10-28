"""Processing of non-trades for Tastytrade."""

from os import path

from johnny.base.etl import petl
from johnny.sources.tastytrade import config_pb2
from johnny.sources.tastytrade import transactions
import johnny.base.nontrades as nontradeslib


def ImportNonTrades(config: config_pb2.Config) -> petl.Table:
    table = transactions.GetOther(path.expandvars(config.dbm_filename))

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
        .rename(
            {
                "transaction-type": "orig_type",
                "transaction-sub-type": "orig_subtype",
            }
        )
        .addfield("account", None)
        .rename("id", "transaction_id")
        .rename("value", "amount")
        .addfield("balance", None)
        .cut(nontradeslib.FIELDS)
    )

    return table.sort("rowtype")
