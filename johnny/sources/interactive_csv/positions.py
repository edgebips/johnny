"""Interactive Brokers - Parse positions flex report.
"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from decimal import Decimal
from os import path
from typing import Dict, List, Optional
import bisect
import collections
import csv
import datetime as dt
import logging

from dateutil import parser
import click

from johnny.base.config import Account
from johnny.base import config as configlib
from johnny.base import discovery
from johnny.base import positions as poslib
from johnny.base.etl import petl, Record, Table
from johnny.base.number import ToDecimal
from johnny.sources.interactive_csv import transactions
from johnny.sources.interactive_csv import config_pb2


ZERO = Decimal(0)
ONE = Decimal(1)
Q4 = Decimal("0.0001")


def GetOptionDelta(r: Record) -> Decimal:
    """Set the option delta to 1 if a stock position, fail otherwise."""
    if r["instype"] in {"Equity", "Future"}:
        return ONE
    raise ValueError(
        "Not enough information from InteractiveBrokers to infer option delta "
        "for options."
    )


def GetSortedValues(
    values: config_pb2.DatedValue,
) -> Dict[str, List[config_pb2.DatedValue]]:
    m = collections.defaultdict(list)
    for v in values:
        m[v.symbol].append((parser.parse(v.date).date(), v.value))
    for values in m.values():
        values.sort()
    return dict(m)


def GetSortedValue(
    svalues: Dict[str, List[config_pb2.DatedValue]], date: dt.date, underlying: str
):
    try:
        date_values = svalues[underlying]
    except KeyError:
        logging.error(
            f"No values available for '{underlying}'; "
            "consider using config.betas or config.index_prices"
        )
        return float("nan")
    else:
        index = min(len(date_values)-1, bisect.bisect(date_values, (date,)))
        return Decimal(date_values[index][1]).quantize(Q4)


def GetLatestDate(filename: str) -> dt.date:
    """Get the date the file was produced, from its first row."""
    with open(filename) as f:
        line = next(iter(csv.reader(f)))
        return parser.parse(line[5]).date()


def GetPositions(filename: str, fallback_database) -> Table:
    """Process the filename, normalize, and produce tables."""

    tablemap = transactions.SplitTables(filename)
    assert set(tablemap) == {"POST"}

    # Prepare lookup tables.
    betas = GetSortedValues(fallback_database.betas)
    index_prices = GetSortedValues(fallback_database.index_prices)

    date = GetLatestDate(filename)

    table = (
        tablemap["POST"]
        .cut(
            "ClientAccountID",
            "AssetClass",
            "Symbol",
            "Description",
            "Quantity",
            "MarkPrice",
            "PositionValue",
            "OpenPrice",
            "CostBasisMoney",
            "FifoPnlUnrealized",
            "Side",
        )
        # Clean up account name to match that from the transactions log.
        .rename("ClientAccountID", "account")
        .convert("account", lambda aid: "x{}".format(aid[3:5]))
        # Instrument type.
        .rename("AssetClass", "instype")
        .convert("instype", {"STK": "Equity"})
        .rename("Symbol", "symbol")
        .rename("Description", "description")
        # Quantity.
        .rename("Quantity", "quantity")
        .convert("quantity", ToDecimal)
        # Prices.
        .rename("OpenPrice", "price")
        .convert("price", ToDecimal)
        .rename("MarkPrice", "mark")
        .convert("mark", ToDecimal)
        .rename("CostBasisMoney", "cost")
        .convert("cost", ToDecimal)
        .rename("PositionValue", "net_liq")
        .convert("net_liq", ToDecimal)
        # Add superfluous fields.
        .addfield("group", "")
        # Add dollar-delta and beta related fields.
        .addfield("unit_delta", GetOptionDelta)
        .addfield("beta", lambda r: GetSortedValue(betas, date, r["symbol"]))
        .addfield("index_price", lambda r: GetSortedValue(index_prices, date, "SPY"))
        .cut(poslib.FIELDS)
    )

    return table


def Import(source: str, config: configlib.Config, logtype: "LogType") -> Table:
    """Process the filename, normalize, and output as a table."""
    filename = discovery.GetLatestFile(source)
    positions = GetPositions(filename, config.fallback_database)
    return {Account.POSITIONS: positions}[logtype]


def ImportPositions(config: config_pb2.Config) -> petl.Table:
    pattern = path.expandvars(config.positions_flex_report_csv_file_pattern)
    filename = discovery.GetLatestFile(pattern)
    return GetPositions(filename, config.fallback_database)



@click.command()
@click.argument("filename", type=click.Path(resolve_path=True, exists=True))
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    help="Configuration filename. Default to $JOHNNY_CONFIG",
)
def main(filename: str, config: Optional[str]):
    """Simple local runner for this translator."""

    # Read the input configuration.
    config_filename = configlib.GetConfigFilenameWithDefaults(config)
    config = configlib.ParseFile(config_filename)

    print(GetPositions(filename, config).lookallstr())


if __name__ == "__main__":
    main()
