"""Interactive Brokers - Parse positions flex report.
"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"


import click

from johnny.base import config as configlib
from johnny.base import discovery
from johnny.base import positions as poslib
from johnny.base.etl import Table
from johnny.base.number import ToDecimal
from johnny.sources.interactive_csv import transactions


def GetPositions(filename: str) -> Table:
    """Process the filename, normalize, and produce tables."""

    tablemap = transactions.SplitTables(filename)
    assert set(tablemap) == {"POST"}

    table = (
        tablemap["POST"]

        .cut('ClientAccountID', 'AssetClass', 'Symbol', 'Description', 'Quantity',
             'MarkPrice', 'PositionValue', 'OpenPrice', 'CostBasisMoney',
             'FifoPnlUnrealized', 'Side')

        # Clean up account name to match that from the transactions log.
        .rename("ClientAccountID", "account")
        .convert("account", lambda aid: "x{}".format(aid[3:5]))

        # Instrument type.
        .rename("AssetClass", "instype")
        .convert("instype", {'STK': 'Equity'})
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

        .cut(poslib.FIELDS)
    )

    return table


def Import(source: str, _: configlib.Config) -> Table:
    """Process the filename, normalize, and output as a table."""
    filename = discovery.GetLatestFile(source)
    return GetPositions(filename)


@click.command()
@click.argument("filename", type=click.Path(resolve_path=True, exists=True))
def main(filename: str):
    """Simple local runner for this translator."""
    print(GetPositions(filename).lookallstr())


if __name__ == "__main__":
    main()
