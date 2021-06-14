#!/usr/bin/env python3
"""Compute total portfolio greeks and notional exposure.

This script parses the "Position Statementa" CSV file download you can export
from ThinkOrSwim. This offers a manually triggered "poor man's" risk monitor,
which you can use for spot checks over your entire portfolio. (Unfortunately, TD
does not provide you with risk management tools so this is the basic thing you
can do to get a sense of aggregate portfolio risk.)

Assumptions:

- You have groups of assets, and have "Show Groups" enabled.
- You have inserted the columns listed under 'FIELDS' below, including the
  various greeks and notional values to sum over.

Instructions:

- Go to the "Monitor >> Activity and Positions" tab.
- Turn on "Beta Weighting" on your market proxy of choice (e.g. SPY).
- Expand all the sections with the double-down arrows (somehow the export only
  outputs the expanded groups).
- Click on "Position Statement" hamburger menu and "Export to File...".
- Run the script with the given file.

The output will include:

- A consolidated position detail table.
- A table with the extrema for each greek or columns.
"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from os import path
import argparse
from decimal import Decimal
import itertools
import logging
import re
from typing import List, Tuple, Optional, NamedTuple

from dateutil.parser import parse
import click

from johnny.broker.ameritrade import utils
from johnny.base import positions as poslib
from johnny.base import futures
from johnny.base import instrument
from johnny.base.number import ToDecimal
from johnny.base.etl import petl, WrapRecords


Table = petl.Table
Record = petl.Record
Q = Decimal("0.01")
ZERO = Decimal(0)


# The set of fields to produce aggregations over.
FIELDS = ['Delta', 'Gamma', 'Theta', 'Vega', 'Beta', 'Net Liq', 'P/L Open', 'P/L Day']


def ParseNumber(string: str) -> Decimal:
    """Parse a single number string."""
    if string in {'N/A', 'N/A (Split Position)'}:
        return Decimal('0')
    sign = 1
    match = re.match(r"\((.*)\)", string)
    if match:
        sign = -1
        string = match.group(1)
    return Decimal(string.replace('$', '').replace(',', '')).quantize(Decimal("0.01")) * sign


class Group(NamedTuple):
    """A named table for a subgroup."""

    # Named group.
    name: str

    # Subgroup within the named group, either 'Equities' or 'Futures'.
    subname: str

    # The corresponding subtable under them.
    table: Table


def SplitGroups(lines: List[str]) -> List[Group]:
    """Split the report into named groups."""

    # Clean up the list of lines before processing them.
    rows = []
    for line in lines:
        # Remove initial BOM marker line.
        if line.startswith('\ufeff'):
            continue
        # Remove bottom subtable that contains only summaries.
        if re.match('Cash & Sweep Vehicle', line):
            break
        rows.append(line.rstrip())

    def AddGroup():
        if name and subname and group:
            source = petl.MemorySource('\n'.join(group).encode('utf8'))
            group_list.append(Group(name, subname, petl.fromcsv(source)))

    # Split up the groups into subtables.
    group_list: List[Group] = []
    name, subname, group = None, None, []
    for row in rows:
        # Skip all empty rows, and reset the group if they occur.
        if not row:
            continue

        # Reset the current group.
        match = re.fullmatch(r'Group "(.*)"', row)
        if match:
            AddGroup()
            name, subname, group = match.group(1), None, []
            continue

        # Skip useless header.
        match = re.match(r"(Equities) and Equity Options", row)
        if match:
            AddGroup()
            subname, group = match.group(1), []
            continue
        match = re.match(r"(Futures) and Futures Options", row)
        if match:
            AddGroup()
            subname, group = match.group(1), []
            continue

        group.append(row)

    # Final table.
    AddGroup()

    return group_list


_FUTSYM = "/([A-Z0-9]+[FGHJKMNQUVXZ]2[0-9])"

def ParseInstrumentDescription(string: str, symroot: str) -> instrument.Instrument:
    """Parse an instrument description to a Beansym."""

    # Handle Future Option, e.g.,
    # 1/125000 JUN 21 (European) /EUUM21 1.13 PUT
    match = re.match(r"1/(\d+) ([A-Z]{3}) (2\d)(?: \(([^)]*)\))? "
                     fr"{_FUTSYM} ([0-9.]+) (PUT|CALL)", string)
    if match:
        (multiplier, month, year, subtype,
         expcode,
         strike, putcall) = match.groups()
        try:
            underlying, month = futures.GetUnderlyingMonth('/{}'.format(expcode[:-3]),
                                                           expcode[-3])
        except KeyError as exc:
            raise KeyError("Missing underlying/month {} from {}, {}".format(
                exc, string, symroot)) from exc
        assert underlying == symroot

        # TODO(blais): If the month is straddling the year, we will have to
        # advance by one here. Do this later. Write unit test.
        year = expcode[-2:]

        underlying = "{}{}{}".format(underlying, month, year)
        return instrument.Instrument(underlying=underlying,
                                     expcode=expcode,
                                     putcall=putcall[0],
                                     strike=Decimal(strike),
                                     multiplier=int(multiplier))

    # Handle Equity Option, e.g.,
    # 100 (Weeklys) 4 JUN 21 4130 CALL
    match = re.match(r"100(?: \(([^)]*)\))? (\d+ [A-Z]{3} 2\d) "
                     r"([0-9.]+) (PUT|CALL)", string)
    if match:
        subtype, day_month_year, strike, putcall = match.groups()
        expiration = parse(day_month_year).date()
        return instrument.Instrument(underlying=symroot,
                                     expiration=expiration,
                                     putcall=putcall[0],
                                     strike=Decimal(strike),
                                     multiplier=100)

    # Handle Future, e.g.,
    # 2-Year U.S. Treasury Note Futures,Jun-2021,ETH (prev. /ZTM1)
    match = re.fullmatch(r"(.*) \(prev. (/.*)\)", string)
    if match:
        symbol = match.group(2)
        underlying = symbol[:-1] + '2' + symbol[-1:]
        return instrument.Instrument(underlying=underlying)

    # Handle Equity, e.g.,
    # ISHARES TRUST CORE S&P TTL STK ETF
    # ISHARES TRUST RUS 2000 GRW ETF
    match = re.fullmatch(r"(.*) ETF( NEW)?", string)
    if match:
        return instrument.Instrument(underlying=symroot)

    raise ValueError("Could not parse description: '{}'".format(string))


def InferCostFromPnl(rec: Record) -> Decimal:
    """Infer the cost from the P/L Open and Net Liq."""
    # Note: The 'P/L Open' field is rounded to cents.
    return rec.pnl_open - rec.net_liq


def InferCostFromTradePrice(rec: Record) -> Decimal:
    """Infer the cost from the Quantity and Trade Price."""
    return -rec.quantity * rec._instrument.multiplier * rec.price


def FoldInstrument(table: Table) -> Table:
    """Given a group table, remove and fold the underlying row into a replicated
    column. This function removes redundant grouping rows, folding their unique
    values as columns.
    """

    # The table is a two-level table of
    #
    # - One row for the underling as a whole. In particular, the 'Instrument'
    #   column for that row is where the ticker is found.
    #
    # - One row for each strategy subgroup. The 'Instrument' column contains the
    #   strategy for these rows. There is no value in Qty.
    #
    # - The remaining rows are actual positions. If positions are all options or
    #   futures options, there will also be rows dedicated to the corresponding
    #   underlyings, even if their quantity is zero. Remove those.

    table = (table

             # Fold the special underlying row.
             .addfield('symroot',
                       lambda r: r['Instrument'] if bool(r['BP Effect']) else None)
             .filldown('symroot')
             .selectfalse('BP Effect')

             # Folder the strategy row.
             .addfield('strategy',
                       lambda r: r['Instrument'] if not r['Qty'] else None)
             .filldown('strategy')
             .selecttrue('Qty')
             .convert('Qty', Decimal)
             .selectne('Qty', ZERO)
             .rename('Qty', 'quantity')

             # Synthetize our symbol.
             .addfield('_instrument',
                       lambda r: ParseInstrumentDescription(r.Instrument, r.symroot))
             .addfield('symbol',
                       lambda r: str(r._instrument))

             # Normalize names of remaining fields.
             .rename('Trade Price', 'price')
             .rename('Mark', 'mark')
             .rename('Net Liq', 'net_liq')
             .rename('P/L Open', 'pnl_open')
             .rename('P/L Day', 'pnl_day')

             # Convert numbers.
             .convert(['price', 'mark', 'net_liq', 'pnl_open', 'pnl_day'], ToDecimal)

             # Make up missing 'cost' field.
             #
             # Unfortunately the cost isn't provided directly, but we infer it
             # from the rest of the information.
             .addfield('cost', InferCostFromTradePrice)
             .cutout('_instrument')

             # Clean up the final table.
             .cut('symbol', 'quantity', 'price', 'mark',
                  'cost', 'net_liq', 'pnl_open', 'pnl_day')
             )

    return ReduceFragmentedPositions(table)


def ReduceFragmentedPositions(table: Table) -> Table:
    """Reduce stock and futures positions of the same underlying to one line."""

    # In TOS, multiple purchases with different costs will show multiple lines.
    # We need to reduce these to a single position with a single average cost in
    # order to join it to the transactions.

    agg = {key: (key, sum) for key in table.header()[1:]}
    return table.aggregate('symbol', agg)


def GetPositions(filename: str) -> Table:
    """Read and parse the positions statement."""

    # Read the positions table.
    with open(filename) as csvfile:
        lines = csvfile.readlines()

    # Prepare tables for aggregation, inserting groups and stripping subtables
    # (no reason to treat Equities and Futures distinctly).
    groups = SplitGroups(lines)

    tables = []
    for x in groups:
        if x.table.nrows() == 0:
            continue

        gtable = (FoldInstrument(x.table)
                  .addfield('group', x.name, index=0))
        tables.append(gtable)

    # Add the account number.
    account = utils.GetAccountNumber(filename)
    table = (petl.cat(*tables)
             .addfield('account', account, index=0))

    return table


def MatchFile(filename: str) -> Optional[Tuple[str, str, callable]]:
    """Return true if this file is a matching transactions file."""
    _FILENAME_RE = r"(\d{4}-\d{2}-\d{2})-PositionStatement.csv"
    match = re.match(_FILENAME_RE, path.basename(filename))
    if not match:
        return None
    date = match.group(1)
    return 'thinkorswim', date, poslib.MakeParser(GetPositions)


@click.command()
@click.argument('filename', type=click.Path(resolve_path=True, exists=True))
def main(filename: str):
    """Simple local runner for this translator."""
    print(GetPositions(filename).lookallstr())


if __name__ == '__main__':
    main()
