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
import petl
petl.config.look_style = 'minimal'

from johnny.broker.ameritrade import utils
from johnny.base import positions as poslib
from johnny.base import futures
from johnny.base import instrument
from johnny.base.number import ToDecimal


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

    return table


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


def ConsolidatePositionStatement(
        table,
        reference: Optional[Decimal] = None,
        debug_tables: bool = False) -> Tuple[Table, Table]:
    """Consolidate all the subtables in the position statement.

    The `reference` value is used to compute a reference-adjusted notional value
    based on deltas.
    """

    # Aggregator. Note: You need to have these columns shown in the UI, for all
    # groups.
    sums = {name: (name, sum) for name in FIELDS}

    # Prepare tables for aggregation, inserting groups and stripping subtables
    # (no reason to treat Equities and Futures distinctly).
    groups = SplitGroups(table)
    tables = []
    for name, subname, gtable in groups:
        counter = iter(itertools.count())
        def OnPosition(x):
            for row in x:
                print("XXX", row)
            print()
        xtable = (gtable
                  .addfield('PosNo',
                            lambda r: next(counter) if bool(r['BP Effect']) else None)
                  .filldown('PosNo')
                  .aggregate('PosNo', OnPosition))
        if debug_tables:
            print(xtable.lookallstr())

        ftable = (gtable
                  # Remove redundant detail.
                  .select(lambda r: bool(r['BP Effect']))
                  # Convert numbers to numbers.
                  .convert(FIELDS, ParseNumber)
                  # Select just the additive numerical fields.
                  .cut(['Instrument'] + FIELDS)
                  # Add group to the resulting table.
                  .addfield('Group', name, index=0)
                  .addfield('Type', subname, index=1)
                  )
        tables.append(ftable)

        if debug_tables:
            print(ftable.lookallstr())
            print(ftable.aggregate(key=None, aggregation=sums))
            print()

    if debug_tables:
        raise SystemExit

    # Consolidate the table.
    atable = petl.cat(*tables)

    # Add delta-equivalent notional value.
    if reference:
        atable = (atable
                  .addfield('Notional', lambda r: (r.Delta * reference).quantize(Q)))
        sums['Notional'] = ('Notional', sum)

    # Aggregate the entire table to a single row.
    totals = (atable
              .aggregate(key=None, aggregation=sums))

    return atable, totals


def Report(atable: Table, totals: Table,
           top_k: int = 5):
    """Print all the desired aggregations and filters."""

    print("# Position Statement\n")
    fields = list(FIELDS)
    if 'Notional' in atable.header():
        fields.append('Notional')

    # Concatenate totals to consolidated table.
    empty_row = petl.wrap([['Group', 'Type', 'Instrument'] + fields,
                           ['---'] * (len(fields) + 3)])
    consolidated = petl.cat(atable.convert(fields, float),
                            empty_row,
                            (totals
                             .convert(fields, float)
                             .addfield('Group', 'Totals', index=0)
                             .addfield('Type', '*', index=1)
                             .addfield('Instrument', '*', index=2)))

    # Print table detail.
    print("## Consolidated Position Detail\n")
    print(consolidated.lookallstr())

    # Print top-K largest positive and negative greeks risk.
    top_tables = []
    sep = '-/-'
    print("## Largest Values\n")
    for field in fields:
        stable = (atable
                  .sort(field, reverse=True)
                  .convert(field, lambda v: float(v))
                  .cut('Instrument', field)
                  .rename('Instrument', ''))
        head_table = stable.head(top_k)
        empty_table = petl.wrap([stable.header(), ['', '...', '...']])
        tail_table = stable.tail(top_k)
        sstable = (petl.cat(head_table, empty_table, tail_table)
                   .addfield(sep, ''))
        top_tables.append(sstable)
        #print(sstable.lookallstr())
    top_table = petl.annex(*top_tables)
    print(top_table.lookallstr())


def MatchFile(filename: str) -> Optional[Tuple[str, str, callable]]:
    """Return true if this file is a matching transactions file."""
    _FILENAME_RE = r"(\d{4}-\d{2}-\d{2})-PositionStatement.csv"
    match = re.match(_FILENAME_RE, path.basename(filename))
    if not match:
        return None
    date = match.group(1)
    return 'thinkorswim', date, poslib.MakeParser(GetPositions)



@click.command()
@click.argument('positions_csv', type=click.Path(resolve_path=True, exists=True))
@click.option('--reference', '-r', type=Decimal, default=None,
              help="Price of the beta-weighted reference applied to the downloaded file.")
@click.option('--notional', '-x', is_flag=True,
              help="Estimate notional exposure for each position.")
def main(positions_csv: str, reference: Decimal, notional: bool):
    """Main program."""

    # If the reference isn't given, attempt to get tit from
    if reference is None:
        try:
            from beanprice.sources import yahoo
        except ImportError:
            pass
        else:
            source = yahoo.Source()
            sprice = source.get_latest_price("SPY")
            reference = sprice.price

    # Read positions statement and consolidate it.
    #print(table.lookallstr())
    table = petl.fromcsv(filename)
    atable, totals = ConsolidatePositionStatement(filename, reference, debug_tables=notional)

    if not notional:
        Report(atable, totals, 10)
    else:
        print(atable.header())
        raise NotImplementedError("Missing parsing for positions.")
        # for row in atable.records():
        #     print(row)

    # TODO(blais): Compute beta-weighted adjusted values yourself (for better betas).
    # TODO(blais): Add % BP per trade, should be 3-5%.
    # TODO(blais): Render total % BP used and available, should be 35%.
    # TODO(blais): Compute notional equivalent exposure.
    # TODO(blais): Add correlation matrix between the major asset classes (oil, bonds, stocks, etc.).
    # TODO: Create a metric of delta, strategy and duration diversification.
    # TODO: Create a distribution of BPR size over Net Liq, should be 1-2%


if __name__ == '__main__':
    main()
