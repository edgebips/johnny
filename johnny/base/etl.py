"""PETL library import with our favorite global configuration parameters."""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from itertools import chain
from decimal import Decimal
from typing import Any, List, Set, Tuple, Union

import petl
petl.config.look_style = 'minimal'
petl.compat.numeric_types = petl.compat.numeric_types + (Decimal,)
petl.config.failonerror = True

Table = petl.Table
Record = petl.Record


# Run assertions. This is expensive, so changing this value to False globally
# disables all assertions.
ASSERT = False


def WrapRecords(records: List[Record]) -> Table:
    """Wrap up a list of records back to a table."""
    return petl.wrap([records[0].flds] + records)


def PrintGroups(table: Table, column: str):
    """Debug print groups of a table."""
    def pr(grouper):
        print(petl.wrap(grouper).lookallstr())
    agg = table.aggregate(column, pr)
    str(agg.lookallstr())


def PrintToPython(table: Table):
    print('table = petl.wrap([')
    print("  {},".format(table.header()))
    for rec in table.records():
        print("  {},".format(repr(rec)))
    print('])')


# NOTE: A big problem with this function is that Table.typeset() forces
# evaluation of the table. Contemplating giving up this method.
def AssertColumns(table: Table,
                  *columns: List[Tuple[str, Set[Any]]]):
    """Assert the presence of a particular subset of columns."""
    if ASSERT is False:
        return
    fieldnames = set(table.fieldnames())
    for name, exptypes in columns:
        assert name in fieldnames
        if not isinstance(exptypes, set):
            exptypes = {exptypes}
        exptypes = {type(None) if t is None else t for t in exptypes}
        exptypes = {t.__name__ for t in exptypes}
        realtypes = table.typeset(name)
        assert realtypes.issubset(exptypes), (name, realtypes, exptypes)


def AssertFields(rec: Union[Record, tuple],
                 *columns: List[Tuple[str, Set[Any]]]):
    """Assert the presence of a particular subset of columns."""
    if ASSERT is False:
        return
    fieldnames = set(rec.flds if isinstance(rec, Record) else rec._fields)
    for name, exptypes in columns:
        assert name in fieldnames
        if not isinstance(exptypes, set):
            exptypes = {exptypes}
        exptypes = {type(None) if t is None else t for t in exptypes}
        exptypes = {t.__name__ for t in exptypes}
        realtype = type(getattr(rec, name)).__name__
        assert realtype in exptypes, (name, realtype, exptypes)


def applyfn(table: Table, function, *args, **kwargs) -> Table:
    """Apply the given function to the table and return the result."""
    return function(table, *args, **kwargs)


petl.Table.applyfn = applyfn
