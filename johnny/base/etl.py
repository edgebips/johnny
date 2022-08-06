"""PETL library import with our favorite global configuration parameters."""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from decimal import Decimal
from typing import Any, List, Set, Tuple, Union
import itertools

import petl
import petl.config
import petl.compat
import petl.util.vis

petl.config.look_style = "minimal"
petl.config.failonerror = True

Table = petl.Table
Record = petl.Record


# Run assertions. This is expensive, so changing this value to False globally
# disables all assertions.
ASSERT = False


def WrapRecords(records: Union[List[Record], itertools._grouper]) -> Table:
    """Wrap up a list of records back to a table."""
    if isinstance(records, itertools._grouper):
        records = list(records)
    return petl.wrap([records[0].flds] + records)


def PrintGroups(table: Table, column: str):
    """Debug print groups of a table."""

    def pr(grouper):
        print(petl.wrap(grouper).lookallstr())

    agg = table.aggregate(column, pr)
    str(agg.lookallstr())


def PrintToPython(table: Table):
    print("table = petl.wrap([")
    print("  {},".format(table.header()))
    for rec in table.records():
        print("  {},".format(repr(rec)))
    print("])")


def AssertTableEqual(table1: Table, table2: Table):
    """Assert equality between two tables."""
    for rec1, rec2 in zip(table1.records(), table2.records()):
        assert rec1 == rec2, (rec1, rec2)


# NOTE: A big problem with this function is that Table.typeset() forces
# evaluation of the table. Contemplating giving up this method.
def AssertColumns(table: Table, *columns: List[Tuple[str, Set[Any]]]):
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


def AssertFields(rec: Union[Record, tuple], *columns: List[Tuple[str, Set[Any]]]):
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


def Replace(rec: Record, **kwargs) -> Record:
    """Replace a field from a record, returning a new record."""
    lrec = list(rec)
    for key, value in kwargs.items():
        idx = rec.flds.index(key)
        lrec[idx] = value
    return type(rec)(lrec, rec.flds)
