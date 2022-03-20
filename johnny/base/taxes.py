"""Validation and routines for parsed worksheets and form 8949 tables.
"""

from johnny.base.etl import Table


_FIELDS_WORKSHEETS = """
instype symbol cost proceeds st_gain_loss lt_gain_loss gain_loss
""".split()


def validate_worksheet(table: Table):
    missing_fields = set(_FIELDS_WORKSHEETS) - set(table.fieldnames())
    if missing_fields:
        raise ValueError(f"Missing fields from worksheet: {missing_fields}")


_FIELDS_FORM8949 = """
instype symbol cost proceeds gain_adj gain_loss term box
""".split()


_BOX_VALUES = set(["A", "B", "C", "D", "E", "F", ""])
_TERM_VALUES = set(["ST", "LT", "60/40"])


def validate_form8949(table: Table):
    missing_fields = set(_FIELDS_FORM8949) - set(table.fieldnames())
    if missing_fields:
        raise ValueError(f"Missing fields from 8949: {missing_fields}")

    invalid_boxes = set(table.values("box")) - _BOX_VALUES
    if invalid_boxes:
        raise ValueError(f"Invalid boxes: {invalid_boxes}")

    invalid_terms = set(table.values("term")) - _TERM_VALUES
    if invalid_terms:
        raise ValueError(f"Invalid terms: {invalid_terms}")
