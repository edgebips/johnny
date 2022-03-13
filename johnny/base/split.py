"""Code to split transactions into multiple parts.

This is used to isolate some positions for long-term/short-term taxation purposes.
"""

from typing import List
from decimal import Decimal

from johnny.base.chains_pb2 import SplitTransaction
from johnny.base.etl import Table, WrapRecords, Replace


Q = Decimal("0.01")


def SplitTransactions(splits: List[SplitTransaction], transactions: Table) -> Table:
    """Process transactions to split based on manual splittin rules.
    Returns a new table of transactions.
    """
    split_map = {split.id: split for split in splits}
    split_rows = []
    for row in transactions.records():
        split = split_map.get(row.transaction_id)
        if split:
            total_quantity = Decimal(0)
            for part in split.parts:
                quantity = Decimal(part.quantity)
                fraction = quantity / row.quantity
                part_cost = (fraction * row.cost).quantize(Q)
                part_commissions = (fraction * row.commissions).quantize(Q)
                part_fees = (fraction * row.fees).quantize(Q)
                nrow = Replace(
                    row,
                    transaction_id=part.id,
                    quantity=quantity,
                    cost=part_cost,
                    commissions=part_commissions,
                    fees=part_fees,
                )
                split_rows.append(nrow)
                total_quantity += quantity
            assert total_quantity == row.quantity
        else:
            split_rows.append(row)
    return WrapRecords(split_rows)
