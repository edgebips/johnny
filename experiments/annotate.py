#!/usr/bin/env python3
"""Fetch a list of chains to be annotated, run an editor with a list, and then
integrate that data in the chains.
"""
__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import logging
import tempfile
import subprocess
import os
import datetime
import os
import sys
from typing import List, Optional

import click

from johnny.base import discovery
from johnny.base import mark
from johnny.base import chains_pb2
from johnny.base import chains as chainslib
from johnny.base import config as configlib
from johnny.base import instrument
from johnny.base import recap as recaplib
from johnny.base.etl import petl, Table


@click.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    help="Configuration filename. Default to $JOHNNY_CONFIG",
)
@click.option(
    "--status",
    "-s",
    default=None,
    type=chains_pb2.ChainStatus.Value,
    help="Set the status on the given chains.",
)
@click.option("--group", "-g", default=None, help="Group to assign to chains.")
@click.argument("chain_ids", nargs=-1)  # Chain ids to set group
def main(
    config: Optional[str],
    status: Optional[int],
    group: Optional[str],
    chain_ids: list[str],
):
    "Find, process and print transactions."

    # Load the database.
    filename = configlib.GetConfigFilenameWithDefaults(config)
    config = configlib.ParseFile(filename)
    transactions = petl.frompickle(config.output.transactions_pickle)
    chains_table = petl.frompickle(config.output.chains_pickle)
    chains_db = configlib.ReadChains(config.output.chains_db)
    chains_map = {c.chain_id: c for c in chains_db.chains}

    # Get today's chains from the recap, filter them out to include their
    # editable fields.
    date = datetime.date.today()
    todays_chains = recaplib.get_chains_at_date(
        transactions, chains_table, chains_map, date
    )

    # Get all chains without a group.
    chain_ids = set(todays_chains.values("chain_id"))
    nogroup_chains = (
        chains_table.selecteq("group", "NoGroup")
        .selectnotin("chain_id", chain_ids)
        .addfield("action", "MissingGroup")
    )

    # Concatenate both groups together.
    edit_chains = (
        petl.cat(nogroup_chains, todays_chains)
        .convert("group", lambda group: "" if group == "NoGroup" else group)
        .cut("action", "chain_id", "underlyings", "days", "group", "strategy")
        .addfield(
            "comment",
            lambda r: (
                chains_map[r.chain_id].comment if r.chain_id in chains_map else ""
            ),
        )
    )

    # Spawn an editor.
    with tempfile.NamedTemporaryFile(suffix=".csv") as csvfile:
        edit_chains.tocsv(csvfile.name)
        csvfile.flush()

        editor = os.getenv("SHEETS_EDITOR", "vd")
        out = subprocess.check_call([editor, csvfile.name], shell=False)

        changes = list(petl.fromcsv(csvfile.name).records())

    # Apply modifications.
    for row in changes:
        chain = chains_map.get(row.chain_id, None)
        if chain is None:
            logging.error(f"Chain id '{row.chain_id}' not found.")
            continue

        # Override attributes.
        for attr in "group", "strategy", "comment":
            value = getattr(row, attr)
            if value and getattr(chain, attr) != value:
                logging.info(f"Overriding '{attr}' on {chain.chain_id}")
                if value:
                    setattr(chain, attr, value)
                else:
                    chain.ClearField(attr)

    # Write out to db.
    with open(config.output.chains_db, "w") as outfile:
        outfile.write(configlib.ToText(chains_db))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)-8s: %(message)s")
    main(obj={})
