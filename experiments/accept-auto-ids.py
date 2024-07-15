#!/usr/bin/env python3
"""Accept all auto_ids suggestions and promote them all to ids.
"""
__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from typing import List, Optional

import click

from johnny.base import chains_pb2
from johnny.base import chains as chainslib
from johnny.base import config as configlib

ChainStatus = chains_pb2.ChainStatus


@click.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    help="Configuration filename. Default to $JOHNNY_CONFIG",
)
def main(config: Optional[str]):
    filename = configlib.GetConfigFilenameWithDefaults(config)
    config = configlib.ParseFile(filename)
    chains_db = configlib.ReadChains(config.input.chains_db)
    chain_map = {c.chain_id: c for c in chains_db.chains}
    for chain in chain_map.values():
        if len(chain.auto_ids) > 0:
            chain.ids.extend(chain.auto_ids)
            chain.ClearField("auto_ids")

    with open(config.output.chains_db, "w") as outfile:
        outfile.write(configlib.ToText(chains_db))


if __name__ == "__main__":
    main(obj={})
