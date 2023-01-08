#!/usr/bin/env python3
"""Produce an HTML list URLs to easily render just the chains with changes (auto_ids).
"""

from typing import Optional

import click

from johnny.base import config as configlib


TEMPLATE_PRE = """
<html>
<head>
</head>
<body>
<h1>Modified chains</h1>
<ul>
"""

BASE_URL = "http://localhost:5000/chain"
TEMPLATE_URL = '<li><a href="{url}">{chain}</a></li>'

TEMPLATE_POST = """
</ul>
</body>
</html>
"""


@click.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    help="Configuration filename. Default to $JOHNNY_CONFIG",
)
def main(config: Optional[str]):
    if config is None:
        filename = configlib.GetConfigFilenameWithDefaults(config)
        config = configlib.ParseFile(filename)
        chains_filename = config.input.chains_db
    else:
        chains_filename = config

    chains_db = configlib.ReadChains(chains_filename)

    print(TEMPLATE_PRE)
    for chain in chains_db.chains:
        if not chain.auto_ids:
            continue
        print(TEMPLATE_URL.format(chain=chain.chain_id,
                                  url=f"{BASE_URL}/{chain.chain_id}"))
    print(TEMPLATE_POST)


if __name__ == "__main__":
    main(obj={})
