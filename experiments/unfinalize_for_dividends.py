#!/usr/bin/env python3
"""Insert dividends in finalized chains.

This is to be used after the fact of completing updates.
"""

from typing import List, Optional
import collections
import datetime

import click
import dateutil.parser

from johnny.base.etl import petl, Record
from johnny.base import config as configlib
from johnny.base import chains as chainslib
from johnny.base import instrument
from johnny.base import mark
from johnny.base.config import ChainStatus


@click.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    help="Configuration filename. Default to $JOHNNY_CONFIG",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(exists=False),
    help="Output chains filename.",
)
@click.argument("filenames", nargs=-1)
def main(config: Optional[str], output: Optional[str], filenames: List[str]):
    # Read chains DB.
    if config is None:
        filename = configlib.GetConfigFilenameWithDefaults(config)
        config = configlib.ParseFile(filename)
        chains_filename = config.input.chains_db
    else:
        chains_filename = config
    chains_db = configlib.ReadChains(chains_filename)
    chain_map = {c.chain_id: c for c in chains_db.chains}

    # Read the transactions map.
    transactions = petl.frompickle(config.output.transactions_pickle).applyfn(
        instrument.Expand, "symbol"
    )
    price_map = mark.GetPriceMap(transactions, config)
    transactions = mark.Mark(transactions, price_map)
    transactions_map = transactions.recordlookupone("transaction_id")

    # Read the list of dividend rows.
    dividends = petl.cat(*[petl.fromcsv(filename) for filename in filenames]).convert(
        "datetime", dateutil.parser.parse
    )
    if 0:
        print(dividends.lookallstr())

    # Precompute a mapping of (account, symbol) to chains potentially involved.
    symbol_map = collections.defaultdict(set)
    for chain in chains_db.chains:
        for transaction_id in chain.ids:
            rec = transactions_map[transaction_id]
            if rec.instype == "Equity":
                symbol_map[(rec.account, rec.symbol)].add(chain.chain_id)

    # Find an associate a chain to each dividend.
    def FindChain(div: Record):
        # TODO(blais): Should use the nickname from the very top.
        matching_chains = []
        for chain_id in symbol_map[(div.account, div.symbol)]:
            chain = chain_map[chain_id]
            transactions = [transactions_map[id] for id in chain.ids]
            mindate = min([rec.datetime for rec in transactions])
            if chain.status == chainslib.ChainStatus.ACTIVE:
                maxdate = datetime.datetime.now()
            else:
                maxdate = max([rec.datetime for rec in transactions])
            maxdate = datetime.datetime.combine(
                maxdate.date() + datetime.timedelta(days=5), datetime.time(23, 59, 59)
            )
            if mindate <= div.datetime <= maxdate:
                matching_chains.append(chain)
        return [chain.chain_id for chain in matching_chains]

    dividends = dividends.convert("account", lambda v: "x{}".format(v[-4:-2])).addfield(
        "chains", FindChain
    )

    # Print out the dividends with associated chain.
    if 1:
        print(dividends.sort(["symbol", "datetime"]).lookallstr())

    for rec in dividends.records():
        chain = chain_map[rec.chains[0]]
        if chain.status == ChainStatus.FINAL:
            chain.status = ChainStatus.CLOSED

    if not output:
        output = chains_filename + ".new"
    print(f"# Outputting to {output}")
    with open(output, "w") as f:
        f.write(configlib.ToText(chains_db))


if __name__ == "__main__":
    main()
