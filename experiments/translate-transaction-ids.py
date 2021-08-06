#!/usr/bin/env python3
"""Translate transaction ids in the input file.

This is used temporarily after changing the definition of the transaction ids to
leverage similarity in the order ids.
"""
__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

import logging
import os
from typing import List, Optional

import click

from johnny.base import discovery
from johnny.base import mark
from johnny.base import chains as chainslib
from johnny.base import config as configlib
from johnny.base import instrument
from johnny.base.etl import petl, Table


@click.command()
@click.option('--config', '-c',
              help="Configuration filename. Default to $JOHNNY_CONFIG")
def main(config: Optional[str]):
    "Find, process and print transactions."

    filename = configlib.GetConfigFilenameWithDefaults(config)
    config = configlib.ParseFile(filename)
    chains_db = configlib.ReadChains(config.input.chains_db)
    logtables = discovery.ReadConfiguredInputs(config)
    table = (logtables[configlib.Account.LogType.TRANSACTIONS]
             .selectne('transaction_new_id', None))

    new_ids = list((table
                    .values('transaction_new_id')))
    assert len(list(new_ids)) == len(set(new_ids)), (
        len(list(new_ids)), len(set(new_ids)))

    mapping = (table
               .lookup('transaction_id', 'transaction_new_id'))

    for chain in chains_db.chains:
        if not chain.ids:
            continue
        ids = list(chain.ids)
        chain.ClearField('ids')
        for id in ids:
            try:
                mapped = mapping[id]
                for newid in mapped:
                    if newid is None:
                        continue
                    chain.ids.append(newid)
            except KeyError:
                chain.ids.append(id)

    print(config)
    #pp(mapping)

    #print(table.lookallstr())


if __name__ == '__main__':
    main(obj={})
