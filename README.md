# Johnny: Trade Monitoring and Analysis

## Overview

This is code that can ingest transactions logs and positions log from various
discount brokers, normalize it, and run various analyses on it. See this
document for more details:
https://docs.google.com/document/d/18AfWSRhQ1sWr0S4rd0GvQFy_7bXxCod-TC1qNrPHCEM/


## Status

There is no release on PyPI. You have to install or run this from the Git
repositories.

## Setting up Johnny in a virtual environment

From the Johnny source directory:

1. Setup the virtual environment: `python3 -m venv venv`
2. Activate it: `source venv/bin/activate`
3. Install the package for local development into the virtual environment: `pip install --editable .`
4. Configure your environment: `export JOHNNY_CONFIG=/path/to/johnny/config.pbtxt`
5. Run the importer to create or update your database of normalized transactions: `johnny-import`
6. Run the web server to view: `johnny-web` and visit the web UI at http://localhost:5000

See `Makefile` for the most up-to-date common commands the author uses.

### Graphviz (optional)

Extra steps are required to install Graphviz: https://pygraphviz.github.io/documentation/stable/install.html


## Development Environment

The most flexible way to setup Johnny is to clone the required repositories and
setup your environment accordingly:

    cd $WORK
    git clone http://github.com/beancount/johnny
    git clone http://github.com/blais/mulmat
    git clone http://github.com/blais/goodkids
    export PYTHONPATH=$PYTHONPATH:$WORK/johnny/johnny:$WORK/mulmat/mulmat:$WORK/goodkids/goodkids
    export PATH=$PATH:$WORK/johnny/bin:$WORK/mulmat/bin:$WORK/goodkids/bin

You can then `git pull` from those repositories to update to the latest,
in-development versions.


## Inputs

There a two tools, which feed off of CSV downloads from either of thinkorswim or
Tastyworks. The tool automatically identifies the files from a directory of
these files all in the same place.

You will need both a positions file and a transactions log downloads.

### Tastyworks

- **Positions** Go to the `Positions` tab, click on `CSV`, save file to a
  directory. Make sure you have the following columns in your position tab:
  `price, mark, cost, net_liq, delta per quantity`.


- **Transactions** Go to the `History` tab, click on `CSV`, select a date range
  up to a point where you had no positions, scroll all the way to the bottom (to
  include the lines in the output, this is a known issue in TW), save file to a
  directory. If you have multiple accounts, do this for each account.

### Think-or-Swim (TD Ameritrade)

- **Positions** Go to the `Monitor >> Activity and Positions` tab, make sure all
  sections are expanded, make sure from the hamburger menu of the `Position
  Statement` section that you have

  * Show Groups turned on
  * Group symbols by: Type
  * Arrange positions by: Order

  Select the hamburger menu, and `Export to File`. Save to directory.

- **Transactions** Go to the `Monitor >> Account Statement` tab, make sure you
  have

  * The `Futures Cash Balance` section enabled (call the desk)
  * Show by symbol: unset
  * A date range that spans an interval where you had no positions on

  Select the hamburger menu, and `Export to File`. Save to directory.

### Interactive Brokers

This is not done yet, but will be integrate with Flex reports.


## Configuration

In order to work with Johnny, you have to provide a list of inputs and outputs.

Two types of inputs can be specified: transaction sources and position sources.

* Transaction sources are logs of transactions that occurred in an account.
* Position sources are lists of positions that provide updated prices. They are
  only used to mark open positions.

Here's an example of an input setup for a Tastyworks account:

    input {
      accounts {
        nickname: "x18"
        logtype: TRANSACTIONS
        module: "johnny.sources.tastyworks_api.transactions"
        source: "/home/user/trading/tastyworks-individual.db"
      }
      accounts {
        nickname: "x20"
        logtype: TRANSACTIONS
        module: "johnny.sources.tastyworks_api.transactions"
        source: "/home/user/trading/tastyworks-roth.db"
      }
      accounts {
        nickname: "twpos"
        logtype: POSITIONS
        module: "johnny.sources.tastyworks_csv.positions"
        source: "/home/user/trading/tastyworks_positions_*_*.csv"
      }
      ...
    }

Note how you can have multiple transactions logs for different subaccounts.

Here's an example of an input setup for a thinkorswim (TD) account:

    input {
      accounts {
        nickname: "x96"
        logtype: TRANSACTIONS
        module: "johnny.sources.thinkorswim_csv.transactions"
        source: "/home/user/Downloads/*-AccountStatement.csv"
        initial: "/home/user/trading/initial_positions.csv"
      }
      accounts {
        nickname: "tdpos"
        logtype: POSITIONS
        module: "johnny.sources.thinkorswim_csv.positions"
        source: "/home/user/Downloads/*-PositionStatement.csv"
      }
    }

If you don't start at the beginning of your account with no position, the list
of initial positions that are to be included can be specified in the `initial`
flag.

Johnny involves a database of chains, which gets updated as a by-product of
importing transactions. At the moment this database is read from a single
`pbtxt` file, the location of which you must specify:

    input {
      ...
      chains_db:    "/home/user/trading/chains.pbtxt"
    }

The modified database will be produced at the `chains_db` in the output:

    output {
      chains_db:    "/home/user/trading/chains.pbtxt.new"
      transactions: "/home/user/trading/transactions.pickledb"
      chains:       "/home/user/trading/chains.pickledb"
    }

The two other outputs from the import are a table of normalized transactions and
a table of trade chains. Note that after importing, you must manually copy the
updated chains db file to its input location:

    cp "/home/user/trading/chains.pbtxt.new" "/home/user/trading/chains.pbtxt"

This is probably temporary. I've been diffing the two files and copying manually
to confirm the correctness of the output.

Here's what a full input configuration might look like:

    input {
      accounts {
        nickname: "x18"
        logtype: TRANSACTIONS
        module: "johnny.sources.tastyworks_api.transactions"
        source: "/home/user/trading/tastyworks-individual.db"
      }
      accounts {
        nickname: "x20"
        logtype: TRANSACTIONS
        module: "johnny.sources.tastyworks_api.transactions"
        source: "/home/user/trading/tastyworks-roth.db"
      }
      accounts {
        nickname: "twpos"
        logtype: POSITIONS
        module: "johnny.sources.tastyworks_csv.positions"
        source: "/home/user/trading/tastyworks_positions_*_*.csv"
      }
      accounts {
        nickname: "x96"
        logtype: TRANSACTIONS
        module: "johnny.sources.thinkorswim_csv.transactions"
        source: "/home/user/Downloads/*-AccountStatement.csv"
        initial: "/home/user/trading/initial_positions.csv"
      }
      accounts {
        nickname: "tdpos"
        logtype: POSITIONS
        module: "johnny.sources.thinkorswim_csv.positions"
        source: "/home/user/Downloads/*-PositionStatement.csv"
      }

      chains_db:    "/home/user/trading/chains.pbtxt"
    }

    output {
      chains_db:    "/home/user/trading/chains.pbtxt.new"
      transactions: "/home/user/trading/transactions.pickledb"
      chains:       "/home/user/trading/chains.pickledb"
    }

In addition, you can provide a list groups and/or tags to ignore from the
web output (for presenting to others):

    presentation {
      ignore_groups: "Investment"
      ignore_groups: "Protective"
      ignore_tags: "#bigloser"
    }


## Basic Usage

### johnny-print

Ingests input files and prints out a normalized table of either positions,
transactions, or chains (trades).

    johnny-print positions <directory-or-files>
    johnny-print transactions <directory-or-files>
    johnny-print chains <directory-or-files>

This can be used to test the ingestion and normalization of input data.

If you want to select a subset of files, you can, e.g., to view only files from
thinkorswim:

    johnny-print chains *Statement.csv


### johnny-web

A simple, local web front-end for the presentation of chains, transactions,
positions, risk values, statistics, and more.

    JOHNNY_ROOT=<directory> johnny-web


## License

Copyright (C) 2020-2021  Martin Blais.  All Rights Reserved.

This code is distributed under the terms of the "GNU GPLv2 only".
See COPYING file for details.
