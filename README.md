# Johnny: Trade Monitoring and Analysis

## Overview

This is code that can ingest transactions logs and positions log from various
discount brokers, normalize it, and run various analyses on it. See this
document for more details:
https://docs.google.com/document/d/18AfWSRhQ1sWr0S4rd0GvQFy_7bXxCod-TC1qNrPHCEM/


## Status

Heavily under development. Assume everything will be moved and broken at some
point or other in the next couple of months.


## Setting up Johnny in a virtual environment

From the Johnny source directory:

1. Setup the virtual environment: `python3 -m venv venv`
2. Activate it: `source venv/bin/activate`
3. Install the package for local development into the virtual environment: `pip install --editable .`
4. Configure your environment: `export JOHNNY_CONFIG=/path/to/johnny/config.pbtxt`
5. Run the importer to create or update your database of normalized transactions: `johnny-import`
6. Run the web server to view: `johnny-web` and visit the web UI at http://localhost:5000

See `Makefile` for the most up-to-date common commands the author uses.

## Graphviz (optional)

Extra steps are required to install Graphviz: https://pygraphviz.github.io/documentation/stable/install.html


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

### thinkorswim

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
