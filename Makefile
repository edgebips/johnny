#!/bin/bash

# Set JOHNNY_CONFIG in order for this to work.
DOWNLOADS = $(HOME)/q/johnny-data/downloads/tastytrade_api
CHAINS = $(shell grep chains_db $(JOHNNY_CONFIG) | head -n1 | sed  -e 's/.*chains_db: *"//;s/"//')
CHAINS_NEW = $(shell grep chains_db $(JOHNNY_CONFIG) | tail -n1 | sed  -e 's/.*chains_db: *"//;s/"//')

test:
	python3 -m pytest -x johnny

update:
	tastytrade-update -a Individual  $(DOWNLOADS)/tastytrade-individual.db
	tastytrade-update -a Roth        $(DOWNLOADS)/tastytrade-roth.db
	tastytrade-update -a Traditional $(DOWNLOADS)/tastytrade-traditional.db

move-files:
	johnny-move-files

import: move-files
	johnny-import

serve:
	johnny-web

config: config-gen config-diff
earnings: config-earnings config-diff

import-light:
	johnny-import -q

config-earnings:
	./experiments/finalize-earnings.py -g Earnings

config-diff diff:
	-xxdiff -D -B $(CHAINS) $(CHAINS_NEW)

tmux-diff tdiff:
	tmux-diff $(CHAINS) $(CHAINS_NEW)

config-clobber clobber:
	cp $(CHAINS_NEW) $(CHAINS)

config-commit commit:
	hg commit -m "(Trading update)" $(CHAINS)

finalize-closed-chains:
	./experiments/finalize-closed-chains.py

accept-auto-ids:
	./experiments/accept-auto-ids.py

annotate:
	./experiments/annotate.py

accept-specific-chains:
	cat | ./experiments/accept-chains.py -g Premium -s FINAL

find-transfers:
	./experiments/find-transfers.py 'Assets:US:(Interactive|Ameritrade|Tastytrade)' # --end-date=2023-01-01

TREASURIES_INPUT = $(shell cat $(JOHNNY_CONFIG) | grep ameritrade_download_transactions_for_treasuries | sed -e 's@.*"\(.*\)"@\1@')
treasuries:
	python3 -m johnny.sources.ameritrade.treasuries $(TREASURIES_INPUT)


# Proto generation rules.
PROTOS_PB2 =                                    \
johnny/base/common_pb2.py                       \
johnny/base/config_pb2.py                       \
johnny/base/chains_pb2.py                       \
johnny/base/transactions_pb2.py                 \
johnny/base/instrument_pb2.py                   \
johnny/base/positions_pb2.py                    \
johnny/base/nontrades_pb2.py                    \
johnny/base/taxes_pb2.py                        \
johnny/sources/ameritrade/config_pb2.py         \
johnny/sources/tastytrade/config_pb2.py         \
johnny/sources/interactive/config_pb2.py

protos: $(PROTOS_PB2)

johnny/base/common_pb2.py: johnny/base/common.proto
	protoc -I . --python_out . --proto_path . $<

johnny/base/config_pb2.py: johnny/base/config.proto
	protoc -I . --python_out . --proto_path . $<

johnny/sources/ameritrade/config_pb2.py: johnny/sources/ameritrade/config.proto
	protoc -I . --python_out . --proto_path . $<

johnny/sources/tastytrade/config_pb2.py: johnny/sources/tastytrade/config.proto
	protoc -I . --python_out . --proto_path . $<

johnny/sources/interactive/config_pb2.py: johnny/sources/interactive/config.proto
	protoc -I . --python_out . --proto_path . $<

johnny/base/chains_pb2.py: johnny/base/chains.proto
	protoc -I . --python_out . --proto_path . $<

johnny/base/instrument_pb2.py: johnny/base/instrument.proto
	protoc -I . --python_out . --proto_path . $<

johnny/base/transactions_pb2.py: johnny/base/transactions.proto
	protoc -I . --python_out . --proto_path . $<

johnny/base/positions_pb2.py: johnny/base/positions.proto
	protoc -I . --python_out . --proto_path . $<

johnny/base/nontrades_pb2.py: johnny/base/nontrades.proto
	protoc -I . --python_out . --proto_path . $<

johnny/base/taxes_pb2.py: johnny/base/taxes.proto
	protoc -I . --python_out . --proto_path . $<
