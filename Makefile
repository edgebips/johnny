#!/bin/bash

# Set JOHNNY_CONFIG in order for this to work.
DOWNLOADS = $(HOME)/trading/downloads
CHAINS = $(shell grep chains_db $(JOHNNY_CONFIG) | head -n1 | sed  -e 's/.*chains_db: "//;s/"//')
CHAINS_NEW = $(shell grep chains_db $(JOHNNY_CONFIG) | tail -n1 | sed  -e 's/.*chains_db: "//;s/"//')

test:
	python3 -m pytest -x johnny

debug:
	python3 ./experiments/johnny-debug

move-files:
	johnny-move-files

update:
	tastyworks-update -a Individual $(DOWNLOADS)/tastyworks-individual.db
	tastyworks-update -a Roth       $(DOWNLOADS)/tastyworks-roth.db

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

config-clobber clobber:
	cp $(CHAINS_NEW) $(CHAINS)

config-commit commit:
	hg commit $(CHAINS)

final:
	./experiments/accept-all.py

annotate:
	./experiments/annotate.py

accept-specific-chains:
	cat | ./experiments/accept-chains.py -g Premium -s FINAL

# Proto generation rules.
protos: johnny/base/config_pb2.py johnny/base/chains_pb2.py

johnny/base/config_pb2.py: johnny/base/config.proto
	protoc -I . --python_out . --proto_path . $<

johnny/base/chains_pb2.py: johnny/base/chains.proto
	protoc -I . --python_out . --proto_path . $<
