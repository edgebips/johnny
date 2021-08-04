#!/bin/bash

# Set JOHNNY_CONFIG in order for this to work.
DOWNLOADS = $(HOME)/trading/downloads
JOHNNY_CONFIG_NEW = $(JOHNNY_CONFIG).new
TODAY = $(shell date +%Y%m%d)
EARNINGS_TODAY = $(HOME)/trading/earnings/earnings-$(TODAY).csv

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

config-gen:
	johnny-import -q

config-earnings:
	./experiments/finalize-earnings.py -g Earnings > $(JOHNNY_CONFIG_NEW)

config-diff diff:
	-xxdiff -D -B $(JOHNNY_CONFIG) $(JOHNNY_CONFIG_NEW)

config-clobber clobber:
	cp $(JOHNNY_CONFIG_NEW) $(JOHNNY_CONFIG)

config-commit commit:
	hg commit $(JOHNNY_CONFIG)

overnight-fetch:
	overnight-fetch --no-headless --output=$(EARNINGS_TODAY)  | tee /tmp/earnings.csv

# Note: Rate-limit the first one, and not the second.
# TODO(blais): Handle rate limiting in the API.
overnight:
	overnight -r -n --ameritrade-cache=/tmp/td -v --csv-filename=$(EARNINGS_TODAY) | tee $(EARNINGS_TODAY:.csv=.overnight_all)
	overnight --ameritrade-cache=/tmp/td -v --csv-filename=$(EARNINGS_TODAY) | tee $(EARNINGS_TODAY:.csv=.overnight)

accept-all:
	./experiments/accept-all.py > $(JOHNNY_CONFIG_NEW)

accept-specific-chains:
	cat | ./experiments/accept-chains.py -g Premium -s FINAL > $(JOHNNY_CONFIG_NEW)

# Proto generation rules.
protos: johnny/base/config_pb2.py johnny/base/chains_pb2.py

johnny/base/config_pb2.py: johnny/base/config.proto
	protoc -I . --python_out . --proto_path . $<

johnny/base/chains_pb2.py: johnny/base/chains.proto
	protoc -I . --python_out . --proto_path . $<
