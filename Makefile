#!/bin/bash

# Set JOHNNY_CONFIG in order for this to work.
DOWNLOADS=$(HOME)/trading/downloads
JOHNNY_CONFIG_NEW=$(JOHNNY_CONFIG).new

test:
	python3 -m pytest -x johnny

debug:
	python3 ./experiments/johnny-debug

move-files:
	@echo
	-mv -f $(HOME)/tasty* $(HOME)/*Statement.csv $(DOWNLOADS)
	@echo

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

fetch-earnings:
	overnight-fetch --no-headless | tee /tmp/earnings.csv
