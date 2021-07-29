#!/bin/bash

# Set JOHNNY_CONFIG in order for this to work.
DOWNLOADS=$(HOME)/trading/downloads
JOHNNY_CONFIG_NEW=$(JOHNNY_CONFIG).new

test:
	python3 -m pytest -x johnny

debug:
	python3 ./experiments/johnny-debug

move-files:
	-mv -f $(HOME)/tasty* $(HOME)/*Statement.csv $(DOWNLOADS)

update:
	tastyworks-update -a Individual $(DOWNLOADS)/tastyworks-individual.db
	tastyworks-update -a Roth       $(DOWNLOADS)/tastyworks-roth.db

import: move-files
	johnny-import

serve:
	johnny-web

config: config-gen config-diff

config-gen:
	johnny-config > $(JOHNNY_CONFIG_NEW)

config-earnings:
	./experiments/finalize-earnings.py -g Overnight > $(JOHNNY_CONFIG_NEW)

config-diff:
	-xxdiff -B $(JOHNNY_CONFIG) $(JOHNNY_CONFIG_NEW)

clobber-config:
	cp $(JOHNNY_CONFIG_NEW) $(JOHNNY_CONFIG)
