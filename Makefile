#!/bin/bash

# Set JOHNNY_CONFIG in order for this to work.
DOWNLOADS=$(HOME)/trading/downloads
JOHNNY_CONFIG_NEW=$(JOHNNY_CONFIG).new

move-files:
	-mv -f $(HOME)/tasty* $(HOME)/*Statement.csv $(DOWNLOADS)

serve: move-files
	johnny-web

config: config-gen config-diff

config-gen:
	johnny-config > $(JOHNNY_CONFIG_NEW)

config-diff:
	-xxdiff -B $(JOHNNY_CONFIG) $(JOHNNY_CONFIG_NEW)

update:
	tastyworks-update -a Individual $(DOWNLOADS)/tastyworks-individual.db
	tastyworks-update -a Roth       $(DOWNLOADS)/tastyworks-roth.db

import:
	johnny-import

test:
	python3 -m pytest -x johnny

debug:
	python3 ./experiments/johnny-debug
