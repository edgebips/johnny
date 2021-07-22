#!/bin/bash

DOWNLOADS=$(HOME)/trading/downloads
#CONFIG=$(JOHNNY_CONFIG)
CONFIG=$(HOME)/r/q/office/accounting/trading/johnny.pbtxt
CONFIG_NEW=$(HOME)/r/q/office/accounting/trading/johnny.pbtxt.new

move-files:
	-mv -f $(HOME)/tasty* $(HOME)/*Statement.csv $(DOWNLOADS)

serve: move-files
	johnny-web $(DOWNLOADS)

config:
	johnny-config $(CONFIG) | tee $(CONFIG_NEW)

config-diff:
	xxdiff -B $(CONFIG) $(CONFIG_NEW)

update:
	tastyworks-update $(DOWNLOADS)/tastyworks-individual.db -a Individual
	tastyworks-update $(DOWNLOADS)/tastyworks-roth.db -a Roth

import:
	johnny-import $(CONFIG)

debug:
	python3 ./experiments/johnny-debug $(CONFIG)
