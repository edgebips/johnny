#!/bin/bash

DOWNLOADS=$(HOME)/trading/downloads
#CONFIG=$(JOHNNY_CONFIG)
CONFIG=$(HOME)/r/q/office/accounting/trading/johnny.pbtxt

move-files:
	-mv -f $(HOME)/tasty* $(HOME)/*Statement.csv $(DOWNLOADS)

serve: move-files
	johnny-web $(DOWNLOADS)

chains:
	johnny-print chains $(DOWNLOADS)

config:
	johnny-config $(DOWNLOADS) $(DOWNLOADS)/johnny_clean.pbtxt $(DOWNLOADS)/johnny_residual.pbtxt

config-diff:
	xxdiff -B $(DOWNLOADS)/johnny.pbtxt $(DOWNLOADS)/johnny_clean.pbtxt

config-clobber:
	mv $(DOWNLOADS)/johnny_clean.pbtxt $(shell readlink $(DOWNLOADS)/johnny.pbtxt)

update:
	tastyworks-update $(DOWNLOADS)/tastyworks-individual.db -a Individual
	tastyworks-update $(DOWNLOADS)/tastyworks-roth.db -a Roth

import:
	johnny-import $(CONFIG)

debug:
	python3 ./experiments/johnny-debug $(CONFIG)
