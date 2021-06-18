#!/bin/bash

DOWNLOADS=$(HOME)/trading/downloads

move-files:
	-mv -f $(HOME)/tasty* $(HOME)/*Statement.csv $(DOWNLOADS)

serve: move-files
	johnny-web $(DOWNLOADS)

transactions txn:
	johnny-print transactions $(DOWNLOADS)

positions pos:
	johnny-print positions $(DOWNLOADS)

chains:
	johnny-print chains $(DOWNLOADS)

config:
	johnny-config $(DOWNLOADS) $(DOWNLOADS)/johnny_clean.pbtxt $(DOWNLOADS)/johnny_residual.pbtxt

config-diff:
	xxdiff -B $(DOWNLOADS)/johnny.pbtxt $(DOWNLOADS)/johnny_clean.pbtxt

config-clobber:
	mv $(DOWNLOADS)/johnny_clean.pbtxt $(shell readlink $(DOWNLOADS)/johnny.pbtxt)
