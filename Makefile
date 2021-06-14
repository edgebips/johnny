#!/bin/bash

DOWNLOADS=$(HOME)/trading/downloads

run:
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
	xxdiff -B $(DOWNLOADS)/johnny_clean.pbtxt $(DOWNLOADS)/johnny.pbtxt

config-clobber:
	mv $(DOWNLOADS)/johnny_clean.pbtxt $(shell readlink $(DOWNLOADS)/johnny.pbtxt)
