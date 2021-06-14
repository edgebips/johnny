#!/bin/bash

DOWNLOADS=$(HOME)/trading/downloads

run:
	johnny-web $(DOWNLOADS)

config:
	johnny-config $(DOWNLOADS) $(DOWNLOADS)/johnny_clean.pbtxt $(DOWNLOADS)/johnny_residual.pbtxt

replace-config:
	mv $(DOWNLOADS)/johnny_clean.pbtxt $(shell readlink $(DOWNLOADS)/johnny.pbtxt)
