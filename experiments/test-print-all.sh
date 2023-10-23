#!/bin/bash
# Just exercise all the print commands as a test while we're refactoring.
set -x
johnny-print config >/dev/null
SOURCES="x96 x18 x20 x38"
for CMD in transactions nontrades positions; do
    for S in $SOURCES; do
        johnny-print transactions $S >/dev/null
    done
done
