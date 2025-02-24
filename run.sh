#!/usr/bin/env bash
set -e
for run in {1..5}; do
	/usr/bin/time -p ./1brc measurements_1b.txt 2>&1 > /dev/null | grep real | sed 's/[^ ]* //'
done

