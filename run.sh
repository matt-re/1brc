#!/bin/bash
set -e
TIMEFORMAT='%2R'
for run in {1..5}; do
	time ./1brc measurements_1b.txt > /dev/null
done

