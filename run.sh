#!/bin/bash
TIMEFORMAT='%2R'
for run in {1..5}; do
	time ./1brc measurements.txt > /dev/null
done

