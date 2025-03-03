#!/bin/bash
rm -f measurements1e9.txt
for i in {1..10000};
do
	cat measurements.txt >> measurements1e9.txt
done
