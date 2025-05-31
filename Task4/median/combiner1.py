#!/usr/bin/env python3
import sys

current = None
count = 0

for line in sys.stdin:
    country, cases_s = line.strip().split('\t', 1)
    key = (country, cases_s)
    if key == current:
        count += 1
    else:
        if current is not None:
            country0, cases0 = current
            print(f"{country0}\t{cases0},{count}")
        current = key
        count = 1

if current is not None:
    country0, cases0 = current
    print(f"{country0}\t{cases0},{count}")
