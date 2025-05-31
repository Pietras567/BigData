#!/usr/bin/env python3
import sys

for line in sys.stdin:
    parts = line.strip().split(',')
    if len(parts) < 3:
        continue
    country, date, cases_s = parts[0], parts[1], parts[2]
    try:
        c = int(cases_s)
    except ValueError:
        continue
    print(f"{country}\t{c}")
