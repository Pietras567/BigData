#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    parts = line.split(',')
    if len(parts) < 3:
        continue
    country, date, cases_str = parts[0], parts[1], parts[2]
    try:
        cases = int(cases_str)
    except ValueError:
        continue
    month = date[:7]
    print(f"{month}\t{cases}")
