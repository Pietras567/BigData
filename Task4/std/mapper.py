#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    parts = line.split(',')
    if len(parts) < 3:
        continue

    country, date, cases_str = parts[0], parts[1], parts[2]
    try:
        x = float(cases_str)
    except ValueError:
        continue

    print(f"{country}\t{x},{x * x},1")
