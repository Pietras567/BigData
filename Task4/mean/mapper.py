#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()

    parts = line.split(',')
    if len(parts) < 3:
        continue

    country, date, cases = parts[0], parts[1], parts[2]
    print(f"{country}\t{cases},1")
