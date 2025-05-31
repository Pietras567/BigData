#!/usr/bin/env python3
import sys

current_country = None
partial_sum = 0
partial_count = 0

for line in sys.stdin:
    line = line.strip()
    parts = line.split('\t', 1)
    if len(parts) != 2:
        continue
    country, sval = parts
    try:
        s_str, c_str = sval.split(',', 1)
        s = int(s_str)
        c = int(c_str)
    except ValueError:
        continue

    if country == current_country:
        partial_sum += s
        partial_count += c
    else:
        if current_country is not None:
            print(f"{current_country}\t{partial_sum},{partial_count}")
        current_country = country
        partial_sum = s
        partial_count = c

if current_country is not None:
    print(f"{current_country}\t{partial_sum},{partial_count}")
