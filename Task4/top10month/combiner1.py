#!/usr/bin/env python3
import sys

current_month = None
partial_sum = 0

for line in sys.stdin:
    month, val = line.strip().split('\t', 1)
    try:
        v = int(val)
    except ValueError:
        continue
    if month == current_month:
        partial_sum += v
    else:
        if current_month is not None:
            print(f"{current_month}\t{partial_sum}")
        current_month = month
        partial_sum = v

if current_month is not None:
    print(f"{current_month}\t{partial_sum}")
