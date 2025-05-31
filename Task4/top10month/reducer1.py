#!/usr/bin/env python3
import sys

current_month = None
total = 0

for line in sys.stdin:
    month, val = line.strip().split('\t', 1)
    try:
        v = int(val)
    except ValueError:
        continue
    if month == current_month:
        total += v
    else:
        if current_month is not None:
            print(f"{current_month}\t{total}")
        current_month = month
        total = v

if current_month is not None:
    print(f"{current_month}\t{total}")
