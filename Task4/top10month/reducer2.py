#!/usr/bin/env python3
import sys

arr = []
for line in sys.stdin:
    line = line.strip()
    key, sval = line.split('\t', 1)
    if key != 'top':
        continue
    try:
        month, total_str = sval.split(',', 1)
        total = int(total_str)
    except ValueError:
        continue
    arr.append((month, total))

arr.sort(key=lambda x: x[1], reverse=True)

for month, total in arr[:10]:
    print(f"{month}\t{total}")
