#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    parts = line.split('\t')
    if len(parts) != 2:
        continue
    month, total_str = parts
    try:
        total = int(total_str)
    except ValueError:
        continue
    print(f"top\t{month},{total}")
