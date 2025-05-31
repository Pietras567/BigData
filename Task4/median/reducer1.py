#!/usr/bin/env python3
import sys

current = None
total_count = 0
pairs = []


def flush():
    if current is None:
        return
    print(f"{current}\t#TOTAL\t{total_count}")
    for val, cnt in pairs:
        print(f"{current}\t{val}\t{cnt}")


for line in sys.stdin:
    country, rest = line.strip().split('\t', 1)
    if current is None:
        current = country

    if country != current:
        flush()
        current = country
        total_count = 0
        pairs = []

    cases_s, cnt_s = rest.split(',', 1)
    count = int(cnt_s)
    val = int(cases_s)
    total_count += count
    pairs.append((val, count))

flush()
