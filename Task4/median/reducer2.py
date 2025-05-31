#!/usr/bin/env python3
import sys

totals = {}
with open('totals.txt') as tf:
    for ln in tf:
        country, tag, n_s = ln.strip().split('\t')
        if tag == '#TOTAL':
            totals[country] = int(n_s)

current = None
values = []


def emit_median(country, values, N):
    mid1 = (N + 1) // 2
    mid2 = (N + 2) // 2 if N % 2 == 0 else mid1
    acc = 0
    m1 = m2 = None
    for val, cnt in values:
        prev = acc
        acc += cnt
        if m1 is None and acc >= mid1:
            m1 = val
        if m2 is None and acc >= mid2:
            m2 = val
            break
    median = (m1 + m2) / 2 if mid1 != mid2 else m1
    print(f"{country}\t{median:.2f}")


def flush():
    if current is None:
        return
    N = totals.get(current, 0)
    if N > 0:
        values.sort(key=lambda x: x[0])

        emit_median(country, values, N)


for line in sys.stdin:
    parts = line.strip().split('\t')
    country = parts[0]
    if parts[1] == '#TOTAL':
        continue
    val = int(parts[1])
    cnt = int(parts[2])

    if current is None:
        current = country

    if country != current:
        flush()
        current = country
        values = []

    values.append((val, cnt))

flush()
