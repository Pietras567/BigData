#!/usr/bin/env python3
import math
import sys

current_country = None
sum_acc = 0.0
sum_sq_acc = 0.0
count_acc = 0

for line in sys.stdin:
    line = line.strip()
    parts = line.split('\t', 1)
    if len(parts) != 2:
        continue
    country, sval = parts
    try:
        s_str, sq_str, c_str = sval.split(',', 2)
        s = float(s_str)
        sq = float(sq_str)
        c = int(c_str)
    except ValueError:
        continue

    if country == current_country:
        sum_acc += s
        sum_sq_acc += sq
        count_acc += c
    else:
        if current_country is not None and count_acc > 0:
            mean = sum_acc / count_acc
            mean_sq = sum_sq_acc / count_acc
            variance = mean_sq - (mean * mean)
            stddev = math.sqrt(variance) if variance > 0 else 0.0
            print(f"{current_country}\t{stddev:.4f}")
        current_country = country
        sum_acc = s
        sum_sq_acc = sq
        count_acc = c

if current_country is not None and count_acc > 0:
    mean = sum_acc / count_acc
    mean_sq = sum_sq_acc / count_acc
    variance = mean_sq - (mean * mean)
    stddev = math.sqrt(variance) if variance > 0 else 0.0
    print(f"{current_country}\t{stddev:.4f}")
