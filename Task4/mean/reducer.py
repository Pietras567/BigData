#!/usr/bin/env python3
import sys

current_country = None
current_sum = 0
current_count = 0
country = None
for line in sys.stdin:
    line = line.strip()
    country, total_cases = line.split('\t')
    try:
        s_str, c_str = total_cases.split(',', 1)
        cases = int(s_str)
        count = int(c_str)
    except ValueError:
        continue
    if current_country == country:
        current_sum += cases
        current_count += count
    else:
        if current_country:
            avg = current_sum / current_count
            print(f"{current_country}\t{avg}")
        current_sum = cases
        current_country = country
        current_count = count

average = current_sum / current_count
print(f"{current_country}\t{average}")
