#!/usr/bin/env python3
import sys

current_country = None
current_cases = 0
country = None
for line in sys.stdin:
    line = line.strip()
    country, cases = line.split('\t')
    try:
        cases = int(cases)
    except ValueError:
        continue
    if current_country == country:
        current_cases = max(current_cases, cases)
    else:
        if current_country:
            print('%s\t%s' % (current_country, current_cases))
        current_cases = cases
        current_country = country
print('%s\t%s' % (current_country, current_cases))
