#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    country, date, cases, _, _, _, _, _ = line.split(',')
    print('%s\t%s' % (country, cases))
