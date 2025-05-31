import math
from functools import reduce


def parse_line(line: str):
    parts = line.strip().split(',')
    if len(parts) < 8:
        return None

    country, date, cases_str = parts[0], parts[1], parts[2]
    try:
        cases = int(cases_str)
    except ValueError:
        return None

    return (country, cases)


with open('incidence.csv', encoding='utf-8') as f:
    all_lines = f.readlines()

mapped = map(parse_line, all_lines)

filtered = filter(lambda x: x is not None, mapped)


def reducer(acc: dict, item: tuple):
    country, cases = item
    if country not in acc:
        acc[country] = [0, 0, 0]
    acc[country][0] += cases
    acc[country][1] += cases * cases
    acc[country][2] += 1
    return acc


stats: dict = reduce(reducer, filtered, {})

for country, (sum_cases, sum_squares, count) in stats.items():
    if count > 0:
        mean = sum_cases / count
        mean_sq = sum_squares / count
        variance = mean_sq - (mean * mean)
        stddev = math.sqrt(variance) if variance > 0 else 0.0
        print(f"{country}\t{stddev:.4f}")
