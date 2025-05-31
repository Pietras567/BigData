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
    acc[country] = acc.get(country, 0) + cases
    return acc


result: dict = reduce(reducer, filtered, {})

for country, total_cases in result.items():
    print(f"{country}\t{total_cases}")
