from functools import reduce


def parse_line(line: str):
    parts = line.strip().split(',')
    if len(parts) < 3:
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
        acc[country] = []
    acc[country].append(cases)
    return acc


country_values: dict = reduce(reducer, filtered, {})


def median_of_list(lst):
    sorted_lst = sorted(lst)
    n = len(sorted_lst)
    mid = n // 2
    if n % 2 == 1:
        return sorted_lst[mid]
    else:
        return (sorted_lst[mid - 1] + sorted_lst[mid]) / 2


for country, values in country_values.items():
    med = median_of_list(values)
    print(f"{country}\t{med}")
