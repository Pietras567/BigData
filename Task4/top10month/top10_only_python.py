from functools import reduce


def parse_line_to_month(line: str):
    parts = line.strip().split(',')
    if len(parts) < 3:
        return None

    date, cases_str = parts[1], parts[2]
    month = date[:7]
    try:
        cases = int(cases_str)
    except ValueError:
        return None

    return (month, cases)


with open('incidence.csv', encoding='utf-8') as f:
    all_lines = f.readlines()

mapped_month = map(parse_line_to_month, all_lines)

filtered_month = filter(lambda x: x is not None, mapped_month)


def sum_by_month(acc: dict, item: tuple):
    month, cases = item
    acc[month] = acc.get(month, 0) + cases
    return acc


monthly_totals: dict = reduce(sum_by_month, filtered_month, {})

top_10 = sorted(
    monthly_totals.items(),
    key=lambda kv: kv[1],
    reverse=True
)[:10]

for month, total in top_10:
    print(f"{month}\t{total}")
