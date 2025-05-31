import pandas as pd

df = pd.read_csv('incidence.csv')

df['new_confirmed'] = pd.to_numeric(df['new_confirmed'], errors='coerce')
df = df.dropna(subset=['new_confirmed'])
df['month'] = df['date'].str.slice(0, 7)

sum_by_month = df.groupby('month', as_index=False)['new_confirmed'].sum()

print(sum_by_month.sort_values(by='new_confirmed', ascending=False).head(10))
