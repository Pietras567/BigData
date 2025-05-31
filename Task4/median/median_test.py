import pandas as pd

df = pd.read_csv('incidence.csv', usecols=['location_key', 'new_confirmed'])

df['new_confirmed'] = pd.to_numeric(df['new_confirmed'], errors='coerce')

df = df.dropna(subset=['new_confirmed'])

result = df.groupby('location_key', as_index=False)['new_confirmed'].median()

print(result)
