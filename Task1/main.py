import os
import time

import pandas as pd
from google.cloud import bigquery

# Color constants
GREEN = '\033[92m'
BLUE = '\033[94m'
BASIC = '\033[0m'


def clean_countries_data(dataframe):
    dataframe['date'] = pd.to_datetime(dataframe['date'])

    # Drop "Netherlands Antilles"
    dataframe = dataframe[dataframe['country_name'] != 'Netherlands Antilles']
    print(f"{BLUE}Dropped rows with Netherlands Antilles"
          f"\nCleaned countries data{BASIC}")

    return dataframe

def process_group(group, new_column_name, cumulative_column_name):
    sorted_group = group.sort_values('date')
    return sorted_group
    # Cleaning time series - repairing missing values
    """
    Algorytm przetwarza wiersze w obrębie każdej grupy w następujący sposób:

    1. Jeśli w trakcie iteracji wartość w polu 'new' jest pusta (None), a aktualne pole 'cumulative' zawiera
       niepustą wartość, to pole 'new' zostaje wypełnione różnicą pomiędzy poprzednią wartością 'cumulative'
       i aktualną wartością 'cumulative'.

    2. Jeśli aktualne pole 'cumulative' jest puste, należy wyszukać najbliższą (dalej w czasie) niepustą
       wartość 'cumulative'. Następnie należy wyliczyć różnicę między poprzednim 'cumulative' a najbliższym
       późniejszym 'cumulative'.

    3. W przypadku gdy aktualne pole 'cumulative' pozostaje puste, to dla uzyskanej (w punkcie 2) różnicy
       należy odjąć wszystkie niepuste wartości 'new' znajdujące się w zakresie między poprzednim polem
       'cumulative' a następnym niepustym 'cumulative'. Pozostałą wartość dzieli się przez liczbę pustych pól
       'new' w tym samym zakresie, zaokrągla do pełnej liczby i zapisuje w aktualnym polu 'new'. Następnie
       pole 'cumulative' uzupełniane jest sumą poprzedniej wartości 'cumulative' oraz bieżącej wartości 'new'.

    4. Jeśli żadna wartość w grupie nie jest inna niż None (czyli wszystkie są puste), to wszystkie pola
       'new' i 'cumulative' zostają wypełnione zerami.

    5. Jeśli pierwsza wartość 'new' i 'cumulative' jest pusta, wstaw w obu kolumnach zera. Jeśli tylko jedna
       z nich jest pusta, należy ją uzupełnić wartością drugiej.

    6. Jeśli aktualne pole new jest puste oraz aktualne pole cumulative jest również puste, a w przyszłości 
    nie znajduje się już jakiekolwiek pole cumulative posiadające wartość, to do pola new należy wstawić wartość 0, 
    a do aktualnego pola cumulative wartość z poprzedniego cumulative.
    """
    # to do - algorytm powyżej
    # Implementacja algorytmu dla 'new_confirmed', 'cumulative_confirmed'

    # Sprawdzenie czy wszystkie wartości są puste (punkt 4)
    if sorted_group[new_column_name].isna().all() and sorted_group[cumulative_column_name].isna().all():
        sorted_group[new_column_name] = 0
        sorted_group[cumulative_column_name] = 0
        return sorted_group

    # Obsługa pierwszego wiersza (punkt 5)
    if pd.isna(sorted_group[new_column_name].iloc[0]) and pd.isna(sorted_group[cumulative_column_name].iloc[0]):
        sorted_group.loc[sorted_group.index[0], new_column_name] = 0
        sorted_group.loc[sorted_group.index[0], cumulative_column_name] = 0
    elif pd.isna(sorted_group[new_column_name].iloc[0]):
        sorted_group[new_column_name].iloc[0] = sorted_group[cumulative_column_name].iloc[0]
    elif pd.isna(sorted_group[cumulative_column_name].iloc[0]):
        sorted_group[cumulative_column_name].iloc[0] = sorted_group[new_column_name].iloc[0]

    # Iteracyjne przetwarzanie pozostałych wierszy
    prev_cumulative = sorted_group[cumulative_column_name].iloc[0]
    for i in range(1, len(sorted_group)):
        current_new = sorted_group[new_column_name].iloc[i]
        current_cumulative = sorted_group[cumulative_column_name].iloc[i]

        # Punkt 1: Puste new, niepuste cumulative
        if pd.isna(current_new) and not pd.isna(current_cumulative):
            sorted_group.loc[sorted_group.index[i], new_column_name] = current_cumulative - prev_cumulative

        # Punkt 2 i 3: Puste cumulative
        elif pd.isna(current_cumulative):
            # Szukanie następnej niepustej wartości cumulative
            next_non_na_index = None
            for j in range(i + 1, len(sorted_group)):
                if not pd.isna(sorted_group[cumulative_column_name].iloc[j]):
                    next_non_na_index = j
                    break

            if next_non_na_index is not None:
                next_cumulative = sorted_group[cumulative_column_name].iloc[next_non_na_index]
                total_diff = next_cumulative - prev_cumulative

                # Odejmowanie wszystkich niepustych wartości 'new' w zakresie
                non_na_new_sum = 0
                empty_new_count = 0
                for j in range(i, next_non_na_index):
                    if not pd.isna(sorted_group[new_column_name].iloc[j]):
                        non_na_new_sum += sorted_group[new_column_name].iloc[j]
                    else:
                        empty_new_count += 1

                remaining_diff = total_diff - non_na_new_sum

                # Jeśli są puste pola 'new', wypełniamy je równomiernie
                if empty_new_count > 0:
                    value_per_empty = round(remaining_diff / empty_new_count)
                    for j in range(i, next_non_na_index):
                        if pd.isna(sorted_group[new_column_name].iloc[j]):
                            sorted_group.loc[sorted_group.index[j], new_column_name] = value_per_empty

                # Uzupełnianie bieżącego pola cumulative
                if not pd.isna(sorted_group[new_column_name].iloc[i]):
                    sorted_group.loc[sorted_group.index[i], cumulative_column_name] = prev_cumulative + sorted_group.loc[sorted_group.index[i], new_column_name]
            else:
                # Punkt 6: Puste 'new' i 'cumulative', oraz brak przyszłych niepustych 'cumulative'
                if pd.isna(current_new):
                    sorted_group.loc[sorted_group.index[i], new_column_name] = 0
                sorted_group.loc[sorted_group.index[i], cumulative_column_name] = (
                        prev_cumulative + sorted_group.loc[sorted_group.index[i], new_column_name])

        # Aktualizacja prev_cumulative dla następnej iteracji
        if not pd.isna(sorted_group[cumulative_column_name].iloc[i]):
            prev_cumulative = sorted_group[cumulative_column_name].iloc[i]

    return sorted_group

def fix_negative_values(dataframe):
    # Fixing negative values
    for column in ['new_confirmed', 'cumulative_confirmed', 'new_tested', 'cumulative_tested']:
        dataframe[column] = pd.to_numeric(dataframe[column], errors='coerce')
        dataframe.loc[dataframe[column] < 0, column] = dataframe.loc[dataframe[column] < 0, column] * -1

    return dataframe

def clean_incidence_data(dataframe):
    dataframe['date'] = pd.to_datetime(dataframe['date'])

    dataframe = fix_negative_values(dataframe)

    result_df = dataframe.groupby('location_key').apply(process_group, new_column_name='new_confirmed',
                                                                       cumulative_column_name='cumulative_confirmed',
                                                                       include_groups=True)

    result_df = result_df.reset_index(drop=True)

    print(f"{BLUE}Cleaned COVID-19 confirmed incidents data{BASIC}")

    result_df = result_df.groupby('location_key').apply(process_group, new_column_name='new_tested',
                                                                       cumulative_column_name='cumulative_tested',
                                                                       include_groups=True)

    result_df = result_df.reset_index(drop=True)

    print(f"{BLUE}Cleaned COVID-19 tests data{BASIC}")

    print(f"{BLUE}Cleaned COVID-19 incidents data{BASIC}")

    return result_df

def clean_mortality_data(dataframe):
    dataframe['date'] = pd.to_datetime(dataframe['date'])

    #dataframe = fix_negative_values(dataframe)

    result_df = dataframe.groupby('location_key').apply(process_group, new_column_name='new_deceased',
                                                        cumulative_column_name='cumulative_deceased',
                                                        include_groups=True)

    result_df = result_df.reset_index(drop=True)

    print(f"{BLUE}Cleaned COVID-19 mortality data{BASIC}")

    return result_df

def clean_vaccination_data(dataframe):
    dataframe['date'] = pd.to_datetime(dataframe['date'])

    #dataframe = fix_negative_values(dataframe)

    result_df = dataframe.groupby('location_key').apply(process_group, new_column_name='new_persons_vaccinated',
                                                        cumulative_column_name='cumulative_persons_vaccinated',
                                                        include_groups=True)

    result_df = result_df.reset_index(drop=True)

    print(f"{BLUE}Cleaned persons vaccinated data{BASIC}")

    result_df = result_df.groupby('location_key').apply(process_group, new_column_name='new_persons_fully_vaccinated',
                                                        cumulative_column_name='cumulative_persons_fully_vaccinated',
                                                        include_groups=True)

    result_df = result_df.reset_index(drop=True)

    print(f"{BLUE}Cleaned persons fully vaccinated data{BASIC}")

    result_df = result_df.groupby('location_key').apply(process_group, new_column_name='new_vaccine_doses_administered',
                                                        cumulative_column_name='cumulative_vaccine_doses_administered',
                                                        include_groups=True)

    result_df = result_df.reset_index(drop=True)

    print(f"{BLUE}Cleaned vaccine doses administered data{BASIC}")

    print(f"{BLUE}Cleaned vaccination data{BASIC}")

    return result_df

def clean_health_data(dataframe):
    dataframe['date'] = pd.to_datetime(dataframe['date'])
    
    df_doctors = pd.read_csv('data/data_doctors.csv')
    df_nurses = pd.read_csv('data/data_nurses.csv')
    df_smoking = pd.read_csv('data/data_smoking.csv', sep=';')

    for index, row in dataframe.iterrows():
        if (pd.isna(row['physicians_per_1000'])) & (row['iso_3166_1_alpha_3'] in df_doctors['SpatialDimValueCode'].values):
            country_doctors = df_doctors.loc[df_doctors['SpatialDimValueCode'] == row['iso_3166_1_alpha_3']].copy()

            # Znajdź rok najbliższy do roku w aktualnym wierszu
            country_doctors['year_diff'] = abs(country_doctors['Period'] - row['date'].year)
            closest_match = country_doctors.loc[country_doctors['year_diff'].idxmin()]

            dataframe.loc[index, 'physicians_per_1000'] = closest_match['Value'] / 10
        else:
            dataframe.loc[index, 'physicians_per_1000'] = 0

        if (pd.isna(row['nurses_per_1000'])) & (row['iso_3166_1_alpha_3'] in df_nurses['SpatialDimValueCode'].values):
            country_nurses = df_nurses.loc[df_nurses['SpatialDimValueCode'] == row['iso_3166_1_alpha_3']].copy()

            # Znajdź rok najbliższy do roku w aktualnym wierszu
            country_nurses['year_diff'] = abs(country_nurses['Period'] - row['date'].year)
            closest_match = country_nurses.loc[country_nurses['year_diff'].idxmin()]

            dataframe.loc[index, 'nurses_per_1000'] = closest_match['Value'] / 10
        else:
            dataframe.loc[index, 'nurses_per_1000'] = 0

        if (pd.isna(row['smoking_prevalence'])) & (row['iso_3166_1_alpha_3'] in df_smoking['Country Code'].values):
            country_smoking = df_smoking.loc[df_smoking['Country Code'] == row['iso_3166_1_alpha_3']].copy()

            # Zbierz wszystkie kolumny, które da się zinterpretować jako lata, np. "1960", "1961", ... "2023"
            year_columns = [col for col in country_smoking.columns if col.isdigit()]

            # Zamieniamy nazwy kolumn (string) na liczby całkowite
            numeric_years = [int(y) for y in year_columns]

            # Odfiltruj tylko te lata, gdzie wartość nie jest NaN
            non_empty_years = {}
            for year in numeric_years:
                val = country_smoking[str(year)].values[0]
                if not pd.isna(val):
                    non_empty_years[year] = val

            if len(non_empty_years) > 0:
                # Znajdź rok, którego różnica względem row['date'].year jest najmniejsza
                target_year = row['date'].year
                year_diffs = {year: abs(year - target_year) for year in non_empty_years}
                closest_year = min(year_diffs, key=year_diffs.get)  # wybieramy ten rok, który ma najmniejszą różnicę

                # Pobierz wartość z dataframe'u dla tego najbliższego roku
                closest_value = non_empty_years[closest_year]

                closest_value = pd.to_numeric(str(closest_value).replace(',', '.'), errors='coerce')

                # Przypisz do 'smoking_prevalence' w głównym dataframe
                dataframe.loc[index, 'smoking_prevalence'] = closest_value

    print(f"{BLUE}Cleaned health indicators data{BASIC}")

    return dataframe


def main():
    start_time = time.time()

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "keys/ferrous-destiny-424600-h9-2ab5d0de9937.json" # path to API key
    client = bigquery.Client()

    # Create catalog if not exists
    folder = os.path.dirname("exported/")
    if folder:
        os.makedirs(folder, exist_ok=True)

    # 4.1 data on all countries of the world, comprehensible to humans and universal and potentially future-proof for further processing.
    # iso_3166_1_alpha_3, country_name
    print(f"\n\n{GREEN}Started extracting and cleaning countries data{BASIC}")

    query = ('select location_key, date, iso_3166_1_alpha_3, wikidata_id, aggregation_level, country_name from bigquery-public-data.covid19_open_data.covid19_open_data where aggregation_level = 0')
    query_job = client.query(query)
    query_result = query_job.result()
    df1 = query_result.to_dataframe()

    print(f"Number of records with empty fields: {df1.isnull().sum().sum()}")
    print(f"Number of records with empty fields in iso_3166_1_alpha_3: {df1['iso_3166_1_alpha_3'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in country_name: {df1['country_name'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in location_key: {df1['location_key'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in wikidata_id: {df1['wikidata_id'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in aggregation_level: {df1['aggregation_level'].isnull().sum().sum()}")

    df1 = clean_countries_data(df1)

    print(f"Number of records with empty fields: {df1.isnull().sum().sum()}")
    print(f"Number of records with empty fields in iso_3166_1_alpha_3: {df1['iso_3166_1_alpha_3'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in country_name: {df1['country_name'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in location_key: {df1['location_key'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in wikidata_id: {df1['wikidata_id'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in aggregation_level: {df1['aggregation_level'].isnull().sum().sum()}")

    df1.to_csv('exported/countries.csv', index=False)
    print(f"{GREEN}Ended extracting and cleaning countries data{BASIC}")

    # 4.2 COVID-19 incidence data worldwide
    # date, new_confirmed, cumulative_confirmed, new_tested, cumulative_tested
    print(f"\n\n{GREEN}Started extracting and cleaning COVID-19 incidents data{BASIC}")

    query = ('select location_key, date, new_confirmed, cumulative_confirmed, new_tested, cumulative_tested from bigquery-public-data.covid19_open_data.covid19_open_data where aggregation_level = 0')
    query_job = client.query(query)
    query_result = query_job.result()
    df2 = query_result.to_dataframe()

    print(f"Number of records with empty fields: {df2.isnull().sum().sum()}")
    print(f"Number of records with empty fields in date: {df2['date'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in new_confirmed: {df2['new_confirmed'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in cumulative_confirmed: {df2['cumulative_confirmed'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in new_tested: {df2['new_tested'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in cumulative_tested: {df2['cumulative_tested'].isnull().sum().sum()}")

    df2 = clean_incidence_data(df2)

    print(f"Number of records with empty fields: {df2.isnull().sum().sum()}")
    print(f"Number of records with empty fields in date: {df2['date'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in new_confirmed: {df2['new_confirmed'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in cumulative_confirmed: {df2['cumulative_confirmed'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in new_tested: {df2['new_tested'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in cumulative_tested: {df2['cumulative_tested'].isnull().sum().sum()}")

    df2.to_csv('exported/incidence.csv', index=False)

    print(f"{GREEN}Ended extracting and cleaning COVID-19 incidents data{BASIC}")

    # 4.3 data on the problem of human mortality caused by the virus
    # new_deceased, cumulative_deceased,
    print(f"\n\n{GREEN}Started extracting and cleaning human mortality data{BASIC}")

    query = ('select location_key, date, new_deceased, cumulative_deceased from bigquery-public-data.covid19_open_data.covid19_open_data where aggregation_level = 0')
    query_job = client.query(query)
    query_result = query_job.result()
    df3 = query_result.to_dataframe()

    print(f"Number of records with empty fields: {df3.isnull().sum().sum()}")
    print(f"Number of records with empty fields in new_deceased: {df3['new_deceased'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in cumulative_deceased: {df3['cumulative_deceased'].isnull().sum().sum()}")

    df3 = clean_mortality_data(df3)

    print(f"Number of records with empty fields: {df3.isnull().sum().sum()}")
    print(f"Number of records with empty fields in new_deceased: {df3['new_deceased'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in cumulative_deceased: {df3['cumulative_deceased'].isnull().sum().sum()}")

    df3.to_csv('exported/mortality.csv', index=False)

    print(f"{GREEN}Ended extracting and cleaning human mortality data{BASIC}")

    # 4.4 COVID-19 vaccination data
    # new_persons_vaccinated, new_persons_fully_vaccinated, cumulative_persons_vaccinated, new_vaccine_doses_administered, cumulative_vaccine_doses_administered
    print(f"\n\n{GREEN}Started extracting and cleaning vaccination data{BASIC}")

    query = ('select location_key, date, new_persons_vaccinated, cumulative_persons_vaccinated, new_persons_fully_vaccinated, cumulative_persons_fully_vaccinated, new_vaccine_doses_administered, cumulative_vaccine_doses_administered from bigquery-public-data.covid19_open_data.covid19_open_data where aggregation_level = 0')
    query_job = client.query(query)
    query_result = query_job.result()
    df4 = query_result.to_dataframe()

    print(f"Number of records with empty fields: {df4.isnull().sum().sum()}")
    print(f"Number of records with empty fields in new_persons_vaccinated: {df4['new_persons_vaccinated'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in cumulative_persons_vaccinated: {df4['cumulative_persons_vaccinated'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in new_persons_fully_vaccinated: {df4['new_persons_fully_vaccinated'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in cumulative_persons_vaccinated: {df4['cumulative_persons_fully_vaccinated'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in new_vaccine_doses_administered: {df4['new_vaccine_doses_administered'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in cumulative_vaccine_doses_administered: {df4['cumulative_vaccine_doses_administered'].isnull().sum().sum()}")

    df4 = clean_vaccination_data(df4)

    print(f"Number of records with empty fields: {df4.isnull().sum().sum()}")
    print(f"Number of records with empty fields in new_persons_vaccinated: {df4['new_persons_vaccinated'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in cumulative_persons_vaccinated: {df4['cumulative_persons_vaccinated'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in new_persons_fully_vaccinated: {df4['new_persons_fully_vaccinated'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in cumulative_persons_vaccinated: {df4['cumulative_persons_fully_vaccinated'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in new_vaccine_doses_administered: {df4['new_vaccine_doses_administered'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in cumulative_vaccine_doses_administered: {df4['cumulative_vaccine_doses_administered'].isnull().sum().sum()}")

    df4.to_csv('exported/vaccination.csv', index=False)

    print(f"{GREEN}Ended extracting and cleaning vaccination data{BASIC}")

    # 4.5 the state of health of the population
    # smoking_prevalence, diabetes_prevalence, infant_mortality_rate, nurses_per_1000, physicians_per_1000, health_expenditure_usd
    print(f"\n\n{GREEN}Started extracting and cleaning the state of health of the population data{BASIC}")

    query = ('select location_key, date, iso_3166_1_alpha_3, smoking_prevalence, diabetes_prevalence, infant_mortality_rate, nurses_per_1000, physicians_per_1000, health_expenditure_usd from bigquery-public-data.covid19_open_data.covid19_open_data where aggregation_level = 0')
    query_job = client.query(query)
    query_result = query_job.result()
    df5 = query_result.to_dataframe()

    print(f"Number of records with empty fields: {df5.isnull().sum().sum()}")
    print(f"Number of records with empty fields in smoking_prevalence: {df5['smoking_prevalence'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in diabetes_prevalence: {df5['diabetes_prevalence'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in infant_mortality_rate: {df5['infant_mortality_rate'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in nurses_per_1000: {df5['nurses_per_1000'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in physicians_per_1000: {df5['physicians_per_1000'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in health_expenditure_usd: {df5['health_expenditure_usd'].isnull().sum().sum()}")

    df5 = clean_health_data(df5)

    print(f"Number of records with empty fields: {df5.isnull().sum().sum()}")
    print(f"Number of records with empty fields in smoking_prevalence: {df5['smoking_prevalence'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in diabetes_prevalence: {df5['diabetes_prevalence'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in infant_mortality_rate: {df5['infant_mortality_rate'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in nurses_per_1000: {df5['nurses_per_1000'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in physicians_per_1000: {df5['physicians_per_1000'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in health_expenditure_usd: {df5['health_expenditure_usd'].isnull().sum().sum()}")

    df5.to_csv('exported/health.csv', index=False)

    print(f"{GREEN}Ended extracting and cleaning the state of health of the population data{BASIC}")

    # 5 Combine all dataframes into one
    print(f"\n\n{GREEN}Started linking data frames{BASIC}")

    # Combine all dataframes into one dataframe
    #combined_df = pd.concat([df1, df2, df3, df4, df5], axis=1)
    combined_df = df1.merge(df2, on=["location_key", "date"], how="inner")
    combined_df = combined_df.merge(df3, on=["location_key", "date"], how="inner")
    combined_df = combined_df.merge(df4, on=["location_key", "date"], how="inner")
    combined_df = combined_df.merge(df5, on=["location_key", "date"], how="inner")

    #combined_df = combined_df[combined_df['aggregation_level'] == 0]
    # Group by country and sort by date
    combined_df = combined_df.sort_values(by=['location_key', 'date'])

    # Filter and save each level directly to a file
    for level in [0, 1, 2, 3]:
        combined_df.query(f"aggregation_level == {level}").to_csv(f'exported/combined_level_{level}.csv', index=False)

    # Saving the combined dataframe to a CSV file
    combined_df.to_csv('exported/combined.csv', index=False)

    print(f"{GREEN}Ended merging data frames and saved to exported/combined.csv{BASIC}")

    end_time = time.time()

    time_in_seconds = end_time - start_time

    # Conversion to desired format
    minutes = int(time_in_seconds // 60)
    seconds = int(time_in_seconds % 60)

    # Time in format MM:SS
    print(f"Execution time: {minutes:02d}:{seconds:02d}")


if __name__ == '__main__':
    main()


