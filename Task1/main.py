import os
import time

import numpy as np
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
    
    7. Jeśli aktualne pole new nie jest puste, ale aktualne pole cumulative jest puste, 
    uzupełnij je o sumę wartości poprzedniego pola cumulative oraz aktualnego pola new.
    """
    # Implementation of the algorithm above for the indicated new and cumulative columns

    # Check that all values are empty (point 4)
    if sorted_group[new_column_name].isna().all() and sorted_group[cumulative_column_name].isna().all():
        sorted_group[new_column_name] = 0
        sorted_group[cumulative_column_name] = 0
        return sorted_group

    # Handling of the first line (point 5)
    if pd.isna(sorted_group[new_column_name].iloc[0]) and pd.isna(sorted_group[cumulative_column_name].iloc[0]):
        sorted_group.loc[sorted_group.index[0], new_column_name] = 0
        sorted_group.loc[sorted_group.index[0], cumulative_column_name] = 0
    elif pd.isna(sorted_group[new_column_name].iloc[0]):
        sorted_group[new_column_name].iloc[0] = sorted_group[cumulative_column_name].iloc[0]
    elif pd.isna(sorted_group[cumulative_column_name].iloc[0]):
        sorted_group[cumulative_column_name].iloc[0] = sorted_group[new_column_name].iloc[0]

    # Iterative processing of the remaining rows
    prev_cumulative = sorted_group[cumulative_column_name].iloc[0]
    for i in range(1, len(sorted_group)):
        current_new = sorted_group[new_column_name].iloc[i]
        current_cumulative = sorted_group[cumulative_column_name].iloc[i]

        # Point 1: Empty new, non-empty cumulative
        if pd.isna(current_new) and not pd.isna(current_cumulative):
            sorted_group.loc[sorted_group.index[i], new_column_name] = current_cumulative - prev_cumulative

        # Point 7: Non-empty new, empty cumulative
        elif not pd.isna(current_new) and pd.isna(current_cumulative):
            sorted_group.loc[sorted_group.index[i], cumulative_column_name] = prev_cumulative + current_new

        # Points 2 and 3: Empty cumulative
        elif pd.isna(current_cumulative):
            # Looking for the next non-empty cumulative value
            next_non_na_index = None
            for j in range(i + 1, len(sorted_group)):
                if not pd.isna(sorted_group[cumulative_column_name].iloc[j]):
                    next_non_na_index = j
                    break

            if next_non_na_index is not None:
                next_cumulative = sorted_group[cumulative_column_name].iloc[next_non_na_index]
                total_diff = next_cumulative - prev_cumulative

                # Subtraction of all non-empty 'new' values in range
                non_na_new_sum = 0
                empty_new_count = 0
                for j in range(i, next_non_na_index):
                    if not pd.isna(sorted_group[new_column_name].iloc[j]):
                        non_na_new_sum += sorted_group[new_column_name].iloc[j]
                    else:
                        empty_new_count += 1

                remaining_diff = total_diff - non_na_new_sum

                # If there are empty 'new' fields, we fill them evenly
                if empty_new_count > 0:
                    value_per_empty = round(remaining_diff / empty_new_count)
                    for j in range(i, next_non_na_index):
                        if pd.isna(sorted_group[new_column_name].iloc[j]):
                            sorted_group.loc[sorted_group.index[j], new_column_name] = value_per_empty

                # Completing the current cumulative field
                if not pd.isna(sorted_group[new_column_name].iloc[i]):
                    sorted_group.loc[sorted_group.index[i], cumulative_column_name] = prev_cumulative + sorted_group.loc[sorted_group.index[i], new_column_name]
            else:
                # Point 6: Empty 'new' and 'cumulative', and no future non-empty 'cumulative'
                if pd.isna(current_new):
                    sorted_group.loc[sorted_group.index[i], new_column_name] = 0
                sorted_group.loc[sorted_group.index[i], cumulative_column_name] = (
                        prev_cumulative + sorted_group.loc[sorted_group.index[i], new_column_name])

        # Update prev_cumulative for next iteration
        if not pd.isna(sorted_group[cumulative_column_name].iloc[i]):
            prev_cumulative = sorted_group[cumulative_column_name].iloc[i]

    return sorted_group


def fix_negative_values(dataframe, new_col, cum_col):
    """
    Funkcja przyjmuje DataFrame zawierający kolumny z danymi numerycznymi new oraz cumulative
    oraz grupuje dane po kolumnie 'location_key'. Dla każdej grupy dokonuje następujących operacji:
      1. Zmiana ujemnych wartości w kolumnie new na ich wartość bezwzględną.
      2. Przeliczenie wartości cumulative:
         - Jeśli dany rekord jest pierwszy lub poprzednia wartość cumulative jest NaN,
           ustawia cumulative = new.
         - W przeciwnym przypadku cumulative = poprzednie cumulative + bieżące new.
      3. Ostateczna korekta cumulative – dla każdego rekordu (od drugiego) wartość cumulative
         zostaje przeliczona jako suma poprzedniego cumulative oraz bieżącego new.

    Args:
        dataframe (pd.DataFrame): DataFrame zawierający kolumny new, cumulative oraz iso_3166_1_alpha_3.
        new_col (str): Nazwa kolumny zawierającej wartości new.
        cum_col (str): Nazwa kolumny zawierającej wartości cumulative.

    Returns:
        pd.DataFrame: Przetworzony DataFrame z poprawionymi wartościami cumulative.
    """

    def process_group(group: pd.DataFrame) -> pd.DataFrame:
        # Tworzymy kopię grupy aby nie modyfikować oryginalnego DataFrame
        df_group = group.copy()

        # Upewniamy się, że kolumna cumulative istnieje; w przeciwnym razie ją tworzymy
        if cum_col not in df_group.columns:
            df_group[cum_col] = np.nan

        # 1. Zamiana ujemnych wartości new na dodatnie
        df_group[new_col] = df_group[new_col].apply(lambda x: abs(x))

        # 2. Pierwsza iteracja – przeliczanie cumulative
        # Wartości cumulative ustawiamy początkowo na NaN
        df_group[cum_col] = np.nan
        for i in range(len(df_group)):
            if i == 0 or pd.isna(df_group.iloc[i - 1][cum_col]):
                df_group.at[df_group.index[i], cum_col] = df_group.iloc[i][new_col]
            else:
                df_group.at[df_group.index[i], cum_col] = df_group.iloc[i - 1][cum_col] + df_group.iloc[i][new_col]

        # 3. Ostateczna korekta – ponowne przeliczenie cumulative
        for i in range(1, len(df_group)):
            df_group.at[df_group.index[i], cum_col] = df_group.at[df_group.index[i - 1], cum_col] + df_group.at[
                df_group.index[i], new_col]

        return df_group

    # Grupujemy dane po kolumnie kodów państw i przetwarzamy każdą grupę osobno
    df_fixed = dataframe.groupby('location_key', group_keys=False).apply(process_group)

    return df_fixed


def clean_incidence_data(dataframe):
    dataframe['date'] = pd.to_datetime(dataframe['date'])
    dataframe['new_confirmed'] = pd.to_numeric(dataframe['new_confirmed'], errors='coerce').astype('Int64')
    dataframe['cumulative_confirmed'] = pd.to_numeric(dataframe['cumulative_confirmed'], errors='coerce').astype('Int64')
    dataframe['new_tested'] = pd.to_numeric(dataframe['new_tested'], errors='coerce').astype('Int64')
    dataframe['cumulative_tested'] = pd.to_numeric(dataframe['cumulative_tested'], errors='coerce').astype('Int64')

    result_df = dataframe.groupby('location_key').apply(process_group, new_column_name='new_confirmed',
                                                                       cumulative_column_name='cumulative_confirmed',
                                                                       include_groups=True)

    result_df = result_df.reset_index(drop=True)

    result_df = fix_negative_values(result_df, 'new_confirmed', 'cumulative_confirmed')

    print(f"{BLUE}Cleaned COVID-19 confirmed incidents data{BASIC}")

    result_df = result_df.groupby('location_key').apply(process_group, new_column_name='new_tested',
                                                                       cumulative_column_name='cumulative_tested',
                                                                       include_groups=True)

    result_df = result_df.reset_index(drop=True)

    result_df = fix_negative_values(result_df, 'new_tested', 'cumulative_tested')

    print(f"{BLUE}Cleaned COVID-19 tests data{BASIC}")

    result_df = result_df.groupby('location_key').apply(process_group, new_column_name='new_recovered',
                                                        cumulative_column_name='cumulative_recovered',
                                                        include_groups=True)

    result_df = result_df.reset_index(drop=True)

    result_df = fix_negative_values(result_df, 'new_recovered', 'cumulative_recovered')

    print(f"{BLUE}Cleaned COVID-19 recovery data{BASIC}")

    print(f"{BLUE}Cleaned COVID-19 incidents data{BASIC}")

    return result_df

def clean_mortality_data(dataframe):
    dataframe['date'] = pd.to_datetime(dataframe['date'])
    dataframe['new_deceased'] = pd.to_numeric(dataframe['new_deceased'], errors='coerce').astype('Int64')
    dataframe['cumulative_deceased'] = pd.to_numeric(dataframe['cumulative_deceased'], errors='coerce').astype('Int64')

    result_df = dataframe.groupby('location_key').apply(process_group, new_column_name='new_deceased',
                                                        cumulative_column_name='cumulative_deceased',
                                                        include_groups=True)

    result_df = result_df.reset_index(drop=True)

    result_df = fix_negative_values(result_df, 'new_deceased', 'cumulative_deceased')

    print(f"{BLUE}Cleaned COVID-19 mortality data{BASIC}")

    return result_df

def clean_vaccination_data(dataframe):
    dataframe['date'] = pd.to_datetime(dataframe['date'])
    dataframe['new_persons_vaccinated'] = pd.to_numeric(dataframe['new_persons_vaccinated'], errors='coerce').astype('Int64')
    dataframe['cumulative_persons_vaccinated'] = pd.to_numeric(dataframe['cumulative_persons_vaccinated'], errors='coerce').astype('Int64')
    dataframe['new_persons_fully_vaccinated'] = pd.to_numeric(dataframe['new_persons_fully_vaccinated'], errors='coerce').astype('Int64')
    dataframe['cumulative_persons_fully_vaccinated'] = pd.to_numeric(dataframe['cumulative_persons_fully_vaccinated'], errors='coerce').astype('Int64')
    dataframe['new_vaccine_doses_administered'] = pd.to_numeric(dataframe['new_vaccine_doses_administered'], errors='coerce').astype('Int64')
    dataframe['cumulative_vaccine_doses_administered'] = pd.to_numeric(dataframe['cumulative_vaccine_doses_administered'], errors='coerce').astype('Int64')

    result_df = dataframe.groupby('location_key').apply(process_group, new_column_name='new_persons_vaccinated',
                                                        cumulative_column_name='cumulative_persons_vaccinated',
                                                        include_groups=True)

    result_df = result_df.reset_index(drop=True)

    result_df = fix_negative_values(result_df, 'new_persons_vaccinated', 'cumulative_persons_vaccinated')

    print(f"{BLUE}Cleaned persons vaccinated data{BASIC}")

    result_df = result_df.groupby('location_key').apply(process_group, new_column_name='new_persons_fully_vaccinated',
                                                        cumulative_column_name='cumulative_persons_fully_vaccinated',
                                                        include_groups=True)

    result_df = result_df.reset_index(drop=True)

    result_df = fix_negative_values(result_df, 'new_persons_fully_vaccinated', 'cumulative_persons_fully_vaccinated')

    print(f"{BLUE}Cleaned persons fully vaccinated data{BASIC}")

    result_df = result_df.groupby('location_key').apply(process_group, new_column_name='new_vaccine_doses_administered',
                                                        cumulative_column_name='cumulative_vaccine_doses_administered',
                                                        include_groups=True)

    result_df = result_df.reset_index(drop=True)

    result_df = fix_negative_values(result_df, 'new_vaccine_doses_administered', 'cumulative_vaccine_doses_administered')

    print(f"{BLUE}Cleaned vaccine doses administered data{BASIC}")

    print(f"{BLUE}Cleaned vaccination data{BASIC}")

    return result_df

def clean_health_data(dataframe):
    dataframe['date'] = pd.to_datetime(dataframe['date'])

    df_doctors = pd.read_csv('data/data_doctors.csv')
    df_doctors2 = pd.read_csv('data/data_doctors2.csv')
    df_nurses = pd.read_csv('data/data_nurses.csv')
    df_nurses2 = pd.read_csv('data/data_nurses2.csv', sep=";")
    df_smoking = pd.read_csv('data/data_smoking.csv', sep=';')
    df_diabetes = pd.read_csv('data/data_diabetes2.csv', sep=';')
    df_diabetes2 = pd.read_csv('data/data_diabetes.csv')
    df_beds = pd.read_csv('data/data_beds.csv')
    df_beds2 = pd.read_csv('data/data_beds2.csv', sep=";")
    df_current_health_expenditure = pd.read_csv('data/data_current_health_expenditure.csv')

    for index, row in dataframe.iterrows():
        if (pd.isna(row['physicians_per_1000'])) & (row['iso_3166_1_alpha_3'] in df_doctors['SpatialDimValueCode'].values):
            country_doctors = df_doctors.loc[df_doctors['SpatialDimValueCode'] == row['iso_3166_1_alpha_3']].copy()

            # Find the year closest to the year in the current row
            country_doctors['year_diff'] = abs(country_doctors['Period'] - row['date'].year)
            closest_match = country_doctors.loc[country_doctors['year_diff'].idxmin()]

            dataframe.loc[index, 'physicians_per_1000'] = closest_match['Value'] / 10

        if (pd.isna(row['physicians_per_1000'])) & (row['iso_3166_1_alpha_3'] in df_doctors2['Country Code'].values):
            country_doctors2 = df_doctors2.loc[df_doctors2['Country Code'] == row['iso_3166_1_alpha_3']].copy()

            # Collect all columns that represent years, under the form: “<year> [YR<year>]”.
            year_columns = []
            for col in country_doctors2.columns:
                if '[YR' in col:
                    possible_year = col.split()[0]

                    if possible_year.isdigit():
                        year_columns.append(col)

            # Create a dictionary: {year_int: value}
            non_empty_years = {}
            for col in year_columns:
                # Read the year (the part before the space)
                year_str = col.split()[0]  # "2015" from "2015 [YR2015]"
                val = country_doctors2[col].values[0]
                val = pd.to_numeric(val, errors='coerce')
                if (year_str.isdigit()) and not pd.isna(val):
                    non_empty_years[int(year_str)] = val

            # If we have any non-empty values for a country
            if len(non_empty_years) > 0:
                target_year = row['date'].year
                year_diffs = {year: abs(year - target_year) for year in non_empty_years}

                # Find the year with the smallest difference from target_year
                closest_year = min(year_diffs, key=year_diffs.get)

                closest_value = non_empty_years[closest_year]

                #closest_value = pd.to_numeric(str(closest_value).replace(',', '.'), errors='coerce')

                dataframe.loc[index, 'physicians_per_1000'] = closest_value
            else:
                dataframe.loc[index, 'physicians_per_1000'] = 0
        else:
            dataframe.loc[index, 'physicians_per_1000'] = 0

        if (pd.isna(row['nurses_per_1000'])) & (row['iso_3166_1_alpha_3'] in df_nurses['SpatialDimValueCode'].values):
            country_nurses = df_nurses.loc[df_nurses['SpatialDimValueCode'] == row['iso_3166_1_alpha_3']].copy()

            # Find the year closest to the year in the current row
            country_nurses['year_diff'] = abs(country_nurses['Period'] - row['date'].year)
            closest_match = country_nurses.loc[country_nurses['year_diff'].idxmin()]

            dataframe.loc[index, 'nurses_per_1000'] = closest_match['Value'] / 10

        if (pd.isna(row['nurses_per_1000'])) & (row['iso_3166_1_alpha_3'] in df_doctors2['Country Code'].values):
            country_nurses2 = df_nurses2.loc[df_nurses2['Country Code'] == row['iso_3166_1_alpha_3']].copy()

            # Collect all columns that can be interpreted as years, e.g. “1960”, “1961”, .... “2023”
            year_columns = [col for col in country_nurses2.columns if col.isdigit()]

            # Convert column names (string) to integers
            numeric_years = [int(y) for y in year_columns]

            # Filter out only those years where the value is not NaN
            non_empty_years = {}
            for year in numeric_years:
                val = country_nurses2[str(year)].values[0]
                if not pd.isna(val):
                    non_empty_years[year] = val

            if len(non_empty_years) > 0:
                # Find the year whose difference from row['date'].year is the smallest
                target_year = row['date'].year
                year_diffs = {year: abs(year - target_year) for year in non_empty_years}
                closest_year = min(year_diffs, key=year_diffs.get)  # Choose the year that has the smallest difference

                # Get the value from the dataframe for this upcoming year
                closest_value = non_empty_years[closest_year]

                closest_value = pd.to_numeric(str(closest_value).replace(',', '.'), errors='coerce')

                # Assign to 'nurses_per_1000' in the main dataframe
                dataframe.loc[index, 'nurses_per_1000'] = closest_value
            else:
                dataframe.loc[index, 'nurses_per_1000'] = 0
        else:
            dataframe.loc[index, 'nurses_per_1000'] = 0

        if (pd.isna(row['smoking_prevalence'])) & (row['iso_3166_1_alpha_3'] in df_smoking['Country Code'].values):
            country_smoking = df_smoking.loc[df_smoking['Country Code'] == row['iso_3166_1_alpha_3']].copy()

            # Collect all columns that can be interpreted as years, e.g. “1960”, “1961”, .... “2023”
            year_columns = [col for col in country_smoking.columns if col.isdigit()]

            # Convert column names (string) to integers
            numeric_years = [int(y) for y in year_columns]

            # Filter out only those years where the value is not NaN
            non_empty_years = {}
            for year in numeric_years:
                val = country_smoking[str(year)].values[0]
                if not pd.isna(val):
                    non_empty_years[year] = val

            if len(non_empty_years) > 0:
                # Find the year whose difference from row['date'].year is the smallest
                target_year = row['date'].year
                year_diffs = {year: abs(year - target_year) for year in non_empty_years}
                closest_year = min(year_diffs, key=year_diffs.get)  # Choose the year that has the smallest difference

                # Get the value from the dataframe for this upcoming year
                closest_value = non_empty_years[closest_year]

                closest_value = pd.to_numeric(str(closest_value).replace(',', '.'), errors='coerce')

                # Assign to 'smoking_prevalence' in the main dataframe
                dataframe.loc[index, 'smoking_prevalence'] = closest_value
            else:
                dataframe.loc[index, 'smoking_prevalence'] = 0
        else:
            dataframe.loc[index, 'smoking_prevalence'] = 0

        if (pd.isna(row['diabetes_prevalence'])) & (row['iso_3166_1_alpha_3'] in df_diabetes['Country Code'].values):
            country_diabetes = df_diabetes.loc[df_diabetes['Country Code'] == row['iso_3166_1_alpha_3']].copy()

            # Collect all columns that can be interpreted as years, e.g. “1960”, “1961”, .... “2023”
            year_columns = [col for col in country_diabetes.columns if col.isdigit()]

            # Convert column names (string) to integers
            numeric_years = [int(y) for y in year_columns]

            # Filter out only those years where the value is not NaN
            non_empty_years = {}
            for year in numeric_years:
                val = country_diabetes[str(year)].values[0]
                if not pd.isna(val):
                    non_empty_years[year] = val

            if len(non_empty_years) > 0:
                # Find the year whose difference from row['date'].year is the smallest
                target_year = row['date'].year
                year_diffs = {year: abs(year - target_year) for year in non_empty_years}
                closest_year = min(year_diffs, key=year_diffs.get)  # Choose the year that has the smallest difference

                # Get the value from the dataframe for this upcoming year
                closest_value = non_empty_years[closest_year]

                closest_value = pd.to_numeric(str(closest_value).replace(',', '.'), errors='coerce')

                # Assign to 'diabetes_prevalence' in the main dataframe
                dataframe.loc[index, 'diabetes_prevalence'] = closest_value

        if (pd.isna(row['diabetes_prevalence'])) & (row['iso_3166_1_alpha_3'] in df_diabetes2['Country Code'].values):
            country_diabetes = df_diabetes2.loc[df_diabetes2['Country Code'] == row['iso_3166_1_alpha_3']].copy()

            # Collect all columns that represent years, under the form: “<year> [YR<year>]”.
            year_columns = []
            for col in country_diabetes.columns:
                if '[YR' in col:
                    possible_year = col.split()[0]

                    if possible_year.isdigit():
                        year_columns.append(col)

            # Create a dictionary: {year_int: value}
            non_empty_years = {}
            for col in year_columns:
                # Read the year (the part before the space)
                year_str = col.split()[0]  # "2015" from "2015 [YR2015]"
                val = country_diabetes[col].values[0]
                val = pd.to_numeric(val, errors='coerce')
                if (year_str.isdigit()) and not pd.isna(val):
                    non_empty_years[int(year_str)] = val

            # If we have any non-empty values for a country
            if len(non_empty_years) > 0:
                target_year = row['date'].year
                year_diffs = {year: abs(year - target_year) for year in non_empty_years}

                # Find the year with the smallest difference from target_year
                closest_year = min(year_diffs, key=year_diffs.get)

                closest_value = non_empty_years[closest_year]

                #closest_value = pd.to_numeric(str(closest_value).replace(',', '.'), errors='coerce')

                dataframe.loc[index, 'diabetes_prevalence'] = closest_value
            else:
                dataframe.loc[index, 'diabetes_prevalence'] = 0
        else:
            dataframe.loc[index, 'diabetes_prevalence'] = 0

        if (pd.isna(row['hospital_beds_per_1000'])) & (row['iso_3166_1_alpha_3'] in df_beds['SpatialDimValueCode'].values):
            country_beds = df_beds.loc[df_beds['SpatialDimValueCode'] == row['iso_3166_1_alpha_3']].copy()

            # Find the year closest to the year in the current row
            country_beds['year_diff'] = abs(country_beds['Period'] - row['date'].year)
            closest_match = country_beds.loc[country_beds['year_diff'].idxmin()]

            dataframe.loc[index, 'hospital_beds_per_1000'] = closest_match['Value'] / 10

        if (pd.isna(row['hospital_beds_per_1000'])) & (row['iso_3166_1_alpha_3'] in df_beds2['Country Code'].values):
            country_beds2 = df_beds2.loc[df_beds2['Country Code'] == row['iso_3166_1_alpha_3']].copy()

            # Collect all columns that can be interpreted as years, e.g. “1960”, “1961”, .... “2023”
            year_columns = [col for col in country_beds2.columns if col.isdigit()]

            # Convert column names (string) to integers
            numeric_years = [int(y) for y in year_columns]

            # Filter out only those years where the value is not NaN
            non_empty_years = {}
            for year in numeric_years:
                val = country_beds2[str(year)].values[0]
                if not pd.isna(val):
                    non_empty_years[year] = val

            if len(non_empty_years) > 0:
                # Find the year whose difference from row['date'].year is the smallest
                target_year = row['date'].year
                year_diffs = {year: abs(year - target_year) for year in non_empty_years}
                closest_year = min(year_diffs, key=year_diffs.get)  # Choose the year that has the smallest difference

                # Get the value from the dataframe for this upcoming year
                closest_value = non_empty_years[closest_year]

                closest_value = pd.to_numeric(str(closest_value).replace(',', '.'), errors='coerce')

                # Assign to 'hospital_beds_per_1000' in the main dataframe
                dataframe.loc[index, 'hospital_beds_per_1000'] = closest_value
            else:
                dataframe.loc[index, 'hospital_beds_per_1000'] = 0
        else:
            dataframe.loc[index, 'hospital_beds_per_1000'] = 0

        if (pd.isna(row['health_expenditure_usd'])) & (row['iso_3166_1_alpha_3'] in df_current_health_expenditure['SpatialDimValueCode'].values):
            country_health_expenditure = df_current_health_expenditure.loc[df_current_health_expenditure['SpatialDimValueCode'] == row['iso_3166_1_alpha_3']].copy()

            # Find the year closest to the year in the current row
            country_health_expenditure['year_diff'] = abs(country_health_expenditure['Period'] - row['date'].year)
            closest_match = country_health_expenditure.loc[country_health_expenditure['year_diff'].idxmin()]

            dataframe.loc[index, 'health_expenditure_usd'] = closest_match['Value']
        else:
            dataframe.loc[index, 'health_expenditure_usd'] = 0

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

    query = ('select location_key, date, iso_3166_1_alpha_3, country_name '
             'from bigquery-public-data.covid19_open_data.covid19_open_data '
             'where aggregation_level = 0')
    query_job = client.query(query)
    query_result = query_job.result()
    df1 = query_result.to_dataframe()

    print(f"Number of records with empty fields: {df1.isnull().sum().sum()}")
    print(f"Number of records with empty fields in iso_3166_1_alpha_3: {df1['iso_3166_1_alpha_3'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in country_name: {df1['country_name'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in location_key: {df1['location_key'].isnull().sum().sum()}")
#    print(f"Number of records with empty fields in wikidata_id: {df1['wikidata_id'].isnull().sum().sum()}")
#    print(f"Number of records with empty fields in aggregation_level: {df1['aggregation_level'].isnull().sum().sum()}")

    df1 = clean_countries_data(df1)

    print(f"Number of records with empty fields: {df1.isnull().sum().sum()}")
    print(f"Number of records with empty fields in iso_3166_1_alpha_3: {df1['iso_3166_1_alpha_3'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in country_name: {df1['country_name'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in location_key: {df1['location_key'].isnull().sum().sum()}")
#    print(f"Number of records with empty fields in wikidata_id: {df1['wikidata_id'].isnull().sum().sum()}")
#    print(f"Number of records with empty fields in aggregation_level: {df1['aggregation_level'].isnull().sum().sum()}")

    df1.to_csv('exported/countries.csv', index=False)
    print(f"{GREEN}Ended extracting and cleaning countries data{BASIC}")

    # 4.2 COVID-19 incidence data worldwide
    # date, new_confirmed, cumulative_confirmed, new_tested, cumulative_tested
    print(f"\n\n{GREEN}Started extracting and cleaning COVID-19 incidents data{BASIC}")

    query = ('select location_key, date, new_confirmed, cumulative_confirmed, new_tested, cumulative_tested,'
             'new_recovered, cumulative_recovered '
             'from bigquery-public-data.covid19_open_data.covid19_open_data '
             'where aggregation_level = 0')
    query_job = client.query(query)
    query_result = query_job.result()
    df2 = query_result.to_dataframe()

    print(f"Number of records with empty fields: {df2.isnull().sum().sum()}")
    print(f"Number of records with empty fields in date: {df2['date'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in new_confirmed: {df2['new_confirmed'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in cumulative_confirmed: {df2['cumulative_confirmed'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in new_tested: {df2['new_tested'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in cumulative_tested: {df2['cumulative_tested'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in new_recovered: {df2['new_recovered'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in cumulative_recovered: {df2['cumulative_recovered'].isnull().sum().sum()}")

    df2 = clean_incidence_data(df2)

    print(f"Number of records with empty fields: {df2.isnull().sum().sum()}")
    print(f"Number of records with empty fields in date: {df2['date'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in new_confirmed: {df2['new_confirmed'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in cumulative_confirmed: {df2['cumulative_confirmed'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in new_tested: {df2['new_tested'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in cumulative_tested: {df2['cumulative_tested'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in new_recovered: {df2['new_recovered'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in cumulative_recovered: {df2['cumulative_recovered'].isnull().sum().sum()}")

    df2.to_csv('exported/incidence.csv', index=False)

    print(f"{GREEN}Ended extracting and cleaning COVID-19 incidents data{BASIC}")

    # 4.3 data on the problem of human mortality caused by the virus
    # new_deceased, cumulative_deceased,
    print(f"\n\n{GREEN}Started extracting and cleaning human mortality data{BASIC}")

    query = ('select location_key, date, new_deceased, cumulative_deceased '
             'from bigquery-public-data.covid19_open_data.covid19_open_data '
             'where aggregation_level = 0')
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

    query = ('select location_key, date, new_persons_vaccinated, cumulative_persons_vaccinated, new_persons_fully_vaccinated, '
             'cumulative_persons_fully_vaccinated, new_vaccine_doses_administered, cumulative_vaccine_doses_administered '
             'from bigquery-public-data.covid19_open_data.covid19_open_data '
             'where aggregation_level = 0')
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

    query = ('select location_key, date, iso_3166_1_alpha_3, smoking_prevalence, diabetes_prevalence, '
             'hospital_beds_per_1000, nurses_per_1000, physicians_per_1000, health_expenditure_usd, '
             'population_urban, population_age_80_and_older, population_clustered, stringency_index, '
             'emergency_investment_in_healthcare, investment_in_vaccines, fiscal_measures '
             'from bigquery-public-data.covid19_open_data.covid19_open_data '
             'where aggregation_level = 0')
    query_job = client.query(query)
    query_result = query_job.result()
    df5 = query_result.to_dataframe()

    print(f"Number of records with empty fields: {df5.isnull().sum().sum()}")
    print(f"Number of records with empty fields in smoking_prevalence: {df5['smoking_prevalence'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in diabetes_prevalence: {df5['diabetes_prevalence'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in hospital_beds_per_1000: {df5['hospital_beds_per_1000'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in nurses_per_1000: {df5['nurses_per_1000'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in physicians_per_1000: {df5['physicians_per_1000'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in health_expenditure_usd: {df5['health_expenditure_usd'].isnull().sum().sum()}")

    df5 = clean_health_data(df5)

    print(f"Number of records with empty fields: {df5.isnull().sum().sum()}")
    print(f"Number of records with empty fields in smoking_prevalence: {df5['smoking_prevalence'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in diabetes_prevalence: {df5['diabetes_prevalence'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in hospital_beds_per_1000: {df5['hospital_beds_per_1000'].isnull().sum().sum()}")
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
    combined_df = combined_df.merge(df5, on=["location_key", "date"], how="inner", suffixes=('', '_from_right'))

    # 6 Combine countries data to COVID-19 data
    print(f"\n\n{GREEN}Started merging countries data frames{BASIC}")

    df_gdp = pd.read_csv('data/gdp.csv')
    #print(max(df_gdp['Year']))
    #print(min(df_gdp['Year']))
    #print(df_gdp['Year'].value_counts())
    #print(df_gdp['Country Code'].nunique())
    #print(df_gdp['Value'].isnull().sum().sum())

    # Sort the data by the 'Year' column
    df_gdp = df_gdp.sort_values(by='Year')

    # Group by Country Code and take the last row (maximum year)
    df_gdp = df_gdp.groupby('Country Code', as_index=False).last()

    df_gdp = df_gdp.rename(columns={'Value': 'GDP'})
    df_gdp = df_gdp.drop(columns=['Country Name', 'Year'])
    combined_df = combined_df.merge(df_gdp, left_on=["iso_3166_1_alpha_3"], right_on=['Country Code'], how="left", suffixes=('', '_from_right'))
    combined_df = combined_df.drop(columns=['Country Code'])

    print(f"{BLUE}Ended merging gdp data frames{BASIC}")

    df_countries = pd.read_csv('data/world_countries.csv')
    df_countries = df_countries[['Rank', 'CCA3', 'Capital', 'Continent', 'Area (km²)', 'Density (per km²)', 'Growth Rate', 'World Population Percentage', '2022 Population', '2020 Population']]

    # Calculate population in 2021 for each country
    df_countries['2021 Population'] = round((df_countries['2022 Population'] / df_countries['Growth Rate']), 0)

    # Convert df_countries table into long format
    df_countries_long = df_countries.melt(
        id_vars="CCA3",
        value_vars=["2020 Population", "2021 Population", "2022 Population"],
        var_name="Year",
        value_name="Population"
    )
    df_countries = df_countries.drop(columns=['2020 Population', '2021 Population', '2022 Population'])
    # Extract the year from the column name and write it in numeric format
    df_countries_long["Year"] = df_countries_long["Year"].str.extract(r'(\d{4})').astype(int)
    df_countries_long = df_countries_long.rename(columns={'CCA3': 'iso_3166_1_alpha_3'})
    df_countries = df_countries.rename(columns={'CCA3': 'iso_3166_1_alpha_3'})

    # Pulling a year out of the combined_df and combining after a year:
    combined_df["Year"] = combined_df["date"].dt.year
    combined_df = combined_df.merge(df_countries_long, on=["iso_3166_1_alpha_3", "Year"], how="left", suffixes=('', '_from_right'))

    combined_df = combined_df.merge(df_countries, on=["iso_3166_1_alpha_3"], how="left", suffixes=('', '_from_right'))

    #for col in combined_df.columns:
    #    print(f"Number of missing values in {col}: {combined_df[combined_df[col].isna()]}")

    print(f"{BLUE}Ended merging countries data frames{BASIC}")

    df_wei = pd.read_csv('data/world_economic_indicators.csv')
    df_wei = df_wei[['Country Code', 'Year', 'Unemployment, total (% of total labor force)']]
    df_wei['Year'] = df_wei['Year'].astype(int)
    df_wei = df_wei[(df_wei['Year'] >= 2020) & (df_wei['Year'] <= 2022)].copy()
    combined_df["Year"] = combined_df["date"].dt.year
    combined_df = combined_df.merge(df_wei, left_on=["iso_3166_1_alpha_3", "Year"], right_on=["Country Code", "Year"], how="left", suffixes=('', '_from_right'))

    df_suicides2020_2021 = pd.read_csv('data/suicide-rate-by-country.csv')
    df_suicides2022 = pd.read_csv('data/countries_indexes_2022.csv')

    df_suicides2020_2021 = df_suicides2020_2021[['SuicideRate_BothSexes_RatePer100k_2020', 'SuicideRate_BothSexes_RatePer100k_2021', 'country']]
    df_suicides2022 = df_suicides2022[['country', 'suicide_rate']]
    df_suicides2022 = df_suicides2022.rename(columns={'suicide_rate': 'SuicideRate_BothSexes_RatePer100k_2022'})
    df_suicides = pd.merge(df_suicides2020_2021, df_suicides2022, on='country', how='left')

    combined_df = combined_df.merge(df_suicides, left_on="country_name", right_on="country", how="left")
    conditions = [
        combined_df["Year"] == 2020,
        combined_df["Year"] == 2021,
        combined_df["Year"] == 2022
    ]
    choices = [
        combined_df["SuicideRate_BothSexes_RatePer100k_2020"],
        combined_df["SuicideRate_BothSexes_RatePer100k_2021"],
        combined_df["SuicideRate_BothSexes_RatePer100k_2022"]
    ]
    combined_df["SuicideRate_BothSexes_RatePer100k"] = np.select(conditions, choices, default=np.nan)

    df_cost_of_living_2020 = pd.read_csv('data/cost_of_living_2020.csv')
    df_cost_of_living_2021 = pd.read_csv('data/cost_of_living_2021.csv')
    df_cost_of_living_2022 = pd.read_csv('data/cost_of_living_2022.csv')

    # Dictionary “frames” linking the year to the corresponding Life Cost DataFrame
    frames = {
        2020: df_cost_of_living_2020,
        2021: df_cost_of_living_2021,
        2022: df_cost_of_living_2022
    }

    def get_cost_values(row):
        year = row['Year']
        country = row['country_name']

        if year in frames:
            df_cost = frames[year]
            matching = df_cost.loc[df_cost['Country'] == country]

            if not matching.empty:
                return pd.Series({
                    'Cost of Living Index': matching['Cost of Living Index'].iloc[0],
                    'Rent Index': matching['Rent Index'].iloc[0],
                    'Cost of Living Plus Rent Index': matching['Cost of Living Plus Rent Index'].iloc[0],
                    'Groceries Index': matching['Groceries Index'].iloc[0],
                    'Restaurant Price Index': matching['Restaurant Price Index'].iloc[0],
                    'Local Purchasing Power Index': matching['Local Purchasing Power Index'].iloc[0],
                })

        # If there is no match, return NaN in all columns
        return pd.Series({
            'Cost of Living Index': float('nan'),
            'Rent Index': float('nan'),
            'Cost of Living Plus Rent Index': float('nan'),
            'Groceries Index': float('nan'),
            'Restaurant Price Index': float('nan'),
            'Local Purchasing Power Index': float('nan'),
        })

    # Assign columns in the combined_df
    combined_df[['Cost of Living Index', 'Rent Index', 'Cost of Living Plus Rent Index', 'Groceries Index', 'Restaurant Price Index', 'Local Purchasing Power Index']] = combined_df.apply(get_cost_values, axis=1)

    df_salary_data = pd.read_csv('data/salary_data.csv')
    df_salary_data = df_salary_data.drop(columns=['continent_name', 'wage_span'])
    combined_df = combined_df.merge(df_salary_data, on=["country_name"], how="left", suffixes=('', '_from_right'))

    # List of columns in which we want to replace NaN with a zero
    columns_with_na = [
        'GDP',
        'Population',
        'Rank',
        'Capital',
        'Continent',
        'Area (km²)',
        'Density (per km²)',
        'Growth Rate',
        'World Population Percentage',
        'SuicideRate_BothSexes_RatePer100k',
        'Unemployment, total (% of total labor force)',
        'Cost of Living Index',
        'Rent Index',
        'Cost of Living Plus Rent Index',
        'Groceries Index',
        'Restaurant Price Index',
        'Local Purchasing Power Index',
        'median_salary',
        'average_salary',
        'lowest_salary',
        'highest_salary',
        'population_urban',
        'population_age_80_and_older',
        'population_clustered',
        'stringency_index',
        'emergency_investment_in_healthcare',
        'investment_in_vaccines',
        'fiscal_measures'
    ]

    # Filling NaN values with zeros in the listed columns
    combined_df[columns_with_na] = combined_df[columns_with_na].fillna(0)

    combined_df = combined_df.drop(columns=['iso_3166_1_alpha_3_from_right', 'SuicideRate_BothSexes_RatePer100k_2020',
                                            'SuicideRate_BothSexes_RatePer100k_2021',
                                            'SuicideRate_BothSexes_RatePer100k_2022',
                                            'country', 'Country Code', 'Year', 'SuicideRate_BothSexes_RatePer100k_2020',
                                            'SuicideRate_BothSexes_RatePer100k_2021',
                                            'SuicideRate_BothSexes_RatePer100k_2022'])

    # Display negative, empty and equal to zero values for each column
    print("\n\n")
    for column in combined_df.select_dtypes(include=[np.number]).columns:
        negative_count = (combined_df[column] < 0).sum()

        if negative_count > 0:
            print(f"Column '{column}' has {negative_count} negative values.")

        #rows_with_negatives_column = combined_df[combined_df[column] < 0]
        #print(rows_with_negatives_column)

    print("\n\n")
    for column in combined_df.select_dtypes(include=[np.number]).columns:
        zero_count = (combined_df[column] == 0).sum()

        if zero_count > 0:
            print(f"Column '{column}' has {zero_count} values equal to 0.")
    print("\n\n")
    for column in combined_df.columns:
        empty_count = combined_df[column].isna().sum()

        if empty_count > 0:
            print(f"Column '{column}' has {empty_count} empty fields (NaN).")
    print("\n\n")

    # Find duplicates
    duplicates = combined_df[combined_df.duplicated(subset=['location_key', 'date'], keep=False)]

    print("Duplicates:")
    print(duplicates)

    counts = combined_df['iso_3166_1_alpha_3'].value_counts()
    counts = counts[counts != 991]
    print(f"\nNumber of countries with amount records not equal to number of dataset duraion:")
    print(counts)

    # Group by country and sort by date
    combined_df = combined_df.sort_values(by=['location_key', 'date'])

    # Saving the combined dataframes to a CSV file
    # Filter and save each level directly to a file
    # for level in [0, 1, 2, 3]:
    #    combined_df.query(f"aggregation_level == {level}").to_csv(f'exported/combined_level_{level}.csv', index=False)

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


