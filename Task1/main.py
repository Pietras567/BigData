import os
import pandas as pd
from google.cloud import bigquery

# Color constants
GREEN = '\033[92m'
BLUE = '\033[94m'
BASIC = '\033[0m'


def clean_countries_data(dataframe):
    # Drop "Netherlands Antilles"
    dataframe = dataframe[dataframe['country_name'] != 'Netherlands Antilles']
    print(f"{BLUE}Dropped rows with Netherlands Antilles"
          f"\nCleaned countries data{BASIC}")

    return dataframe

def process_group(group):
    pass

def fix_negative_values(dataframe):
    pass

def clean_incidence_data(dataframe):
    dataframe['date'] = pd.to_datetime(dataframe['date'])

    # Fixing negative values
    for column in ['new_confirmed', 'cumulative_confirmed', 'new_tested', 'cumulative_tested']:
        dataframe[column] = pd.to_numeric(dataframe[column], errors='coerce')
        dataframe.loc[dataframe[column] < 0, column] = dataframe.loc[dataframe[column] < 0, column] * -1

    def process_group(group):
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
        """
        # to do - algorytm powyżej

        return sorted_group

    result_df = dataframe.groupby('location_key').apply(process_group, include_groups=False)
    result_df = result_df.reset_index(drop=True)

    print(f"{BLUE}Cleaned COVID-19 incidents data{BASIC}")

    return result_df


def main():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "keys/ferrous-destiny-424600-h9-2ab5d0de9937.json" # path to API key
    client = bigquery.Client()

    # Create catalog if not exists
    folder = os.path.dirname("exported/")
    if folder:
        os.makedirs(folder, exist_ok=True)

    # 4.1 data on all countries of the world, comprehensible to humans and universal and potentially future-proof for further processing.
    # iso_3166_1_alpha_3, country_name
    print(f"\n\n{GREEN}Started extracting and cleaning countries data{BASIC}")

    query = ('select location_key, date, iso_3166_1_alpha_3, wikidata_id, aggregation_level, country_name from bigquery-public-data.covid19_open_data.covid19_open_data')
    query_job = client.query(query)
    query_result = query_job.result()
    df1 = query_result.to_dataframe()

    print(f"Number of records with empty fields: {df1.isnull().sum().sum()}")
    print(f"Number of records with empty fields in iso_3166_1_alpha_3: {df1['iso_3166_1_alpha_3'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in country_name: {df1['country_name'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in location_key: {df1['location_key'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in wikidata_id: {df1['wikidata_id'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in aggregation_level: {df1['aggregation_level'].isnull().sum().sum()}")

    #df1 = clean_countries_data(df1)

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

    query = ('select location_key, date, new_confirmed, cumulative_confirmed, new_tested, cumulative_tested from bigquery-public-data.covid19_open_data.covid19_open_data')
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

    query = ('select location_key, date, new_deceased, cumulative_deceased from bigquery-public-data.covid19_open_data.covid19_open_data')
    query_job = client.query(query)
    query_result = query_job.result()
    df3 = query_result.to_dataframe()

    print(f"Number of records with empty fields: {df3.isnull().sum().sum()}")
    print(f"Number of records with empty fields in new_deceased: {df3['new_deceased'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in cumulative_deceased: {df3['cumulative_deceased'].isnull().sum().sum()}")

    df3.to_csv('exported/mortality.csv', index=False)

    print(f"{GREEN}Ended extracting and cleaning human mortality data{BASIC}")

    # 4.4 COVID-19 vaccination data
    # new_persons_vaccinated, new_persons_fully_vaccinated, cumulative_persons_vaccinated, new_vaccine_doses_administered, cumulative_vaccine_doses_administered
    print(f"\n\n{GREEN}Started extracting and cleaning vaccination data{BASIC}")

    query = ('select location_key, date, new_persons_vaccinated, new_persons_fully_vaccinated, cumulative_persons_vaccinated, new_vaccine_doses_administered, cumulative_vaccine_doses_administered from bigquery-public-data.covid19_open_data.covid19_open_data')
    query_job = client.query(query)
    query_result = query_job.result()
    df4 = query_result.to_dataframe()

    print(f"Number of records with empty fields: {df4.isnull().sum().sum()}")
    print(f"Number of records with empty fields in new_persons_vaccinated: {df4['new_persons_vaccinated'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in new_persons_fully_vaccinated: {df4['new_persons_fully_vaccinated'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in cumulative_persons_vaccinated: {df4['cumulative_persons_vaccinated'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in new_vaccine_doses_administered: {df4['new_vaccine_doses_administered'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in cumulative_vaccine_doses_administered: {df4['cumulative_vaccine_doses_administered'].isnull().sum().sum()}")

    df4.to_csv('exported/vaccination.csv', index=False)

    print(f"{GREEN}Ended extracting and cleaning vaccination data{BASIC}")

    # 4.5 the state of health of the population
    # smoking_prevalence, diabetes_prevalence, infant_mortality_rate, nurses_per_1000, physicians_per_1000, health_expenditure_usd
    print(f"\n\n{GREEN}Started extracting and cleaning the state of health of the population data{BASIC}")

    query = ('select location_key, date, smoking_prevalence, diabetes_prevalence, infant_mortality_rate, nurses_per_1000, physicians_per_1000, health_expenditure_usd from bigquery-public-data.covid19_open_data.covid19_open_data')
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


if __name__ == '__main__':
    main()


