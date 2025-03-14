import os
import pandas as pd
from google.cloud import bigquery
import pycountry

def update_country_codes(dataframe):
    # Change "Netherlands Antilles" to "Netherlands"
    dataframe.loc[dataframe['country_name'] == 'Netherlands Antilles', 'country_name'] = 'Netherlands'

    def get_alpha_3(row):
        if pd.isna(row['iso_3166_1_alpha_3']) or row['iso_3166_1_alpha_3'] == 'nan':
            try:
                country = pycountry.countries.get(name=row['country_name'])
                if country:
                    return country.alpha_3

                print(f"Nie znaleziono kodu ISO dla: {row['country_name']}")
            except Exception as e:
                print(f"Błąd dla kraju {row['country_name']}: {e}")

        return row['iso_3166_1_alpha_3']

    # Update ISO codes
    dataframe['iso_3166_1_alpha_3'] = dataframe.apply(get_alpha_3, axis=1)
    return dataframe

def main():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "keys/ferrous-destiny-424600-h9-2ab5d0de9937.json" # path to API key
    client = bigquery.Client()

    # Create catalog if not exists
    folder = os.path.dirname("exported/")
    if folder:
        os.makedirs(folder, exist_ok=True)

    #query = ('select * from bigquery-public-data.covid19_open_data.covid19_open_data limit 10')
    #query_job = client.query(query)
    #query_result = query_job.result()
    #df = query_result.to_dataframe()

    #print(df)
    #for col, dtype in df.dtypes.items():
    #    print(f"Nazwa kolumny: {col}, Typ: {dtype}")

    # 4.1 data on all countries of the world, comprehensible to humans and universal and potentially future-proof for further processing.
    # iso_3166_1_alpha_3, country_name
    print("\n\nStarted extracting and cleaning countries data")

    query = ('select iso_3166_1_alpha_3, country_name from bigquery-public-data.covid19_open_data.covid19_open_data limit 100')
    query_job = client.query(query)
    query_result = query_job.result()
    df1 = query_result.to_dataframe()

    print(df1)
    print(f"Number of records with empty fields: {df1.isnull().sum().sum()}")
    print(f"Number of records with empty fields in iso_3166_1_alpha_3: {df1['iso_3166_1_alpha_3'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in country_name: {df1['country_name'].isnull().sum().sum()}")

    df1 = update_country_codes(df1)

    print(f"Number of records with empty fields: {df1.isnull().sum().sum()}")
    print(f"Number of records with empty fields in iso_3166_1_alpha_3: {df1['iso_3166_1_alpha_3'].isnull().sum().sum()}")
    print(f"Number of records with empty fields in country_name: {df1['country_name'].isnull().sum().sum()}")

    df1.to_csv('exported/countries.csv', index=False)
    print("Ended extracting and cleaning countries data")

    # 4.2 COVID-19 incidence data worldwide
    # date, new_confirmed, cumulative_confirmed, new_tested, cumulative_tested
    print("\n\nStarted extracting and cleaning COVID-19 incidents data")

    query = ('select date, new_confirmed, cumulative_confirmed, new_tested, cumulative_tested from bigquery-public-data.covid19_open_data.covid19_open_data limit 100')
    query_job = client.query(query)
    query_result = query_job.result()
    df2 = query_result.to_dataframe()

    print(f"Number of records with empty fields: {df2.isnull().sum().sum()}")

    df2.to_csv('exported/incidence.csv', index=False)

    print("Ended extracting and cleaning COVID-19 incidents data")

    # 4.3 data on the problem of human mortality caused by the virus
    # new_deceased, cumulative_deceased,
    print("\n\nStarted extracting and cleaning human mortality data")

    query = ('select new_deceased, cumulative_deceased from bigquery-public-data.covid19_open_data.covid19_open_data limit 100')
    query_job = client.query(query)
    query_result = query_job.result()
    df3 = query_result.to_dataframe()

    print(f"Number of records with empty fields: {df3.isnull().sum().sum()}")

    df3.to_csv('exported/mortality.csv', index=False)

    print("Ended extracting and cleaning human mortality data")

    # 4.4 COVID-19 vaccination data
    # new_persons_vaccinated, new_persons_fully_vaccinated, cumulative_persons_vaccinated, new_vaccine_doses_administered, cumulative_vaccine_doses_administered
    print("\n\nStarted extracting and cleaning vaccination data")

    query = ('select new_persons_vaccinated, new_persons_fully_vaccinated, cumulative_persons_vaccinated, new_vaccine_doses_administered, cumulative_vaccine_doses_administered from bigquery-public-data.covid19_open_data.covid19_open_data limit 100')
    query_job = client.query(query)
    query_result = query_job.result()
    df4 = query_result.to_dataframe()

    print(f"Number of records with empty fields: {df4.isnull().sum().sum()}")

    df4.to_csv('exported/vaccination.csv', index=False)

    print("Ended extracting and cleaning vaccination data")

    # 4.5 the state of health of the population
    # smoking_prevalence, diabetes_prevalence, infant_mortality_rate, nurses_per_1000, physicians_per_1000, health_expenditure_usd
    print("\n\nStarted extracting and cleaning the state of health of the population data")

    query = ('select smoking_prevalence, diabetes_prevalence, infant_mortality_rate, nurses_per_1000, physicians_per_1000, health_expenditure_usd from bigquery-public-data.covid19_open_data.covid19_open_data limit 100')
    query_job = client.query(query)
    query_result = query_job.result()
    df5 = query_result.to_dataframe()

    print(f"Number of records with empty fields: {df5.isnull().sum().sum()}")

    df5.to_csv('exported/health.csv', index=False)

    print("Ended extracting and cleaning the state of health of the population data")


if __name__ == '__main__':
    main()


