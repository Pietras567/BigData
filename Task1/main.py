import os
import pandas as pd
from google.cloud import bigquery


def main():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "keys/ferrous-destiny-424600-h9-2ab5d0de9937.json" # path to API key
    client = bigquery.Client()

    query = ('select * from bigquery-public-data.covid19_open_data.covid19_open_data limit 10')
    query_job = client.query(query)
    query_result = query_job.result()
    df = query_result.to_dataframe()

    print(df)
    for col, dtype in df.dtypes.items():
        print(f"Nazwa kolumny: {col}, Typ: {dtype}")

    # 4.1 data on all countries of the world, comprehensible to humans and universal and potentially future-proof for further processing.
    # location_key, iso_3166_1_alpha_3, country_name
    print("Started extracting and cleaning countries data")

    print("Ended extracting and cleaning countries data")

    # 4.2 COVID-19 incidence data worldwide
    # date, new_confirmed, cumulative_confirmed, new_tested, cumulative_tested
    print("Started extracting and cleaning COVID-19 incidents data")

    print("Ended extracting and cleaning COVID-19 incidents data")

    # 4.3 data on the problem of human mortality caused by the virus
    # new_deceased, cumulative_deceased,
    print("Started extracting and cleaning human mortality data")
    
    print("Ended extracting and cleaning human mortality data")

    # 4.4 COVID-19 vaccination data
    # new_persons_vaccinated, new_persons_fully_vaccinated, cumulative_persons_vaccinated, new_vaccine_doses_administered, cumulative_vaccine_doses_administered
    print("Ended extracting and cleaning vaccination data")

    print("Started extracting and cleaning vaccination data")

    # 4.5 to do
    #



if __name__ == '__main__':
    main()


