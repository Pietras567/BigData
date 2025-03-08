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

if __name__ == '__main__':
    main()


