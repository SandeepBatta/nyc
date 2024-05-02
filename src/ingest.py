import os
import os.path as osp
import pandas as pd
from google.cloud import storage
from sodapy import Socrata
from bq_utils import load_csv_bq, schema_dict
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BQ_TABLE = os.environ.get("GCP_BIGQUERY_TABLE")

LIMIT = 500_000

client = Socrata("data.cityofnewyork.us", None)


def get_data_quarter(date_start, date_end):
    df = fetch_results_df(date_start, date_end)
    file_name = "2023|1.csv"
    save_path = osp.join(file_name)
    print("Saving data to CSV file...")
    df.to_csv(save_path, index=False)
    print("CSV file created")


def fetch_results_df(date_start, date_end):
    i = 0
    results = []
    print("Fetching data though API...")
    while True:
        result = client.get(
            "qgea-i56i",
            limit=LIMIT,
            offset=i,
            where=f"rpt_dt between '{date_start}' and '{date_end}'",
        )
        i += LIMIT
        if len(result) == 0:
            break
        results.append(result)

    print("Data downloaded")

    df = [pd.DataFrame.from_records(r) for r in results]
    df = pd.concat(df, ignore_index=True)

    df = df[list(schema_dict.keys())]
    df["cmplnt_to_tm"].replace("(null)", None, inplace=True)
    return df


def upload_to_gcs(bucket):
    file_name = "2023|1.csv"
    local_file = osp.join(file_name)

    print(f"Uploading {local_file} to GCS...")

    # Upload to GCS
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    client = storage.Client()
    bucket = client.bucket(bucket)

    object_name = f"crime_data/{file_name}"
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

    print("Upload to GCS Done")


def main():
    date_start = "2023-01-01"
    date_end = "2023-03-31"
    get_data_quarter(date_start, date_end)
    upload_to_gcs(BUCKET)
    load_csv_bq(BQ_TABLE)


if __name__ == "__main__":
    main()
