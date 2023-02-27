from pathlib import Path
import pandas as pd
import sys
import argparse
from datetime import timedelta
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from prefect_gcp import GcpCredentials
from google.cloud import storage
from google.cloud import bigquery


@flow(log_prints=True)
def list_gcs_blobs(color: str, years: list[int], months: list[int]) -> tuple:
    # create a list of paths in GCS that exist and a list of paths that do not exist
    gcs_block = GcsBucket.load("zoomcamp-gcs-bucket")
    ls_gcs_blobs = []
    no_blobs = []
    for year in years:
        for month in months:
            gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
            if check_gcs_blob(color, year, month):
                ls_gcs_blobs.append(gcs_path)
            else:
                no_blobs.append(gcs_path)
    return ls_gcs_blobs, no_blobs


@task(log_prints=True, retries=3)
# check if the file exists in GCS. If it does, return True. If it doesn't, return False
def check_gcs_blob(color: str, year: int, month: int) -> bool:
    storage_client = storage.Client()
    bucket = storage_client.bucket("dtc_data_lake_focused-brace-374920")
    blob = bucket.blob(
        "data/{color}/{color}_tripdata_{year}-{month:02}.parquet".format(
            color=color, year=year, month=month
        )
    )
    return blob.exists()


@task(
    log_prints=True,
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def extract_from_gcs(gcs_path: Path) -> Path:
    """Download trip data from GCS"""
    gcs_block = GcsBucket.load("zoomcamp-gcs-bucket")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def read(path: Path) -> pd.DataFrame:
    """read the data into pandas"""
    df = pd.read_parquet(path)
    return df


@task()
def write_bq(df: pd.DataFrame) -> int:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoomcamp-gcs-creds")

    # Define the destination table
    table_ref = bigquery.DatasetReference.from_string("focused-brace-374920.dezoomcamp")
    table = bigquery.Table(table_ref)

    # Query the destination table for records that match the primary key of the incoming data
    query = f"""
        SELECT COUNT(*) as count
        FROM `focused-brace-374920.dezoomcamp.rides`
        WHERE tpep_pickup_datetime = "{df['tpep_pickup_datetime'][0]}" AND
              tpep_dropoff_datetime = "{df['tpep_dropoff_datetime'][0]}" AND
              passenger_count = {df['passenger_count'][0]}
    """

    # Initialize the BigQuery client and store the result of the query in a DataFrame
    client = bigquery.Client(
        credentials=gcp_credentials_block.get_credentials_from_service_account()
    )
    query_job = client.query(query)
    results = query_job.result().to_dataframe()
    # print(results.iloc[0]["count"])
    count = results.iloc[0]["count"]

    # If the primary key is not found, append the data to the destination table
    if count == 0:
        df.to_gbq(
            destination_table="dezoomcamp.rides",
            project_id="focused-brace-374920",
            credentials=gcp_credentials_block.get_credentials_from_service_account(),
            chunksize=500_000,
            if_exists="append",
        )
        return len(df)
    else:
        print("Data already exists in BigQuery. Skipping...")
        return 0


@flow(log_prints=True)
def el_gcs_to_bq(gcs_path) -> int:
    """Main ETL flow to load data into Big Query"""

    path = extract_from_gcs(gcs_path)
    df = read(path)
    row_count = write_bq(df)
    return row_count


@flow(log_prints=True)
def el_parent_gcs_to_bq(
    months: list[int] = [1, 2], years: list[int] = [2019, 2020], color: str = "yellow"
):
    """Main EL flow to load data into Big Query"""
    total_rows = 0
    ls_gcs_blobs, no_blobs = list_gcs_blobs(color, years, months)

    if ls_gcs_blobs == []:
        print("None of the requested files were found in GCS. Exiting...")
        sys.exit()
    else:
        print("Files that exists in GCS and will be added to BigQuery: ", ls_gcs_blobs)

    if no_blobs == []:
        print("All requested files were found in GCS and will be added to BigQuery")
    else:
        print("Files that do not exist in GCS: ", no_blobs)

    for gcs_path in ls_gcs_blobs:
        rows = el_gcs_to_bq(gcs_path)
        total_rows += rows
    print(total_rows)


if __name__ == "__main__":
    # Use parser to get parameters from command line
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", nargs="+", type=int, default=[1, 2, 3])
    parser.add_argument("--years", nargs="+", type=int, default=[2019, 2020])
    parser.add_argument("--color", type=str, default="yellow")
    args = parser.parse_args()

    el_parent_gcs_to_bq(months=args.months, years=args.years, color=args.color)
