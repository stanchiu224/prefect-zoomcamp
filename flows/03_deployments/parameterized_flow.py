from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta
import argparse


@task(
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
    log_prints=True,
)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    print(f"Attempting to read {dataset_url} to dataframe...")
    df = pd.read_csv(dataset_url)
    print(f"Successfully read {dataset_url} to dataframe.")
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    print(f"Cleaning dataframe.")
    """Fix dtype issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    Path(f"data/{color}/{dataset_file}").mkdir(parents=True, exist_ok=True)
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoomcamp-gcs-bucket")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow(log_prints=True)
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    print(f"Downloading from {dataset_url}")

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


@flow(log_prints=True)
def etl_parent_flow(
    months: list[int] = [1, 2], years: list[int] = [2020, 2021], color: str = "yellow"
):
    # the value parameters above are just default values and are used if parameters are not given in __main__
    for year in years:
        for month in months:
            etl_web_to_gcs(year, month, color)


if __name__ == "__main__":
    # Use parser to get parameters from command line
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", nargs="+", type=int, default=[1, 2, 3])
    parser.add_argument("--years", nargs="+", type=int, default=[2019, 2020])
    parser.add_argument("--color", type=str, default="yellow")
    args = parser.parse_args()

    etl_parent_flow(months=args.months, years=args.years, color=args.color)
