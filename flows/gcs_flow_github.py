import os
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> tuple[str, str]:
    """Write DataFrame out locally as parquet file"""
    path = f"data/{color}/{dataset_file}.parquet"
    path_local = os.path.normpath(f"{os.getcwd()}/{path}")
    dir_to_save = os.path.dirname(path_local)
    if not os.path.exists(dir_to_save):
        os.makedirs(dir_to_save)
    df.to_parquet(path_local, compression="gzip")
    return path_local, path


@task()
def write_gcs(path_local: str, path: str) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path_local, to_path=path)
    return


@flow(log_prints=True)
def github_web_to_gcs() -> None:
    """The main ETL function"""
    color = "green"
    year = 2020
    month = 11
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path_sys, path_gcs = write_local(df_clean, color, dataset_file)
    write_gcs(path_sys, path_gcs)


if __name__ == "__main__":
    github_web_to_gcs()
