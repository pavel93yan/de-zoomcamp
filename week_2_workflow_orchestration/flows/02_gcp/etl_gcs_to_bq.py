from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def write_bq(path: Path) -> int:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("prefect-etl-acc")
    df = pd.read_parquet(path)
    df.to_gbq(
        destination_table="trips_data_all.rides",
        project_id="de-project-375820",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )
    return len(df)


@flow()
def etl_gcs_to_bq(year: int, month: int, color: str) -> int:
    """Main ETL flow to load data into Big Query"""

    path = extract_from_gcs(color, year, month)
    rows = write_bq(path)
    return rows


@flow()
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    rows = 0
    for month in months:
       rows += etl_gcs_to_bq(year, month, color)

    print(f"rows: {rows}")


if __name__ == "__main__":
    color = "yellow"
    months = [2, 3]
    year = 2021
    etl_parent_flow(months, year, color)
