from pathlib import Path
import pandas as pd
import os
from sqlalchemy import create_engine
from time import time
from prefect import flow, task, Flow
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket


@task()
def fetch(dataset_url) -> pd.DataFrame:
    """Read Data from web to DataFrame"""
    # os.system(f"wget {dataset_url} -O output.csv.gz")

    df = pd.read_csv("output.csv.gz")
    return df


@task(log_prints=True)
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    print(df.head(2))
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))

    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    path = Path(f"{dataset_file}.parquet")
    df.head(20000).to_parquet(path, compression='gzip')

    return path

@task()
def write_to_gcs(path: Path) -> None:
    gcp_cloud_storage_bucket_block = GcsBucket.load("prefect-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=path,
        to_path=f"data/{path}"
    )




def etl_web_to_gcs(color: str, month: int, year: int) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    print(dataset_url)
    df = fetch(dataset_url)
    clean_df = clean_data(df)
    path = write_local(clean_df, color, dataset_file)

    write_to_gcs(path)

fl= Flow(etl_web_to_gcs, name="single_etl_web_gcs_{color}")

@flow(name="parent_read_to_gcs")
def parent_etl_flow(colors, months, years):

    for color in colors:
        for mth in months:
            for year in years:

                
                fl._run(color, mth, year)



if __name__ == '__main__':
    parent_etl_flow(["yellow", "green"], [1,2,3], [2020, 2021])