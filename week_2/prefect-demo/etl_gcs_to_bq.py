from pathlib import Path
import pandas as pd
import os
from sqlalchemy import create_engine
from time import time
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

gcp_credentials_block = GcpCredentials.load("gcs-creds")

@task()
def extract_from_gcs(color, year, month) -> Path:
    """Download Trip Data from GCS"""

    gcs_path=f"data/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("prefect-gcs")
    gcs_block.get_directory(
        from_path=gcs_path,
        local_path=f"../data/"
    )

    return Path(f"../data/{gcs_path}")

@task()
def read_and_transform(path: Path) -> pd.DataFrame:
    """Read the data into DataFrame"""

    df = pd.read_parquet(path)
    df['passenger_count'].fillna(0, inplace=True)

    return df

@task()
def load_to_bq(df: pd.DataFrame) -> None:
    """Load data to Big Query"""

    df.to_gbq(
        destination_table="de_zoom2.all_rides",
        project_id="de-zoomcamp-375823",
        if_exists="replace",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=1000
    )


@flow(log_prints=True)
def etl_gcs_to_bq(color: str, month: int, year: int) -> None:
    """The main ETL function"""

    path = extract_from_gcs(color, year, month)
    print(path)
    df = read_and_transform(path)
    df['color'] = color
    return df


@flow(name="parent_flow_to_gbq")
def parent_etl_flow(colors, months, years):

    overall_df: pd.DataFrame = pd.DataFrame([])

    for color in colors:
        for mth in months:
            for year in years:
                df = etl_gcs_to_bq(color, mth, year)
                if overall_df.empty:
                    overall_df = df
                else:
                    overall_df = pd.concat([overall_df, df])

    load_to_bq(overall_df)




    

if __name__ == '__main__':
    parent_etl_flow(["yellow", "green"], [1,2,3], [2020, 2021])