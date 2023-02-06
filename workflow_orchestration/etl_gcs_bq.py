import os
from prefect import flow, task
import pandas as pd
from prefect_gcp.cloud_storage import GcsBucket
from pathlib import Path
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_gcp import GcpCredentials


@task(log_prints=True, name = "ExtractPathFromGCS",
      description="Extract File Path From GCS Bucket Environment",
      tags = ["Extract", "Path", "GCS"])
def extract_from_gcs(color:str, year:int, month:int) -> Path:
    """
    Download/Extract Data From GCS to Local Environment...
    """
    gcs_path = f"{color}_tripdata_{year}-{month:02}.parquet"
    print("Connecting to GCS Bucket..")
    gcs_bucket_block = GcsBucket.load("de-zoomcamp-2023")
    abs_path = gcs_bucket_block.download_object_to_path(from_path = gcs_path, to_path = f"./{gcs_path}")
    print(f"Data in GCS {gcs_path} downloaded to {abs_path}")
    return abs_path

@task(log_prints=True, name = "Transform/Clean Data", 
      description = "Clean Data and Transform it before moving to BigQuery..",
      tags = ["Transform", "Clean"])
def transform_data(path:Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task(log_prints = True, name = "Load to BigQuery", 
      description="Load File after Transformation to BigQuery..",
      tags = ["Load", "BigQuery"])
def load_to_bq(df:pd.DataFrame, color:str, year:int, month:int) -> None:
    gcp_cred_block = GcpCredentials.load("zoomcamp-gcp-creds")
    df.to_gbq(
        project_id = "nyc-taxi-pipeline", credentials=gcp_cred_block.get_credentials_from_service_account(),
        destination_table=f"nyc-taxi-pipeline.nyc_trips_data.{color}_tripdata_{year}_{month}", chunksize = 300_000,
        if_exists="append"
    )

@flow(name = "ETL From GCS To BigQuery...",
      description="Workflow of Loading Data From GCS to BigQuery..")
def etl_gcs_to_bq() -> None:
    """
    Main ETL workflow From GCS to BigQuery Function...
    """
    color = "yellow"
    year = 2021
    month = 1
    file_path = extract_from_gcs(color, year, month)
    trans_df = transform_data(file_path)
    load_to_bq(trans_df, color, year, month)
if __name__ == "__main__":
    etl_gcs_to_bq()