import os
from prefect import flow, task
import pandas as pd
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import bigquery_create_table
from pathlib import Path
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_gcp import GcpCredentials
#from prefect_gcp.bigquery import BigQueryWarehouse
from google.cloud import bigquery


@task(name = "FetchData", description = "Export/Get Data from Source",
      retries = 2, tags = ["Export", "Fetch", "RawData"], cache_key_fn = task_input_hash,
      cache_expiration = timedelta(hours = 1), log_prints=True)
def fetch(url:str) -> pd.DataFrame:
    """
    Read Data File from Web as Pandas DataFrame...
    """
    df = pd.read_csv(url)
    print(f"Number of Records: {len(df)}")
    return df

@task(log_prints = True, name = "WriteToLocalEnv",
      description="Save Cleaned File as Parquet file to local Environment..",
      tags = ["LocalEnv", "WriteToLocal"])
def write_to_local(df:pd.DataFrame, color:str, dataset_file:str) -> Path:
    path = Path(f"{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    print(f"file {path} saved to Loacal Environment..")
    return path

@task(log_prints=True, name = "LoadToGCS",
      description="Load File from Loacl Environment to GCS Bucket..",
      tags = ["Load", "GCS"])
def load_to_gcs(path:Path, dataset_file:str) -> str:
    """
    Load Data to GCS Bucket (DataLake)...
    """
    print(f"connecting to GCP-GCS Bucket..")
    gcs_bucket_block = GcsBucket.load("de-zoomcamp-2023")
    gcs_bucket_block.upload_from_path(from_path = path,  timeout=1200)
    print(f"file uploaded successfully to GCS..")
    gcs_path = f"gs://dtc_data_lake_nyc-taxi-pipeline/{dataset_file}.parquet"
    print(f"Data Now in {gcs_path}")
    return gcs_path 

# this will work as subflow..
# in this subflow, table will be created and loaded to BigQuery....
@flow(name = "ELFromGCSTOBQ",
      description = "Workflow of Loading Data From GCS to BigQuery..",
      log_prints = True)
def el_gcs_to_bq(color:str, year:int, month:int) -> None:
    """
    Main ETL workflow From GCS to BigQuery Function...
    last part of subflow, Load Data Directly to BigQuery From GCS..
    """
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    df = fetch(dataset_url)
    file_path = write_to_local(df, color, dataset_file) 
    gcs_data_path = load_to_gcs(file_path, dataset_file)
    gcp_cred_block = GcpCredentials.load("zoomcamp-gcp-creds")
    external_table_options = bigquery.ExternalConfig('PARQUET')
    external_table_options.autodetect = True
    external_table_options.source_uris = [gcs_data_path]
    res = bigquery_create_table(
        dataset='nyc_trips_data',
        table=dataset_file,
        external_config= external_table_options,
        gcp_credentials=gcp_cred_block,
        location = "EU"
    )
    print(res)

# this will work as parent workflow
@flow(name = "ParentELBQ",
      description = "This Function Manages the Workflow of EL Workflow to run for different parameters..",
      log_prints=True)
def el_parent_bq_flow(months:list[int]=[2, 3], year=2019, colors:list[str]=["yellow"]):
    for color in colors:
        for month in months:
            res = el_gcs_to_bq(color, year, month)
            print(res)

if __name__ == "__main__":
    el_parent_bq_flow()