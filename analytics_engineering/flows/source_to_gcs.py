from prefect import flow, task
import pandas as pd
from prefect_gcp.cloud_storage import GcsBucket
from pathlib import Path
from prefect.tasks import task_input_hash
from datetime import timedelta
import os

@task(name = "FetchData", description = "Export/Get Data from Source to Loacal Environment and save it as parquet",
      retries = 2, tags = ["Export", "Fetch", "RawData", "save"], cache_key_fn = task_input_hash,
      cache_expiration = timedelta(minutes = 15), log_prints=True)
def fetch(url:str, dataset_file:str) -> Path:
    """
    Read Data File from Web as Pandas DataFrame...
    """
    df = pd.read_parquet(url)
    print(f"Number of Records: {len(df)}")
    path = Path(f"../temp_data/{dataset_file}.parquet")
    df.to_parquet(path, compression = "gzip")
    print(f"file {path} saved to Local Environment..")
    return path

@task(log_prints=True, name = "LoadToGCS",
      description="Load File from Loacl Environment to GCS Bucket..",
      tags = ["Load", "GCS"])
def load_to_gcs(path:Path, dataset_file:str) -> None:
    """
    Load Data to GCS Bucket (DataLake)...
    """
    print(f"connecting to GCP-GCS Bucket..")
    gcs_bucket_block = GcsBucket.load("de-zoomcamp-2023")
    gcs_bucket_block.upload_from_path(from_path = path,  timeout=15000)
    print(f"file uploaded successfully to GCS..")
    gcs_path = f"gs://dtc_data_lake_nyc-taxi-pipeline/fhv_data/{dataset_file}.parquet"
    print(f"Data Now in {gcs_path}")

def del_file_from_local_env(path:Path) -> None:
    os.remove(path)
    print(f"file removed successfully from data directory..")
    
@flow(name = "ELToGCS",
      description = "Workflow of Extract/Load Data to GCS..",
      log_prints = True)
def el_gcs(car_type:str, year:int, month:int) -> None:
    """
    Main EL workflow to Load Data From Web Source to GCS Bucket...
    """
    dataset_file = f"{car_type}_tripdata_{year}-{month:02}"
    dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet"
    path = fetch(dataset_url, dataset_file)
    load_to_gcs(path, dataset_file)
    del_file_from_local_env(path)