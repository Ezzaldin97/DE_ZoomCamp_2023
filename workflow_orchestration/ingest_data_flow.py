#!/usr/bin/env python
# coding: utf-8
import os
import pandas as pd
import pyarrow.parquet as pr
from sqlalchemy import create_engine
import time
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector
from datetime import timedelta

def chunkify(df: pd.DataFrame, chunk_size: int):
    start = 0
    length = df.shape[0]
    # If DF is smaller than the chunk, return the DF
    if length <= chunk_size:
        yield df[:]
        return
    # Yield individual chunks
    while start + chunk_size <= length:
        yield df[start:chunk_size + start]
        start = start + chunk_size
    # Yield the remainder chunk, if needed
    if start < length:
        yield df[start:]

@task(log_prints = True, retries = 3, cache_key_fn = task_input_hash, cache_expiration = timedelta(days = 1))
def extract(file_name : str):
    os.system(f'curl -kLSs https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name} -o {file_name}')
    if file_name.endswith(".csv"):
        trips = pd.read_csv(file_name)
    else:
        # else it is parquet file.....
        trips = pr.read_table(file_name)
        trips = trips.to_pandas()
    print(f"data extracted from source, data shape: {trips.shape}")
    return trips
    
@task(log_prints = True, retries = 3)
def transform(table_name, trips):
    if table_name.startswith("yellow"):
        trips.tpep_pickup_datetime = pd.to_datetime(trips.tpep_pickup_datetime)
        trips.tpep_dropoff_datetime = pd.to_datetime(trips.tpep_dropoff_datetime)
        print("datetime columns converted")
    elif table_name.startswith("green"):
        trips.lpep_pickup_datetime = pd.to_datetime(trips.lpep_pickup_datetime)
        trips.lpep_dropoff_datetime = pd.to_datetime(trips.lpep_dropoff_datetime)
        print("datetime columns converted")
    trips = trips[trips["passenger_count"] != 0]
    return trips

@task(log_prints = True, retries = 3)
def load(table_name, trips):
    database_block = SqlAlchemyConnector.load("postgres-database")
    with database_block.get_connection(begin = True) as engine:
        trips.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        trips_chunks = chunkify(trips, 100000)
        counter = 0
        while True:
            try:
                start = time.time()
                df = next(trips_chunks)
                df.to_sql(name = table_name, con = engine, if_exists = 'append')
                end = time.time()
                print(f'chunk {counter} inserted in {end - start} seconds..')
                counter+=1
            except StopIteration:
                print('all records Inserted into database successfully...')
                break

@flow(name = "Ingest Data")
def main_flow(table_name:str="green_tripdata_2019-01"):
    raw_data = extract("green_tripdata_2019-01.parquet")
    transformed_data = transform(table_name, raw_data)
    load(table_name, transformed_data)

if __name__ == '__main__':
    main_flow()