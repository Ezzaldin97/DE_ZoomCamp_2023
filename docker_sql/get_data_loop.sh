#!/bin/bash

# yellow link example: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-01.parquet
# green links example: https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-01.parquet
# lookup: https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv

URL="https://d37ci6vzurychx.cloudfront.net/trip-data"
TAXI_LIST=("yellow" "green")

wget "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv" -O "taxi_zone_lookup.csv"
python ingest_data.py \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name="taxi_zone_lookup" \
    --file_name="taxi_zone_lookup.csv"

wget "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz" -O "green_tripdata_2019-01.csv.gz"
gzip -d "green_tripdata_2019-01.csv.gz"
python ingest_data.py \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name="green_tripdata_2019-01" \
    --file_name="green_tripdata_2019-01.csv"
# we can loop through yellow and green taxi trips to load all of them to postgres..
'''
for taxi in ${TAXI_LIST[@]}
do
  echo "process of getting and ingesting ${taxi} taxi data to postgres started"
  for num in {1..12}
  do
    fidx=`printf "%02d" ${num}`
    file_url="${URL}/${taxi}_tripdata_2019-${fidx}.parquet"
    file_name="${taxi}_tripdata_2019-${fidx}.parquet"
    table_name="${taxi}_tripdata_2019-${fidx}"
    echo "DOWNLOADING ${file_url}"
    wget ${file_url} -O ${file_name}
    python ingest_data.py \
      --user=root \
      --password=root \
      --host=pgdatabase \
      --port=5432 \
      --db=ny_taxi \
      --table_name=${table_name} \
      --file_name=${file_name}
  done
done
'''
echo "all parquet files inserted successfully to postgres"