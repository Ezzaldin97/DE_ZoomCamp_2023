-- external table creation
CREATE OR REPLACE EXTERNAL TABLE `nyc_trips_data.fhv_tripdata_external_table`
OPTIONS (
   format = "PARQUET",
   uris = ["gs://dtc_data_lake_nyc-taxi-pipeline/fhv_*.parquet"]
);

-- q1
SELECT COUNT(*) FROM `nyc_trips_data.fhv_tripdata_external_table`;
-- answer: 43244696

-- q2
SELECT COUNT(DISTINCT(dispatching_base_num)) FROM `nyc_trips_data.fhv_tripdata_external_table`; 

SELECT COUNT(*) FROM `nyc_trips_data.fhv_tripdata_external_table`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;

-- q4
CREATE OR REPLACE TABLE `taxi-rides-ny.nytaxi.fhv_nonpartitioned_tripdata`
AS SELECT * FROM `nyc_trips_data.fhv_tripdata_external_table`;

CREATE OR REPLACE TABLE `taxi-rides-ny.nytaxi.fhv_partitioned_tripdata`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS (
  SELECT * FROM `taxi-rides-ny.nytaxi.fhv_tripdata`
);

-- Q5
SELECT count(*) FROM  `taxi-rides-ny.nytaxi.fhv_nonpartitioned_tripdata`
WHERE dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');

-- Q6
SELECT count(*) FROM `taxi-rides-ny.nytaxi.fhv_partitioned_tripdata`
WHERE dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');