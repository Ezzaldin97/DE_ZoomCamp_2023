#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
import warnings
import pyspark.sql.functions as F
warnings.filterwarnings("ignore")


spark = SparkSession.builder.appName("test").getOrCreate()

df = spark.read.option("header", "true").parquet("./data/stage/green_tripdata_2020-01_partitioned")

df = df.withColumn("lpep_pickup_date", F.to_date(df["lpep_pickup_datetime"])).withColumn("lpep_dropoff_date", F.to_date(df["lpep_dropoff_datetime"]))

location_lookup = spark.read.option("header", "true").csv("./data/taxi+_zone_lookup.csv")

merged_df = df.alias("base").join(location_lookup.alias("a"),
                                  F.col("base.PULocationID") == F.col("a.LocationID"),
                                  how = "left")\
                            .join(location_lookup.alias("b"),
                                  F.col("base.DOLocationID") == F.col("b.LocationID"),
                                  how = "left")\
                            .select(F.col("a.Zone").alias("PUZone"), F.col("b.Zone").alias("DOZone"), "trip_distance")
merged_df.repartition(4).write.parquet("./data/trip_loc_dist/green_202001")



