
from pyspark.sql import SparkSession
import warnings
import pyspark.sql.functions as F
warnings.filterwarnings("ignore")


spark = SparkSession.builder.appName("test").getOrCreate()

spark.conf.set('temporaryGcsBucket', "dataproc-temp-europe-west6-61645669347-jb86zzrg")

df = spark.read.option("header", "true").parquet("gs://dtc_data_lake_nyc-taxi-pipeline/staging_area/green_tripdata_*.parquet")

df = df.withColumn("lpep_pickup_date", F.to_date(df["lpep_pickup_datetime"])).withColumn("lpep_dropoff_date", F.to_date(df["lpep_dropoff_datetime"]))

location_lookup = spark.read.option("header", "true").csv("gs://dtc_data_lake_nyc-taxi-pipeline/staging_area/taxi+_zone_lookup.csv")

merged_df = df.alias("base").join(location_lookup.alias("a"),
                                  F.col("base.PULocationID") == F.col("a.LocationID"),
                                  how = "left")\
                            .join(location_lookup.alias("b"),
                                  F.col("base.DOLocationID") == F.col("b.LocationID"),
                                  how = "left")\
                            .select(F.col("base.lpep_pickup_date").alias("pickup_date"), F.col("base.lpep_dropoff_date").alias("dropoff_date"),
                                    F.col("a.Zone").alias("PUZone"), F.col("b.Zone").alias("DOZone"), "trip_distance")
merged_df.write.format("bigquery").option("table", "nyc_trips_data.trips_green_20_19").save()