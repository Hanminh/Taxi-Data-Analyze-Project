import findspark
findspark.init()

import uuid
import pyspark
import os
import sys
from env_variable import *
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, LongType
from pyspark.sql.functions import col, coalesce, lit

# Environment settings
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Spark session
spark = SparkSession.builder \
    .appName('Read Data from HDFS') \
    .config('spark.default.parallelism', 100) \
    .config('spark.cassandra.connection.host', CASSANDRA_HOST) \
    .getOrCreate()
spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")
# Initialize HDFS client
client = InsecureClient(HDFS_URL)

# Get list of files from HDFS directory
directory = client.list(HDFS_PATH)
paths = [f"{HDFS_NAMENODE_URL}{HDFS_PATH}{file}" for file in directory]

# Define schema for the incoming JSON
schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True)
])


# Read data from Cassandra
df_cassandra = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="passenger_number_statistics", keyspace="statistics") \
    .load()

df_cassandra = df_cassandra.withColumn("count", df_cassandra["count"].cast(LongType()))

# Read and combine all Parquet files into a single DataFrame
df = spark.read.format('parquet').load(paths)
df = df.withColumn("passenger_count", df["passenger_count"].cast(IntegerType()))
# df.show()

# Rename columns to lowercase
column_mapping = {
    "VendorID": "vendorid",
    "PULocationID": "pulocationid",
    "DOLocationID": "dolocationid",
    "RatecodeID": "ratecodeid"
}
for old_col, new_col in column_mapping.items():
    if old_col in df.columns:
        df = df.withColumnRenamed(old_col, new_col)

df.na.drop()
df.show()
# show the schema 
df.printSchema()
# Aggregate passenger count data
df =  df.withColumn("passenger_count", df["passenger_count"].cast(IntegerType())).groupBy("passenger_count") \
    .count()
    # .withColumnRenamed("passenger_count", "passenger_number") \
    # .withColumnRenamed("count", "trip_count")

df.show()

# df_passenger_number.na.drop()
# # Merge with existing Cassandra data
# df_merged = df_passenger_number.join(df_cassandra, "passenger_number", "full") \
#     .withColumn("count", coalesce(col("trip_count"), lit(0)) + coalesce(col("count"), lit(0))) \
#     .drop("trip_count")
# # Add the partition key
# df_merged = df_merged.withColumn("partition_key", lit(1))

# # Remove null values
# df_merged = df_merged.na.drop()

# df_merged.show()

# Format the passenger_number column to integer
# df_merged = df_merged.withColumn("passenger_number", df_merged["passenger_number"])

# Write updated data back to Cassandra
# df_merged.write \
#     .format("org.apache.spark.sql.cassandra") \
#     .mode("append") \
#     .options(table="passenger_number_statistics", keyspace="statistics") \
#     .save()

print("Data successfully written to Cassandra")
