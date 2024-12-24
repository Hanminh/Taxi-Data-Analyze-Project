import findspark
findspark.init()
import pyspark
import os
import sys
from env_variable import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, coalesce, lit, year, month, dayofmonth, dayofyear, hour, udf, expr, to_date
from pyspark.sql.types import StringType, StructType, DoubleType, IntegerType, StructField, TimestampType

# Set up environment variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Spark session
spark = SparkSession.builder \
    .appName('Kafka Streaming to Cassandra') \
    .config('spark.default.parallelism', 100) \
    .config('spark.cassandra.connection.host', 'localhost') \
    .getOrCreate()

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

# Read streaming data from Kafka
streaming_data = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", 'test1') \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON from Kafka into a DataFrame
parsed_df = streaming_data.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
# Rename the columns to lowercase
parsed_df = parsed_df.toDF(*[c.lower() for c in parsed_df.columns])

#using watermark to handle late data
parsed_df = parsed_df.groupBy(
    year("tpep_dropoff_datetime").alias("year"),
    month("tpep_dropoff_datetime").alias("month"),
    dayofmonth("tpep_dropoff_datetime").alias("day")
).agg({"total_amount": "sum"})

#make the column timestampday is the begginning of the day of the dropoff datetime
parsed_df = parsed_df.withColumn("timestampday", to_date(expr("concat(year, '-', month, '-', day)")).cast(TimestampType()))
parsed_df = parsed_df.withWatermark("timestampday", "1 day")

def process_total_amount_statistics(batch_df, batch_id):
    df_cassandra = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="total_amount_statistics", keyspace="statistics") \
        .load()
    updated_df = batch_df.join(df_cassandra, ['year', 'month', 'day'], 'left')\
        .withColumn('final_amount', coalesce(col('sum(total_amount)'), lit(0)) + coalesce(col('total_amount'), lit(0)))\
            .drop('sum(total_amount)')\
                .drop('total_amount')\
                    .withColumnRenamed('final_amount', 'total_amount')
    updated_df.show()
    
    updated_df = updated_df.withColumn("timestamp", to_date(expr("concat(year, '-', month, '-', day)")))
    
    updated_df = updated_df.withColumn('partition_key', lit(1))
    
    #drop column timestampday
    updated_df = updated_df.drop('timestampday')
    
    updated_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="total_amount_statistics", keyspace="statistics") \
        .save()
        
query_total_amount = parsed_df.writeStream \
    .foreachBatch(process_total_amount_statistics) \
    .outputMode("complete") \
    .option("checkpointLocation", "tmp/checkpoint") \
    .start()

query_total_amount.awaitTermination()