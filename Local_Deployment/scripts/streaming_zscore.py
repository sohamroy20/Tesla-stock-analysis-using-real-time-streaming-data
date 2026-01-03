from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, TimestampType, DoubleType
from pyspark.sql.functions import col, window, avg, stddev, expr

# Create SparkSession
spark = SparkSession.builder \
    .appName("TSLA_ZScore") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

# Define schema
schema = StructType() \
    .add("Datetime", TimestampType()) \
    .add("Open", DoubleType()) \
    .add("High", DoubleType()) \
    .add("Low", DoubleType()) \
    .add("Close", DoubleType()) \
    .add("Adj Close", DoubleType()) \
    .add("Volume", DoubleType())

# Read stream
df = spark.readStream \
    .option("header", "true") \
    .schema(schema) \
    .csv("data/stream_input")

# Add watermark and apply sliding window
windowed_df = df \
    .withWatermark("Datetime", "2 minutes") \
    .groupBy(window("Datetime", "5 minutes", "1 minute")) \
    .agg(
        avg("Close").alias("mean_close"),
        stddev("Close").alias("std_close"),
        expr("last(Close)").alias("latest_close")
    )

# Compute Z-score
zscore_df = windowed_df \
    .withColumn("z_score", (col("latest_close") - col("mean_close")) / col("std_close")) \
    .withColumn("is_anomaly", expr("abs(z_score) > 2"))

# Output anomalies only
query = zscore_df.select("window", "latest_close", "z_score", "is_anomaly").writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()