from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, TimestampType, DoubleType
from pyspark.sql.functions import col, window, avg, stddev, expr

# Create SparkSession
spark = SparkSession.builder \
    .appName("TSLA_BollingerBands") \
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

# Apply 5-minute sliding window
windowed_df = df \
    .withWatermark("Datetime", "2 minutes") \
    .groupBy(window("Datetime", "5 minutes", "1 minute")) \
    .agg(
        avg("Close").alias("mean_close"),
        stddev("Close").alias("std_close"),
        expr("last(Close)").alias("latest_close")
    )

# Compute Bollinger Bands and check for anomalies
bollinger_df = windowed_df \
    .withColumn("upper_band", col("mean_close") + 2 * col("std_close")) \
    .withColumn("lower_band", col("mean_close") - 2 * col("std_close")) \
    .withColumn("is_outside_band",
                (col("latest_close") > col("upper_band")) |
                (col("latest_close") < col("lower_band")))

# Output flagged data
query = bollinger_df.select("window", "latest_close", "upper_band", "lower_band", "is_outside_band").writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()