from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, TimestampType, DoubleType
from pyspark.sql.functions import col, window, avg, stddev, expr, collect_list, udf

# Spark session
spark = SparkSession.builder \
    .appName("TSLA_Combined_AnomalyDetection") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

# Schema
schema = StructType() \
    .add("Datetime", TimestampType()) \
    .add("Open", DoubleType()) \
    .add("High", DoubleType()) \
    .add("Low", DoubleType()) \
    .add("Close", DoubleType()) \
    .add("Adj Close", DoubleType()) \
    .add("Volume", DoubleType())

# Streaming source
df = spark.readStream \
    .option("header", "true") \
    .schema(schema) \
    .csv("data/stream_input")

# EWMA UDF
def compute_ewma(prices, alpha=0.3):
    if not prices:
        return None
    ewma = prices[0]
    for price in prices[1:]:
        ewma = alpha * price + (1 - alpha) * ewma
    return ewma

ewma_udf = udf(compute_ewma, DoubleType())

# Windowing and feature computation
windowed_df = df.withWatermark("Datetime", "2 minutes") \
    .groupBy(window("Datetime", "5 minutes", "1 minute")) \
    .agg(
        avg("Close").alias("mean_close"),
        stddev("Close").alias("std_close"),
        expr("last(Close)").alias("latest_close"),
        collect_list("Close").alias("close_list")
    )

# Add EWMA, Z-score, Bollinger Bands
anomaly_df = windowed_df \
    .withColumn("EWMA", ewma_udf("close_list")) \
    .withColumn("z_score", (col("latest_close") - col("mean_close")) / col("std_close")) \
    .withColumn("is_z_anomaly", expr("abs(z_score) > 2")) \
    .withColumn("upper_band", col("mean_close") + 2 * col("std_close")) \
    .withColumn("lower_band", col("mean_close") - 2 * col("std_close")) \
    .withColumn("is_band_anomaly", (col("latest_close") > col("upper_band")) | (col("latest_close") < col("lower_band")))

# Output to file
query = anomaly_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    "latest_close", "EWMA", "z_score", "is_z_anomaly",
    "upper_band", "lower_band", "is_band_anomaly"
).writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "data/output/combined_anomalies") \
    .option("checkpointLocation", "data/output/checkpoint_combined") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
