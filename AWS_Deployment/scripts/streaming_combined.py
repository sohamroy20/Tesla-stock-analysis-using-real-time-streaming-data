from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, TimestampType, DoubleType
from pyspark.sql.functions import col, window, avg, stddev, expr, collect_list, udf

# Spark session
# Note: you must change the Spark driver host address to that of you
# Name node's IP address recognized by the Data nodes, as specified
# in your Hadoop configuration (core-site.xml, hdfs-site.xml, etc.)
spark = SparkSession.builder \
    .appName("TSLA_Combined_AnomalyDetection") \
    .master("yarn") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.driver.host", "xxx.xx.xx.xxx") \
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
# Note: you must update the IP address to that of your Name node.
# If HDFS is accessible via a different port in your configuration,
# update the port too.
df = spark.readStream \
    .option("header", "true") \
    .schema(schema) \
    .csv("hdfs://xxx.xx.xx.xxx:54310/Workshop/data/stream_input")

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
# Note: you must update the IP addresses in the file path to that of your Name node.
query = anomaly_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    "latest_close", "EWMA", "z_score", "is_z_anomaly",
    "upper_band", "lower_band", "is_band_anomaly"
).writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "hdfs://xxx.xx.xx.xxx:54310/Workshop/data/output/combined_anomalies") \
    .option("checkpointLocation", "hdfs://xxx.xx.xx.xxx:54310/Workshop/data/output/checkpoint_combined") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
