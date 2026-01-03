from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, TimestampType, DoubleType
from pyspark.sql.functions import col, window, collect_list, udf
from pyspark.sql.types import ArrayType

# Create SparkSession
spark = SparkSession.builder \
    .appName("TSLA_EWMA") \
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

# Group into 5-minute windows sliding every 1 minute
windowed_df = df \
    .withWatermark("Datetime", "2 minutes") \
    .groupBy(window("Datetime", "5 minutes", "1 minute")) \
    .agg(collect_list("Close").alias("close_prices"))

# Define EWMA UDF
def compute_ewma(prices, alpha=0.3):
    if not prices:
        return None
    ewma = prices[0]
    for price in prices[1:]:
        ewma = alpha * price + (1 - alpha) * ewma
    return ewma

ewma_udf = udf(compute_ewma, DoubleType())

# Apply EWMA UDF to windowed prices
result_df = windowed_df.withColumn("EWMA", ewma_udf(col("close_prices")))

# Output
query = result_df.select("window", "EWMA").writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()