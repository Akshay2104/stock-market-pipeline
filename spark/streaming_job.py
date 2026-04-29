from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, avg, min, max, sum, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from delta import configure_spark_with_delta_pip

# Initialize Spark with Delta Lake
builder = SparkSession.builder \
    .appName("StockMarketPipeline") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.1.0")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Define schema for our stock data
schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("open", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

# ============ BRONZE LAYER ============
# Read raw data from Kafka - no transformations
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_prices") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the Kafka message value from bytes to JSON
bronze_df = raw_stream \
    .selectExpr("CAST(key AS STRING) as ticker_key", "CAST(value AS STRING) as raw_value", "topic", "partition", "offset", "timestamp as kafka_timestamp") \
    .withColumn("ingestion_time", current_timestamp())

# Write Bronze to Delta Lake
bronze_query = bronze_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "data/checkpoints/bronze") \
    .start("data/bronze/stock_prices")

# ============ SILVER LAYER ============
# Parse JSON, apply schema, clean data
silver_df = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("processed_time", current_timestamp()) \
    .filter(col("price").isNotNull()) \
    .filter(col("price") > 0)

# Write Silver to Delta Lake
silver_query = silver_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "data/checkpoints/silver") \
    .start("data/silver/stock_prices")

# ============ GOLD LAYER ============
# Aggregate: 1-minute window summaries per ticker
gold_df = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp").cast(TimestampType())) \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("ticker")
    ) \
    .agg(
        avg("price").alias("avg_price"),
        min("low").alias("min_price"),
        max("high").alias("max_price"),
        sum("volume").alias("total_volume")
    )

# Write Gold to Delta Lake
gold_query = gold_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "data/checkpoints/gold") \
    .start("data/gold/stock_prices")

print("Pipeline running... Bronze, Silver, and Gold layers active.")
print("Writing to data/bronze, data/silver, data/gold")

# Keep all streams running
spark.streams.awaitAnyTermination()