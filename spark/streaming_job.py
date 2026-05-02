from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, avg, min, max, sum, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from delta import configure_spark_with_delta_pip

S3_BUCKET = "s3a://stock-pipeline-data-lake-666a66c4"

# Initialize Spark with Delta Lake and S3 support
builder = SparkSession.builder \
    .appName("StockMarketPipeline") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "io.delta:delta-spark_2.12:3.1.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

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
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_prices") \
    .option("startingOffsets", "earliest") \
    .load()

bronze_df = raw_stream \
    .selectExpr("CAST(key AS STRING) as ticker_key",
                "CAST(value AS STRING) as raw_value",
                "topic", "partition", "offset",
                "timestamp as kafka_timestamp") \
    .withColumn("ingestion_time", current_timestamp())

bronze_query = bronze_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{S3_BUCKET}/checkpoints/bronze") \
    .start(f"{S3_BUCKET}/bronze/stock_prices")

# ============ SILVER LAYER ============
silver_df = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("processed_time", current_timestamp()) \
    .filter(col("price").isNotNull()) \
    .filter(col("price") > 0)

silver_query = silver_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{S3_BUCKET}/checkpoints/silver") \
    .start(f"{S3_BUCKET}/silver/stock_prices")

# ============ GOLD LAYER ============
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

gold_query = gold_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{S3_BUCKET}/checkpoints/gold") \
    .start(f"{S3_BUCKET}/gold/stock_prices")

print("Pipeline running... Writing to S3.")
print(f"Bucket: {S3_BUCKET}")
print("Bronze, Silver, and Gold layers active.")

spark.streams.awaitAnyTermination()