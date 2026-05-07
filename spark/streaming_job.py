from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, avg, min, max, sum, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import to_date
from pyspark.sql.functions import from_json, col, current_timestamp, avg, min, max, sum, window, to_date, round, when, lag
from pyspark.sql.window import Window

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
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("trade_date", to_date(col("kafka_timestamp")))

bronze_query = bronze_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .partitionBy("trade_date") \
    .option("checkpointLocation", f"{S3_BUCKET}/checkpoints/bronze") \
    .start(f"{S3_BUCKET}/bronze/stock_prices")

# ============ SILVER LAYER ============
silver_df = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("processed_time", current_timestamp()) \
    .withColumn("trade_date", to_date(col("timestamp"))) \
    .filter(col("price").isNotNull()) \
    .filter(col("price") > 0)

silver_query = silver_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .partitionBy("trade_date") \
    .option("checkpointLocation", f"{S3_BUCKET}/checkpoints/silver") \
    .start(f"{S3_BUCKET}/silver/stock_prices")

# ============ GOLD LAYER ============
# Parse and prepare the stream for aggregation
parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp").cast(TimestampType())) \
    .withWatermark("event_time", "2 minutes")

# Step 1 — Aggregate into 1-minute windows per ticker
gold_df = parsed_stream \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("ticker")
    ) \
    .agg(
        round(avg("price"), 2).alias("avg_price"),
        min("low").alias("min_price"),
        max("high").alias("max_price"),
        sum("volume").alias("total_volume")
    )

# Step 2 — Calculate volume baseline using last 5 windows per ticker
# Window spec: look back at previous 5 rows for the same ticker, ordered by window start time
ticker_window = Window \
    .partitionBy("ticker") \
    .orderBy("window") \
    .rowsBetween(-5, -1)  # look at previous 5 rows, not including current row

# Add baseline average volume column
gold_with_baseline = gold_df \
    .withColumn(
        "avg_volume_baseline",
        round(avg("total_volume").over(ticker_window), 0)
    )

# Step 3 — Flag anomalies where volume exceeds 2x baseline
gold_with_anomaly = gold_with_baseline \
    .withColumn(
        "volume_ratio",
        round(col("total_volume") / col("avg_volume_baseline"), 2)
    ) \
    .withColumn(
        "is_anomaly",
        when(
            col("avg_volume_baseline").isNull(), False  # not enough history yet
        ).when(
            col("total_volume") > col("avg_volume_baseline") * 2, True
        ).otherwise(False)
    ) \
    .withColumn(
        "anomaly_reason",
        when(
            col("is_anomaly") == True,
            # Builds a readable reason string e.g. "Volume 3.5x above 5-window baseline"
            col("volume_ratio").cast("string")
        ).otherwise(None)
    )

# Write Gold to S3 Delta Lake
gold_query = gold_with_anomaly.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{S3_BUCKET}/checkpoints/gold_v2") \
    .option("mergeSchema", "true") \
    .start(f"{S3_BUCKET}/gold/stock_prices")