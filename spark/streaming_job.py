from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, avg, min, max, sum, window, to_date, round, when, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from delta import configure_spark_with_delta_pip

S3_BUCKET = "s3a://stock-pipeline-data-lake-666a66c4"

builder = SparkSession.builder \
    .appName("StockMarketPipeline") \
    .master("local[2]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.default.parallelism", "2") \
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
    .option("startingOffsets", "latest") \
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
    .option("mergeSchema", "true") \
    .start(f"{S3_BUCKET}/silver/stock_prices")

# ============ GOLD LAYER ============
parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp").cast(TimestampType())) \
    .withWatermark("event_time", "2 minutes")

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

volume_thresholds = {
    'AAPL':  3000000,
    'GOOGL': 2000000,
    'MSFT':  3000000,
    'AMZN':  2500000,
    'TSLA':  4000000
}

gold_with_anomaly = gold_df \
    .withColumn(
        "volume_threshold",
        when(col("ticker") == "AAPL", volume_thresholds['AAPL'])
        .when(col("ticker") == "GOOGL", volume_thresholds['GOOGL'])
        .when(col("ticker") == "MSFT", volume_thresholds['MSFT'])
        .when(col("ticker") == "AMZN", volume_thresholds['AMZN'])
        .when(col("ticker") == "TSLA", volume_thresholds['TSLA'])
        .otherwise(1000000)
    ) \
    .withColumn("is_anomaly", col("total_volume") > col("volume_threshold")) \
    .withColumn(
        "anomaly_reason",
        when(col("is_anomaly") == True,
             col("total_volume").cast("string")
        ).otherwise(None)
    ) \
    .drop("volume_threshold")

gold_query = gold_with_anomaly.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{S3_BUCKET}/checkpoints/gold_v2") \
    .option("mergeSchema", "true") \
    .start(f"{S3_BUCKET}/gold/stock_prices")

print("Pipeline running...")
spark.streams.awaitAnyTermination()