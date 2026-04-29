# Data Folder Structure

This document explains the structure of the `data/` directory created by the Spark Structured Streaming job.

## Overview

The data folder implements the **Medallion Architecture** (Bronze → Silver → Gold) using **Delta Lake** for ACID transactions and time travel capabilities.

```
data/
├── bronze/                    # Raw data layer
│   └── stock_prices/
│       ├── _delta_log/        # Delta Lake transaction log (52 files)
│       │   └── *.json         # Transaction metadata & schema
│       └── *.parquet          # 60 parquet files (852KB total)
│
├── silver/                    # Cleaned data layer
│   └── stock_prices/
│       ├── _delta_log/        # Delta Lake transaction log (50 files)
│       │   └── *.json
│       └── *.parquet          # 59 parquet files (788KB total)
│
├── gold/                      # Aggregated data layer
│   └── stock_prices/
│       ├── _delta_log/        # Delta Lake transaction log (80 files)
│       │   └── *.json
│       └── *.parquet          # 213 parquet files (2.2MB total)
│
└── checkpoints/               # Spark streaming checkpoints
    ├── bronze/
    │   ├── commits/           # Micro-batch commit tracking
    │   ├── offsets/           # Kafka offset tracking
    │   └── sources/0/         # Source metadata
    ├── silver/
    │   ├── commits/
    │   ├── offsets/
    │   └── sources/0/
    └── gold/
        ├── commits/
        ├── offsets/
        ├── sources/0/
        └── state/0/           # Windowed aggregation state
```

## Layer Details

### Bronze Layer (Raw Ingestion)

**Location:** `data/bronze/stock_prices/`

**Purpose:** Store raw Kafka messages exactly as received, with minimal transformation.

**Schema:**
```
ticker_key      STRING      # Kafka message key (ticker symbol)
raw_value       STRING      # JSON payload from Kafka (unparsed)
topic           STRING      # Kafka topic name
partition       INTEGER     # Kafka partition number
offset          LONG        # Kafka message offset
kafka_timestamp TIMESTAMP   # When message was published to Kafka
ingestion_time  TIMESTAMP   # When Spark ingested the message
```

**Defined in:** `spark/streaming_job.py:38-47`

**Key Characteristics:**
- No data validation or filtering
- Preserves original Kafka metadata (partition, offset, timestamp)
- JSON data stored as string for auditability
- Enables replay and debugging of raw data

**Storage:**
- 60 parquet files (snappy compressed)
- Total size: 852KB
- ~810 records in first micro-batch

### Silver Layer (Cleaned Data)

**Location:** `data/silver/stock_prices/`

**Purpose:** Parse JSON, validate data quality, and prepare for analytics.

**Schema:**
```
ticker          STRING      # Stock symbol (AAPL, GOOGL, etc.)
price           DOUBLE      # Current price
high            DOUBLE      # Day high
low             DOUBLE      # Day low
open            DOUBLE      # Opening price
volume          INTEGER     # Trading volume
timestamp       STRING      # Event timestamp from producer
processed_time  TIMESTAMP   # When Spark processed this record
```

**Defined in:** `spark/streaming_job.py:51-64`

**Transformations Applied:**
- Parse JSON from `raw_value` field
- Apply schema validation
- Filter out null prices: `col("price").isNotNull()`
- Filter invalid prices: `col("price") > 0`
- Add `processed_time` for tracking

**Storage:**
- 59 parquet files (snappy compressed)
- Total size: 788KB
- Slightly smaller than bronze due to filtering

### Gold Layer (Aggregated Analytics)

**Location:** `data/gold/stock_prices/`

**Purpose:** Business-ready aggregations for analytics and reporting.

**Schema:**
```
window          STRUCT      # Time window (start, end)
  - start       TIMESTAMP
  - end         TIMESTAMP
ticker          STRING      # Stock symbol
avg_price       DOUBLE      # Average price in window
min_price       DOUBLE      # Minimum price (from 'low' field)
max_price       DOUBLE      # Maximum price (from 'high' field)
total_volume    LONG        # Sum of volume in window
```

**Defined in:** `spark/streaming_job.py:68-90`

**Aggregations:**
- **Window Size:** 1 minute (tumbling window)
- **Watermark:** 2 minutes (handles late-arriving data)
- **Group By:** `window` + `ticker`
- **Metrics:**
  - `avg("price")` - Average price
  - `min("low")` - Lowest price in window
  - `max("high")` - Highest price in window
  - `sum("volume")` - Total trading volume

**Storage:**
- 213 parquet files (most files due to many time windows)
- Total size: 2.2MB (largest layer)
- Each window creates new partitions

**Use Cases:**
- Real-time dashboards
- Trend analysis
- Volume-weighted average price (VWAP)
- Price volatility detection

## Delta Lake Transaction Logs

Each layer maintains a `_delta_log/` directory containing JSON transaction files.

### Log File Naming
- `00000000000000000000.json` - Initial transaction (metadata + protocol)
- `00000000000000000001.json` - Second transaction
- Sequential numbering for each write operation

### Transaction Log Contents

**Example from `data/bronze/stock_prices/_delta_log/00000000000000000000.json`:**

```json
{
  "commitInfo": {
    "timestamp": 1777431305463,
    "operation": "STREAMING UPDATE",
    "operationParameters": {
      "outputMode": "Append",
      "queryId": "723cb8ea-90ae-4a6c-b187-7110582b1c80",
      "epochId": "0"
    },
    "operationMetrics": {
      "numRemovedFiles": "0",
      "numOutputRows": "2025",
      "numOutputBytes": "126865",
      "numAddedFiles": "3"
    },
    "engineInfo": "Apache-Spark/3.5.1 Delta-Lake/3.1.0"
  }
}
```

**What the log tracks:**
- Schema definitions and evolution
- Files added/removed in each transaction
- Row counts and data statistics
- Min/max values for optimization
- Protocol versions for compatibility

### Benefits of Delta Lake

1. **ACID Transactions:** All writes are atomic
2. **Time Travel:** Query historical versions
3. **Schema Evolution:** Safely modify schema over time
4. **Audit Trail:** Complete history of changes
5. **Optimized Reads:** Statistics enable file skipping

## Checkpoints (Exactly-Once Semantics)

**Location:** `data/checkpoints/{bronze,silver,gold}/`

**Purpose:** Enable exactly-once processing semantics and fault tolerance.

### Checkpoint Components

#### 1. `commits/` Directory
- Tracks which micro-batches have been successfully committed
- Prevents reprocessing of already-processed data
- Critical for idempotency

#### 2. `offsets/` Directory
- Stores Kafka partition offsets for each micro-batch
- Enables resumption from last processed offset after failure
- Format: JSON files with partition → offset mappings

#### 3. `sources/0/` Directory
- Kafka source configuration and metadata
- Partition assignments
- Consumer group information

#### 4. `state/0/` Directory (Gold Layer Only)
- Maintains aggregation state for windowed operations
- Stores partial aggregates across micro-batches
- Required for stateful streaming transformations
- Uses RocksDB for efficient state management

### How Checkpoints Enable Fault Tolerance

1. **Spark Job Crashes:**
   - Restart reads checkpoint to find last committed batch
   - Resumes from last processed Kafka offset
   - Reprocesses uncommitted data only

2. **Kafka Offset Management:**
   - Checkpoint stores: `{partition_0: 640, partition_1: 320, partition_2: 640}`
   - On restart, Spark reads from these exact offsets
   - No data loss or duplication

3. **Stateful Aggregations:**
   - Gold layer checkpoint preserves window state
   - Partial aggregates survive restarts
   - Watermark state maintained

## File Format: Parquet

All data files use **Apache Parquet** with **Snappy compression**.

### Why Parquet?

- **Columnar Storage:** Read only needed columns
- **Compression:** Snappy provides good balance of speed/size
- **Schema Embedded:** Self-describing files
- **Optimized for Analytics:** Perfect for OLAP queries
- **Widely Compatible:** Works with Spark, Athena, Redshift Spectrum

### Naming Convention

```
part-00000-a6171083-ef8a-4a11-9eed-a84194e4eae3-c000.snappy.parquet
│    │     └─────────────── UUID ────────────────┘ │    └─── compression
│    └─ Task ID                                    └─ Attempt number
└─ Partition number
```

## Storage Statistics

| Layer  | Parquet Files | Total Size | Avg File Size |
|--------|---------------|------------|---------------|
| Bronze | 60            | 852KB      | 14.2KB        |
| Silver | 59            | 788KB      | 13.4KB        |
| Gold   | 213           | 2.2MB      | 10.6KB        |

**Why Gold is largest:**
- Many time windows create many files
- Each 1-minute window produces new partitions
- Aggregation metadata adds overhead

## Data Flow Summary

```
Kafka (stock_prices topic)
        │
        ▼
┌───────────────────────────────────────────────────┐
│ BRONZE: Raw Kafka Messages                        │
│ - Preserves original JSON                         │
│ - Stores Kafka metadata                           │
│ - No transformations                              │
└───────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────────┐
│ SILVER: Cleaned & Validated                       │
│ - Parse JSON to structured columns                │
│ - Filter nulls and invalid prices                 │
│ - Ready for queries                               │
└───────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────────┐
│ GOLD: Aggregated Analytics                        │
│ - 1-minute windowed aggregations                  │
│ - Per-ticker summaries                            │
│ - Business KPIs (avg, min, max, volume)           │
└───────────────────────────────────────────────────┘
```

## Querying the Data

### Read Bronze Layer
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

bronze_df = spark.read.format("delta").load("data/bronze/stock_prices")
bronze_df.show()
```

### Read Silver Layer
```python
silver_df = spark.read.format("delta").load("data/silver/stock_prices")
silver_df.filter("ticker = 'AAPL'").show()
```

### Read Gold Layer
```python
gold_df = spark.read.format("delta").load("data/gold/stock_prices")
gold_df.orderBy("window").show(truncate=False)
```

### Time Travel (Query Historical Data)
```python
# Query data as of version 5
historical_df = spark.read.format("delta") \
    .option("versionAsOf", 5) \
    .load("data/silver/stock_prices")

# Query data as of timestamp
historical_df = spark.read.format("delta") \
    .option("timestampAsOf", "2026-04-28 22:55:00") \
    .load("data/silver/stock_prices")
```

## Migration to S3

This local structure is designed to be deployed to S3 with minimal changes:

**Current (Local):**
```
data/bronze/stock_prices
data/silver/stock_prices
data/gold/stock_prices
```

**Future (S3):**
```
s3://your-bucket/delta-lake/bronze/stock_prices
s3://your-bucket/delta-lake/silver/stock_prices
s3://your-bucket/delta-lake/gold/stock_prices
```

Simply update the paths in `spark/streaming_job.py` and configure AWS credentials.

## Maintenance Operations

### Compact Small Files (Optimize)
```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "data/gold/stock_prices")
delta_table.optimize().executeCompaction()
```

### Vacuum Old Files (Delete after 7 days retention)
```python
delta_table.vacuum(retentionHours=168)  # 7 days
```

### View Transaction History
```python
delta_table.history().show()
```

## Best Practices

1. **Checkpoint Management:**
   - Checkpoints grow over time
   - Monitor checkpoint directory size
   - Consider periodic cleanup for very long-running jobs

2. **File Size Optimization:**
   - Target 128MB - 1GB per parquet file
   - Use `optimize()` to compact small files
   - Configure `spark.sql.files.maxRecordsPerFile`

3. **Retention Policies:**
   - Run `vacuum()` to remove old file versions
   - Default retention: 7 days
   - Keep longer for audit requirements

4. **Schema Evolution:**
   - Use `mergeSchema` option when schema changes
   - Delta Lake handles schema evolution automatically
   - Test schema changes in development first

## Troubleshooting

### Q: Why are there so many small files?

A: Spark Structured Streaming creates a new file for each micro-batch (default: every few seconds). Use `trigger` option to control batch frequency:

```python
.trigger(processingTime="30 seconds")
```

### Q: How do I clean up checkpoint data?

A: Stop the streaming job first, then delete and recreate:

```bash
rm -rf data/checkpoints/bronze
# Restart job - it will recreate checkpoints from earliest offset
```

### Q: What happens if I delete `_delta_log`?

A: **Don't do this!** Delta log is critical. Without it:
- Data files become orphaned
- ACID guarantees lost
- Time travel impossible
- Schema information lost

### Q: Can I query with regular Spark (without Delta)?

A: No. Delta Lake files require Delta reader. But you can export:

```python
silver_df = spark.read.format("delta").load("data/silver/stock_prices")
silver_df.write.parquet("output/silver_export")
```

## Related Documentation

- `spark/streaming_job.py` - Streaming job implementation
- `README.md` - Project overview and setup
- `consumer-group-comparison.md` - Kafka consumer group analysis
- Delta Lake Documentation: https://docs.delta.io/
- Spark Structured Streaming: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
