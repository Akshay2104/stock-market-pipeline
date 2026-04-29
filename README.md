# Real-Time Stock Market Data Pipeline

A real-time data pipeline that ingests stock market data, processes it with streaming transformations, and serves it for analytics — built to learn and demonstrate Kafka, Spark Structured Streaming, and the modern data engineering stack on AWS.

## Architecture

```
Stock Data (Yahoo Finance API / Mock Generator)
        │
        ▼
  Kafka Producer (Python)
        │
        ▼
  Kafka Broker (Docker)
  Topic: stock_prices [3 partitions]
        │
        ▼
  Spark Structured Streaming (PySpark)
        │
        ▼
  S3 — Delta Lake (Medallion Architecture)
  ┌─────────┬─────────────┐
  Bronze    Silver       Gold
  (raw)     (cleaned)    (aggregated)
  └─────────┴─────────────┘
        │
        ▼
  dbt (Batch Transformations)
        │
        ▼
  Amazon Redshift (Data Warehouse)

  Docker Compose — Local infrastructure
  Terraform — S3 + Redshift provisioning
```

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Data Source | Yahoo Finance API / Mock Generator | Real-time and simulated stock price data |
| Ingestion | Python, Kafka Producer | Fetch and publish stock data to Kafka |
| Message Broker | Apache Kafka (Docker) | Decouple producers from consumers, durable message storage |
| Stream Processing | PySpark Structured Streaming | Real-time transformations, windowed aggregations |
| Storage | S3 + Delta Lake | ACID transactions, time travel, medallion architecture |
| Batch Transforms | dbt | SQL-based transformations on the gold layer |
| Data Warehouse | Amazon Redshift | Serving layer for analytics and dashboards |
| Infrastructure | Docker Compose, Terraform | Local dev orchestration, IaC for AWS provisioning |

## Project Structure

```
stock-market-pipeline/
├── docker-compose.yml          # Kafka + Zookeeper setup
├── requirement.txt             # Python dependencies
├── README.md
├── DATA_STRUCTURE.md           # Delta Lake folder structure documentation
├── .gitignore
├── producers/
│   └── stock_producer.py       # Kafka producer (mock + API support)
├── consumers/
│   └── stock_consumer.py       # Simple consumer for verification
├── spark/
│   └── streaming_job.py        # Spark Structured Streaming job (Bronze/Silver/Gold)
├── data/                       # Local Delta Lake storage (gitignored)
│   ├── bronze/                 # Raw Kafka messages
│   ├── silver/                 # Cleaned & validated data
│   ├── gold/                   # Windowed aggregations
│   └── checkpoints/            # Spark streaming checkpoints
├── dbt/
│   └── ...                     # dbt models for gold layer
└── terraform/
    └── main.tf                 # S3 + Redshift provisioning
```

## Getting Started

### Prerequisites

- Docker Desktop
- Python 3.10+
- Java 11+ (for Spark)
- AWS account (free tier)

### 1. Start Kafka Infrastructure

```bash
docker-compose up -d
```

This starts Zookeeper and a single-node Kafka broker. Verify with:

```bash
docker-compose ps
```

### 2. Create Kafka Topic

```bash
docker exec stock-market-pipeline-kafka-1 kafka-topics --create \
  --topic stock_prices \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### 3. Install Python Dependencies

Create a virtual environment and install dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirement.txt
```

**Note:** This project uses `kafka-python-ng==2.2.3` for Python 3.13+ compatibility. The original `kafka-python` package is no longer maintained and doesn't support Python 3.13+.

### 4. Run the Producer

```bash
source venv/bin/activate  # Activate virtual environment
python producers/stock_producer.py
```

The producer sends stock data (AAPL, GOOGL, MSFT, AMZN, TSLA) to Kafka every 10 seconds. Currently configured with mock data generation to avoid Yahoo Finance API rate limits. Supports live prices via Yahoo Finance API when available.

**Verify messages are flowing:**

```bash
docker exec stock-market-pipeline-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic stock_prices \
  --from-beginning \
  --max-messages 10
```

### 5. Run Spark Streaming

```bash
spark-submit spark/streaming_job.py
```

Reads from Kafka, applies transformations, and writes to Delta Lake in medallion architecture (Bronze → Silver → Gold).

**What it does:**
- **Bronze Layer**: Ingests raw Kafka messages with metadata (topic, partition, offset)
- **Silver Layer**: Parses JSON, validates data (filters null/invalid prices)
- **Gold Layer**: 1-minute windowed aggregations per ticker (avg, min, max, volume)

The streaming job creates a `data/` directory with Delta Lake tables. See [DATA_STRUCTURE.md](DATA_STRUCTURE.md) for detailed documentation on the folder structure, schemas, and how to query the data.

**Monitor the pipeline:**
```bash
# Check consumer group lag
docker exec stock-market-pipeline-kafka-1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group spark-streaming-stock-prices \
  --describe
```

### 6. AWS Infrastructure

```bash
cd terraform
terraform init
terraform apply
```

Provisions S3 buckets for Delta Lake storage and a Redshift cluster for the serving layer.

## Key Concepts Demonstrated

- **Kafka partitioning by key**: Messages are keyed by ticker symbol, ensuring all data for a given stock lands in the same partition and maintains ordering.
- **Medallion architecture**: Bronze (raw ingestion) → Silver (cleaned, validated) → Gold (aggregated, business-ready) layers in Delta Lake.
- **Delta Lake features**: ACID transactions, time travel, transaction logs, and Parquet columnar storage with Snappy compression.
- **Exactly-once semantics**: Spark checkpointing + Delta Lake ACID writes ensure no data loss or duplication.
- **Stateful streaming**: Windowed aggregations with watermarking for handling late-arriving data.
- **Decoupled architecture**: Producer and consumer operate independently — Kafka durably stores messages until they are consumed.
- **Infrastructure as Code**: Terraform manages all AWS resources for reproducibility and version control.

## Data Layer Schemas

### Bronze Layer
Raw Kafka messages with metadata: `ticker_key`, `raw_value`, `topic`, `partition`, `offset`, `kafka_timestamp`, `ingestion_time`

### Silver Layer
Parsed and validated stock data: `ticker`, `price`, `high`, `low`, `open`, `volume`, `timestamp`, `processed_time`

### Gold Layer
1-minute windowed aggregations: `window`, `ticker`, `avg_price`, `min_price`, `max_price`, `total_volume`

See [DATA_STRUCTURE.md](DATA_STRUCTURE.md) for complete documentation including query examples, maintenance operations, and troubleshooting.

## Status

- [x] Kafka + Zookeeper infrastructure (Docker Compose)
- [x] Stock data producer with mock data generator
- [x] Simple consumer for verification
- [x] Spark Structured Streaming consumer
- [x] Delta Lake medallion architecture (Bronze/Silver/Gold) - Local
- [ ] S3 integration for Delta Lake
- [ ] dbt models for analytics layer
- [ ] Redshift serving layer
- [ ] Terraform configuration (S3 + Redshift)
