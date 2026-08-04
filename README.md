# Data Lakehouse 

A modern **Data Lakehouse** pipeline combining real-time streaming ingestion with ACID-compliant transactional storage (Apache Hudi) on S3-compatible object storage (MinIO).

<!--## Flow

```
Olist CSV → Replay Producer (Python) → Kafka (KRaft) → Spark Structured Streaming → Hudi → MinIO
```
-->



## Quick Start

### 1. Start all services

```bash
./scripts/start.sh
```

This builds the Spark image with Hudi/Kafka JARs, starts Kafka (KRaft), MinIO, Spark master + worker, creates the `orders` topic and MinIO buckets.

### 2. Check service health

```bash
./scripts/status.sh
```

### 3. Download the Olist dataset

```bash
./scripts/download_dataset.sh
```

This downloads the [Brazilian E-Commerce Public Dataset (Olist)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (~100k real orders, 2016–2018) into `data/olist/`. Requires the Kaggle CLI (`pip install kaggle`) with API credentials configured.

### 4. Start the event producer

```bash
cd producer
pip install -r requirements.txt
python event_producer.py --rate 50
```

The producer reads Olist CSV files, joins orders + items + payments + customers + products, and replays ~112k enriched order-item events into Kafka at the specified rate. Use `--loop` to replay continuously.

### 5. Start the Spark streaming consumer

```bash
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-jobs/bronze_streaming_consumer.py
```

The consumer reads from Kafka, parses JSON, runs quality checks, and prints valid/quarantined records to the console.
```bash
./scripts/submit_streaming.sh          # foreground (Ctrl+C to stop)
./scripts/submit_streaming.sh --bg     # background (detached)
```

The pipeline reads from Kafka, applies 6 quality checks, routes bad records to quarantine, deduplicates valid records by `order_id`, and writes three layers to MinIO as Parquet:
- **Bronze** (`s3a://lakehouse/bronze/orders/`)  all records, raw audit zone
- **Silver** (`s3a://lakehouse/silver/orders/`)  validated + deduplicated, analytics-ready
- **Quarantine** (`s3a://lakehouse/bronze/quarantine/`)  failed QC records with failure reasons

Per-batch quality metrics are written to `s3a://lakehouse/metrics/quality/`.

#### Hudi ACID Pipeline 

```bash
./scripts/submit_streaming.sh --hudi       
./scripts/submit_streaming.sh --hudi-bg    # background (detached)
```

The Hudi pipeline upgrades silver to an **ACID-compliant Hudi table** with:
- **Upsert dedup** : cross-batch deduplication via `order_id` record key + `event_time` precombine
- **Time-travel** : query any historical snapshot via the Hudi commit timeline
- **Incremental reads** : pull only new/changed records since a given commit (CDC pattern)
- **Schema evolution** : add nullable columns without rewriting data or restarting the pipeline

Output layers:
- **Bronze** (`s3a://lakehouse/bronze/orders/`) : Parquet (raw audit)
- **Silver (Hudi)** (`s3a://lakehouse/hudi/silver/orders/`) : CoW upsert table
- **Quarantine** (`s3a://lakehouse/bronze/quarantine/`) : Parquet (failed QC)

#### Hudi Query Examples

```bash
./scripts/submit_streaming.sh --queries               # all demos
./scripts/submit_streaming.sh --queries upsert         # upsert verification only
./scripts/submit_streaming.sh --queries timetravel     # time-travel only
./scripts/submit_streaming.sh --queries incremental    # incremental reads only
```

#### Schema Evolution Demo

```bash
./scripts/submit_streaming.sh --schema
```

Adds `review_score` and `delivery_delay_days` columns to the Hudi table, demonstrating backward-compatible schema evolution.

View quality dashboard:

```bash
./scripts/submit_streaming.sh --dashboard
```

Stop the streaming pipeline:

```bash
./scripts/submit_streaming.sh --stop
```

### 6. Stop everything

```bash
./scripts/stop.sh
```

<!-- 
---

## Service Endpoints

| Service        | URL                         | Credentials            |
|----------------|-----------------------------|------------------------|
| Kafka          | `localhost:9092`            | —                      |
| MinIO API      | `http://localhost:9000`     | `minioadmin/minioadmin`|
| MinIO Console  | `http://localhost:9001`     | `minioadmin/minioadmin`|
| Spark Master   | `http://localhost:8080`     | —                      |
| Spark Worker   | `http://localhost:8081`     | —                      |
| Spark Driver   | `http://localhost:4040`     | —                      |

-->

