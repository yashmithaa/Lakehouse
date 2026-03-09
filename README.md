# Incremental Data Lakehouse [ACID & Time Travel]

A modern **Data Lakehouse** pipeline combining real-time streaming ingestion (Kafka → Spark) with ACID-compliant transactional storage (Apache Hudi) on S3-compatible object storage (MinIO).

## Architecture

```
Olist CSV → Replay Producer (Python) → Kafka (KRaft) → Spark Structured Streaming → Hudi → MinIO
```

---

## Quick Start

### Prerequisites

- **Docker** ≥ 24.0 and **Docker Compose** v2
- **Python** ≥ 3.10 (for the producer)

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
- **Bronze** (`s3a://lakehouse/bronze/orders/`) — all records, raw audit zone
- **Silver** (`s3a://lakehouse/silver/orders/`) — validated + deduplicated, analytics-ready
- **Quarantine** (`s3a://lakehouse/bronze/quarantine/`) — failed QC records with failure reasons

Per-batch quality metrics are written to `s3a://lakehouse/metrics/quality/`.

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

---

## Project Structure

```
.
├── docker-compose.yml              # Full stack definition
├── docker/spark/Dockerfile         # Spark image + Hudi/Kafka/S3 JARs
├── conf/spark-defaults.conf        # Spark config (Hudi, S3, tuning)
├── data/olist/                     # Olist CSV files (downloaded, git-ignored)
├── producer/
│   ├── event_producer.py           # Olist dataset replay producer
│   └── requirements.txt            # Python dependencies
├── spark-jobs/
│   ├── bronze_streaming_consumer.py  # Week 1: Kafka → console (debug)
│   ├── pipeline_utils.py             # Week 2: shared schema, QC, paths
│   ├── streaming_pipeline.py         # Week 2: Kafka → Bronze + Silver + Quarantine
│   └── quality_dashboard.py          # Week 2: quality metrics report
├── scripts/
│   ├── start.sh                    # One-command startup
│   ├── stop.sh                     # One-command teardown
│   ├── status.sh                   # Health check
│   ├── download_dataset.sh         # Download Olist dataset
│   └── submit_streaming.sh         # submit/stop streaming pipeline
```

