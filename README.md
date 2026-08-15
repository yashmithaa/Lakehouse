# Data Lakehouse 

A **Data Lakehouse** pipeline combining real-time streaming ingestion with ACID-compliant transactional storage (Apache Hudi) on S3-compatible object storage (MinIO).

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

### 4. Install producer dependencies once 

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r producer/requirements.txt
```

Start producer in one terminal
```bash
python event_producer.py --rate 50
```

The producer reads Olist CSV files, joins orders + items + payments + customers + products, and replays ~112k enriched order-item events into Kafka at the specified rate. Use `--loop` to replay continuously.

### 5. Start Hudi pipeline
in 2nd terminal
```bash
./scripts/submit_streaming.sh --hudi-bg
```

After 1-3 minutes inspect the quality and Hudi behavior
```bash
./scripts/submit_streaming.sh --dashboard
./scripts/submit_streaming.sh --queries
./scripts/submit_streaming.sh --queries upsert
./scripts/submit_streaming.sh --queries timetravel
./scripts/submit_streaming.sh --queries incremental
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

### 6. Build Gold tables

```bash
./scripts/submit_streaming.sh --gold
```

### 7. Run analytics

```bash
./scripts/submit_streaming.sh --analytics
./scripts/submit_streaming.sh --trino-sql
```


### 8. Open Trino and query interactively
```bash
docker exec -it lakehouse-trino trino
```
### 9. Run maintenance job
```bash
./scripts/submit_streaming.sh --maintenance
```

### 10. Stop the streaming pipeline when done
```bash
./scripts/submit_streaming.sh --stop
```
### 11. Stop everything
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

