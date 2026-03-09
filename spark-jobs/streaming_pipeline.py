import json
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipeline_utils import (
    KAFKA_BOOTSTRAP, KAFKA_TOPIC,
    BRONZE_PATH, SILVER_PATH, QUARANTINE_PATH, METRICS_PATH,
    PIPELINE_CHECKPOINT,
    parse_kafka_stream, apply_quality_checks,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("streaming-pipeline")

_cumulative = {
    "total": 0,
    "valid": 0,
    "quarantine": 0,
    "silver": 0,
    "dedup_removed": 0,
    "batches": 0,
}
_spark_ref = None  # set in main()

# Bronze: everything the pipeline touches (raw audit zone)
BRONZE_COLUMNS = [
    "order_id", "customer_id", "product_id", "product_category",
    "seller_id", "quantity", "unit_price", "freight_value",
    "total_price", "order_status", "customer_state", "customer_city",
    "payment_type", "payment_value",
    "event_time", "event_ts",
    "order_delivered_ts", "order_estimated_ts",
    "processing_time", "processing_ts",
    "kafka_ts", "kafka_partition", "kafka_offset",
    "msg_key", "json_str",
    "is_valid",
    "ingestion_ts",
]

# Silver: clean business + lineage columns only
SILVER_COLUMNS = [
    "order_id", "customer_id", "product_id", "product_category",
    "seller_id", "quantity", "unit_price", "freight_value",
    "total_price", "order_status", "customer_state", "customer_city",
    "payment_type", "payment_value",
    "event_time", "event_ts",
    "order_delivered_ts", "order_estimated_ts",
    "processing_time", "processing_ts",
    "kafka_ts",
    "ingestion_ts",
]

# Quarantine: bronze + all individual QC flags + raw JSON for debugging
QUARANTINE_COLUMNS = BRONZE_COLUMNS + [
    "qc_has_order_id", "qc_valid_status", "qc_valid_state",
    "qc_positive_qty", "qc_positive_price", "qc_has_event_time",
]


def process_batch(batch_df: DataFrame, batch_id: int):
    """
    Process one micro-batch:
      1. Stamp ingestion time
      2. Apply quality checks
      3. Write bronze (all records)
      4. Write quarantine (invalid records)
      5. Dedup valid records (latest event per order_id)
      6. Write silver (deduped valid records)
      7. Persist per-batch quality metrics
    """
    global _cumulative, _spark_ref

    if batch_df.rdd.isEmpty():
        log.info("Batch %d: empty — skipping.", batch_id)
        return

    batch_start = datetime.now(timezone.utc)

    stamped = batch_df.withColumn("ingestion_ts", F.current_timestamp())

    checked = apply_quality_checks(stamped)
    checked.cache()

    total_count = checked.count()

    (
        checked
        .select(*BRONZE_COLUMNS)
        .write
        .mode("append")
        .partitionBy("customer_state")
        .parquet(BRONZE_PATH)
    )

    quarantine = checked.filter(F.col("is_valid") == False)  # noqa: E712
    quarantine_count = quarantine.count()

    if quarantine_count > 0:
        (
            quarantine
            .select(*QUARANTINE_COLUMNS)
            .write
            .mode("append")
            .parquet(QUARANTINE_PATH)
        )

    #  Keep the latest event per order_id within this micro-batch using a
    #  window ranked by event_ts DESC.  This mirrors the upsert behavior
    #  that Hudi will enforce in Week 3 (precombine on event_time).
    valid = checked.filter(F.col("is_valid") == True)  # noqa: E712
    valid_count = valid.count()

    window = Window.partitionBy("order_id").orderBy(
        F.col("event_ts").desc_nulls_last()
    )
    deduped = (
        valid
        .withColumn("_dedup_rank", F.row_number().over(window))
        .filter(F.col("_dedup_rank") == 1)
        .drop("_dedup_rank")
    )
    silver_count = deduped.count()
    duplicates_removed = valid_count - silver_count

    #deduped valid records
    if silver_count > 0:
        (
            deduped
            .select(*SILVER_COLUMNS)
            .write
            .mode("append")
            .partitionBy("customer_state")
            .parquet(SILVER_PATH)
        )

    batch_duration = (datetime.now(timezone.utc) - batch_start).total_seconds()

    _cumulative["total"]         += total_count
    _cumulative["valid"]         += valid_count
    _cumulative["quarantine"]    += quarantine_count
    _cumulative["silver"]        += silver_count
    _cumulative["dedup_removed"] += duplicates_removed
    _cumulative["batches"]       += 1

    metrics = {
        "batch_id":              str(batch_id),
        "timestamp":             batch_start.isoformat(),
        "total_records":         str(total_count),
        "valid_records":         str(valid_count),
        "quarantine_records":    str(quarantine_count),
        "silver_records":        str(silver_count),
        "duplicates_removed":    str(duplicates_removed),
        "valid_pct":             str(round(valid_count / max(total_count, 1) * 100, 2)),
        "quarantine_pct":        str(round(quarantine_count / max(total_count, 1) * 100, 2)),
        "dedup_pct":             str(round(duplicates_removed / max(valid_count, 1) * 100, 2)),
        "batch_duration_sec":    str(round(batch_duration, 2)),
        "cumulative_total":      str(_cumulative["total"]),
        "cumulative_valid":      str(_cumulative["valid"]),
        "cumulative_quarantine": str(_cumulative["quarantine"]),
        "cumulative_silver":     str(_cumulative["silver"]),
        "cumulative_dedup":      str(_cumulative["dedup_removed"]),
        "cumulative_batches":    str(_cumulative["batches"]),
    }

    # Persist batch metrics as JSON to MinIO
    try:
        from pyspark.sql import Row
        metrics_df = _spark_ref.createDataFrame([Row(**metrics)])
        metrics_df.coalesce(1).write.mode("append").json(METRICS_PATH)
    except Exception as e:
        log.warning("Batch %d: failed to write metrics — %s", batch_id, e)

    checked.unpersist()

    log.info(
        "Batch %d │ total=%d  valid=%d  quarantine=%d  silver=%d  dedup_removed=%d │ %.1fs │ cumulative: %d total, %d silver",
        batch_id, total_count, valid_count, quarantine_count,
        silver_count, duplicates_removed, batch_duration,
        _cumulative["total"], _cumulative["silver"],
    )


def main():
    global _spark_ref
    spark = (
        SparkSession.builder
        .appName("StreamingPipeline_Week2")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    _spark_ref = spark

    log.info("=" * 72)
    log.info("  Week 2 — Streaming Pipeline: Kafka → Bronze + Silver + Quarantine")
    log.info("=" * 72)
    log.info("  Kafka:      %s  /  topic: %s", KAFKA_BOOTSTRAP, KAFKA_TOPIC)
    log.info("  Bronze:     %s", BRONZE_PATH)
    log.info("  Silver:     %s", SILVER_PATH)
    log.info("  Quarantine: %s", QUARANTINE_PATH)
    log.info("  Metrics:    %s", METRICS_PATH)
    log.info("  Checkpoint: %s", PIPELINE_CHECKPOINT)
    log.info("-" * 72)

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")        # process all history
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "10000")      # throttle per batch
        .load()
    )

    parsed = parse_kafka_stream(raw_stream)

    # Watermark: drop events arriving > 1 hour late (by Kafka ingest ts)
    # This protects the pipeline from unbounded state growth and handles
    # late-arriving events that are no longer relevant.
    watermarked = parsed.withWatermark("kafka_ts", "1 hour")

    # Streaming query — foreachBatch handles bronze/silver/quarantine ──
    query = (
        watermarked
        .writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", PIPELINE_CHECKPOINT)
        .trigger(processingTime="30 seconds")
        .queryName("streaming-pipeline")
        .start()
    )

    log.info("Streaming query started — awaiting termination …")
    log.info(
        "Stop gracefully with:  docker exec lakehouse-spark-master "
        "pkill -f streaming_pipeline"
    )
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
