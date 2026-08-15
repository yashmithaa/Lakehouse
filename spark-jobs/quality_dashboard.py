from pyspark.sql import SparkSession
from pyspark.sql import functions as F


METRICS_PATH    = "s3a://lakehouse/metrics/quality/"
BRONZE_PATH     = "s3a://lakehouse/bronze/orders/"
SILVER_PATH     = "s3a://lakehouse/silver/orders/"
HUDI_SILVER_PATH = "s3a://lakehouse/hudi/silver/orders/"
QUARANTINE_PATH = "s3a://lakehouse/bronze/quarantine/"


def main():
    spark = (
        SparkSession.builder
        .appName("QualityDashboard_Week3")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    sep = "=" * 72

    # Batch-level quality metrics 
    print(f"\n{sep}")
    print("  DATA QUALITY DASHBOARD")
    print(sep)

    try:
        metrics = (
            spark.read.json(METRICS_PATH)
            .withColumn("batch_id_int",           F.col("batch_id").cast("int"))
            .withColumn("total_records_int",      F.col("total_records").cast("int"))
            .withColumn("valid_records_int",      F.col("valid_records").cast("int"))
            .withColumn("quarantine_records_int", F.col("quarantine_records").cast("int"))
            # Handle both Week 2 (silver_records) and Week 3 (silver_upserted) metrics
            .withColumn("silver_records_int",
                F.coalesce(
                    F.col("silver_records").cast("int"),
                    F.col("silver_upserted").cast("int"),
                    F.lit(0),
                ))
            .withColumn("duplicates_removed_int", F.col("duplicates_removed").cast("int"))
            .withColumn("valid_pct_dbl",          F.col("valid_pct").cast("double"))
            .withColumn("quarantine_pct_dbl",     F.col("quarantine_pct").cast("double"))
            .withColumn("dedup_pct_dbl",          F.col("dedup_pct").cast("double"))
            .withColumn("batch_duration_dbl",     F.col("batch_duration_sec").cast("double"))
            .orderBy("batch_id_int")
        )
        metrics.cache()
        batch_count = metrics.count()

        if batch_count == 0:
            print("\n  No metrics data found. Run the streaming pipeline first.\n")
            return

        summary = metrics.agg(
            F.sum("total_records_int").alias("total_records"),
            F.sum("valid_records_int").alias("valid_records"),
            F.sum("quarantine_records_int").alias("quarantine_records"),
            F.sum("silver_records_int").alias("silver_records"),
            F.sum("duplicates_removed_int").alias("duplicates_removed"),
            F.avg("valid_pct_dbl").alias("avg_valid_pct"),
            F.avg("quarantine_pct_dbl").alias("avg_quarantine_pct"),
            F.avg("dedup_pct_dbl").alias("avg_dedup_pct"),
            F.avg("batch_duration_dbl").alias("avg_batch_duration_sec"),
            F.max("batch_duration_dbl").alias("max_batch_duration_sec"),
            F.count("*").alias("batch_count"),
        ).collect()[0]

        print(f"\n  Batches processed:    {summary['batch_count']}")
        print(f"  Total records:        {summary['total_records']}")
        print(f"  Valid records:        {summary['valid_records']}")
        print(f"  Quarantine records:   {summary['quarantine_records']}")
        print(f"  Silver records:       {summary['silver_records']}")
        print(f"  Duplicates removed:   {summary['duplicates_removed']}")
        print(f"\n  Avg valid %%:          {summary['avg_valid_pct']:.2f}%")
        print(f"  Avg quarantine %%:     {summary['avg_quarantine_pct']:.2f}%")
        print(f"  Avg dedup %%:          {summary['avg_dedup_pct']:.2f}%")
        print(f"  Avg batch duration:   {summary['avg_batch_duration_sec']:.2f}s")
        print(f"  Max batch duration:   {summary['max_batch_duration_sec']:.2f}s")

        print(f"\n{sep}")
        print("  PER-BATCH BREAKDOWN")
        print(sep)
        (
            metrics
            .select(
                "batch_id_int", "total_records_int", "valid_records_int",
                "quarantine_records_int", "silver_records_int",
                "duplicates_removed_int", "valid_pct", "batch_duration_sec",
            )
            .show(100, truncate=False)
        )

        metrics.unpersist()

    except Exception as e:
        print(f"\n  Could not read metrics: {e}")
        print("  Make sure the streaming pipeline has run at least one batch.\n")

    print(f"\n{sep}")
    print("  LAYER RECORD COUNTS")
    print(sep)

    for label, path, fmt in [
        ("Bronze",        BRONZE_PATH,      "parquet"),
        ("Silver (Prq)",  SILVER_PATH,      "parquet"),
        ("Silver (Hudi)", HUDI_SILVER_PATH, "hudi"),
        ("Quarantine",    QUARANTINE_PATH,  "parquet"),
    ]:
        try:
            if fmt == "hudi":
                count = spark.read.format("hudi").load(path).count()
            else:
                count = spark.read.parquet(path).count()
            print(f"  {label:18s}  {count:>10,} records")
        except Exception:
            print(f"  {label:18s}  (no data yet)")

    # ── Hudi commit timeline ──────────────────────────────────────────
    print(f"\n{sep}")
    print("  HUDI SILVER — COMMIT TIMELINE")
    print(sep)

    try:
        hudi_silver = spark.read.format("hudi").load(HUDI_SILVER_PATH)
        timeline = (
            hudi_silver
            .groupBy("_hoodie_commit_time")
            .agg(F.count("*").alias("records"))
            .orderBy("_hoodie_commit_time")
        )
        commit_count = timeline.count()
        print(f"\n  Total commits: {commit_count}")
        timeline.show(50, truncate=False)

        # Unique order_ids (proves upsert dedup)
        unique_orders = hudi_silver.select("order_id").distinct().count()
        total_rows = hudi_silver.count()
        print(f"  Total rows:        {total_rows:,}")
        print(f"  Unique order_ids:  {unique_orders:,}")
        print(f"  Upsert dedup OK:   {'YES ✓' if total_rows == unique_orders else 'NO ✗'}")
    except Exception:
        print("  (no Hudi silver data yet — run hudi_streaming_pipeline first)")

    print(f"\n{sep}")
    print("  SILVER DATA SAMPLE (latest 10 records by event_ts)")
    print(sep)

    # Prefer Hudi silver, fall back to Parquet silver
    silver_read = False
    try:
        silver = spark.read.format("hudi").load(HUDI_SILVER_PATH)
        silver_read = True
        print("  (reading from Hudi silver)")
    except Exception:
        try:
            silver = spark.read.parquet(SILVER_PATH)
            silver_read = True
            print("  (reading from Parquet silver — Hudi not available)")
        except Exception:
            pass

    if silver_read:
        (
            silver
            .orderBy(F.col("event_ts").desc())
            .select(
                "order_id", "order_status", "customer_state",
                "total_price", "payment_type", "event_ts",
            )
            .show(10, truncate=False)
        )
    else:
        print("  (no silver data yet)\n")

    print(f"\n{sep}")
    print("  QUARANTINE FAILURE REASONS")
    print(sep)

    try:
        quarantine = spark.read.parquet(QUARANTINE_PATH)
        (
            quarantine
            .groupBy(
                "qc_has_order_id", "qc_valid_status", "qc_valid_state",
                "qc_positive_qty", "qc_positive_price", "qc_has_event_time",
            )
            .count()
            .orderBy(F.col("count").desc())
            .show(20, truncate=False)
        )
    except Exception:
        print("  (no quarantine data yet)\n")

    print(f"\n{sep}")
    print("  Dashboard complete.")
    print(f"{sep}\n")

    spark.stop()


if __name__ == "__main__":
    main()
