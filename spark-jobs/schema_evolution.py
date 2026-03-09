import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType,
)

from pipeline_utils import (
    HUDI_SILVER_PATH, HUDI_COMMON_OPTS, HUDI_TABLE_NAME,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("schema-evolution")

SEP = "=" * 76


def main():
    spark = (
        SparkSession.builder
        .appName("SchemaEvolution_Week3")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{SEP}")
    print("  SCHEMA EVOLUTION DEMO — Week 3")
    print(f"  Table: {HUDI_SILVER_PATH}")
    print(SEP)

    print("\n  Step 1: Current Schema")
    print("-" * 76)

    try:
        current = spark.read.format("hudi").load(HUDI_SILVER_PATH)
    except Exception as e:
        print(f"\n  ERROR: Cannot read Hudi silver table — {e}")
        print("  Run the Hudi streaming pipeline first.\n")
        spark.stop()
        return

    current_count = current.count()
    current_cols = current.columns

    print(f"  Records:    {current_count:,}")
    print(f"  Columns:    {len(current_cols)}")
    print(f"  Schema:")
    for field in current.schema.fields:
        nullable = "nullable" if field.nullable else "required"
        print(f"    {field.name:40s} {str(field.dataType):20s} {nullable}")

    has_review_score = "review_score" in current_cols
    has_delivery_delay_days = "delivery_delay_days" in current_cols

    if has_review_score and has_delivery_delay_days:
        print("\n  Schema evolution already applied (review_score and delivery_delay_days exist).")
        print("  Showing current state and exiting.\n")

        # Show sample with evolved columns
        print("  Sample records with evolved columns:")
        (
            current
            .select(
                "order_id", "order_status", "customer_state",
                "total_price", "review_score", "delivery_delay_days",
                "event_time",
            )
            .orderBy(F.col("event_time").desc())
            .limit(10)
            .show(truncate=False)
        )
        spark.stop()
        return

    print("\n  Step 2: Adding New Columns via Schema Evolution")
    print("-" * 76)
    print("  New columns:")
    print("    review_score        IntegerType   nullable   (1-5 customer rating)")
    print("    delivery_delay_days DoubleType    nullable   (actual - estimated delivery)")

    # Take a sample of existing order_ids - we'll upsert them with new columns
    sample_orders = (
        current
        .select(
            "order_id", "customer_id", "product_id", "product_category",
            "seller_id", "quantity", "unit_price", "freight_value",
            "total_price", "order_status", "customer_state", "customer_city",
            "payment_type", "payment_value",
            "event_time", "event_ts",
            "order_delivered_ts", "order_estimated_ts",
            "processing_time", "processing_ts",
            "kafka_ts", "ingestion_ts",
        )
        .filter(F.col("order_status") == "delivered")
        .limit(100)
    )
    sample_count = sample_orders.count()

    if sample_count == 0:
        print("\n  No delivered orders found for schema evolution demo.")
        print("  Run the pipeline with more data first.\n")
        spark.stop()
        return

    evolved = (
        sample_orders
        .withColumn(
            "review_score",
            (F.abs(F.hash("order_id")) % 5 + 1).cast("int")
        )
        .withColumn(
            "delivery_delay_days",
            F.when(
                F.col("order_delivered_ts").isNotNull() & F.col("order_estimated_ts").isNotNull(),
                F.round(
                    F.datediff(
                        F.to_timestamp("order_delivered_ts"),
                        F.to_timestamp("order_estimated_ts"),
                    ).cast("double"),
                    1,
                )
            )
        )
    )

    print(f"\n  Upserting {sample_count} records with evolved schema…")

    # Build Hudi write options with schema evolution enabled
    hudi_opts = dict(HUDI_COMMON_OPTS)
    hudi_opts.update({
        "hoodie.datasource.write.reconcile.schema": "true",
        "hoodie.schema.on.read.enable": "true",
    })

    (
        evolved
        .write
        .format("hudi")
        .options(**hudi_opts)
        .mode("append")
        .save(HUDI_SILVER_PATH)
    )

    print(f"  Upsert complete.")

    print("\n  Step 3: Verify Schema Evolution")
    print("-" * 76)

    evolved_table = spark.read.format("hudi").load(HUDI_SILVER_PATH)
    new_cols = evolved_table.columns
    new_count = evolved_table.count()

    print(f"  Records:    {new_count:,}  (was {current_count:,})")
    print(f"  Columns:    {len(new_cols)}  (was {len(current_cols)})")

    added_cols = set(new_cols) - set(current_cols)
    if added_cols:
        print(f"  New columns added: {sorted(added_cols)}")
    else:
        print("  No new columns detected (schema may have already been merged)")

    print("\n  Step 4: Backward Compatibility Check")
    print("-" * 76)

    # Old records should have NULL for new columns
    if "review_score" in new_cols:
        null_review = (
            evolved_table
            .filter(F.col("review_score").isNull())
            .count()
        )
        non_null_review = (
            evolved_table
            .filter(F.col("review_score").isNotNull())
            .count()
        )
        print(f"  review_score:")
        print(f"    NULL (old records): {null_review:,}")
        print(f"    Non-NULL (evolved): {non_null_review:,}")
        print(f"    Backward compat:    {'YES ✓' if null_review > 0 else 'N/A (all records evolved)'}")

    if "delivery_delay_days" in new_cols:
        null_delay = (
            evolved_table
            .filter(F.col("delivery_delay_days").isNull())
            .count()
        )
        non_null_delay = (
            evolved_table
            .filter(F.col("delivery_delay_days").isNotNull())
            .count()
        )
        print(f"  delivery_delay_days:")
        print(f"    NULL (old/no-data): {null_delay:,}")
        print(f"    Non-NULL (evolved): {non_null_delay:,}")

    print(f"\n  Sample: Evolved records (with review_score and delivery_delay_days):")
    (
        evolved_table
        .filter(F.col("review_score").isNotNull())
        .select(
            "order_id", "order_status", "customer_state",
            "total_price", "review_score", "delivery_delay_days",
            "event_time", "_hoodie_commit_time",
        )
        .orderBy(F.col("event_time").desc())
        .limit(10)
        .show(truncate=False)
    )

    print(f"  Sample: Original records (before schema evolution — NULLs expected):")
    (
        evolved_table
        .filter(F.col("review_score").isNull())
        .select(
            "order_id", "order_status", "customer_state",
            "total_price", "review_score", "delivery_delay_days",
            "event_time", "_hoodie_commit_time",
        )
        .orderBy(F.col("event_time").desc())
        .limit(10)
        .show(truncate=False)
    )

    print(f"\n  Final Schema (after evolution):")
    for field in evolved_table.schema.fields:
        nullable = "nullable" if field.nullable else "required"
        marker = "  ← NEW" if field.name in added_cols else ""
        print(f"    {field.name:40s} {str(field.dataType):20s} {nullable}{marker}")

    print("  Schema evolution demo complete.")
    print(f"  → New columns added without full pipeline rewrite or table rebuild.")
    print(f"  → Old records seamlessly return NULLs for new columns (schema-on-read).")

    spark.stop()


if __name__ == "__main__":
    main()
