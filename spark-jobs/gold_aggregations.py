import argparse
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from pipeline_utils import (
    HUDI_SILVER_PATH, HUDI_DATABASE,
    GOLD_DAILY_REVENUE_PATH, GOLD_STATE_METRICS_PATH,
    GOLD_STATUS_FUNNEL_PATH, GOLD_CATEGORY_PATH,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("gold-aggregations")


def _gold_hudi_opts(table_name: str, record_key: str,
                    partition_field: str = "") -> dict:
    opts = {
        "hoodie.table.name":                          table_name,
        "hoodie.database.name":                       HUDI_DATABASE,
        "hoodie.datasource.write.recordkey.field":    record_key,
        "hoodie.datasource.write.precombine.field":   "agg_updated_ts",
        "hoodie.datasource.write.operation":          "upsert",
        "hoodie.datasource.write.table.type":         "COPY_ON_WRITE",
        "hoodie.index.type":                          "BLOOM",
        "hoodie.upsert.shuffle.parallelism":          "2",
        "hoodie.insert.shuffle.parallelism":          "2",
        "hoodie.parquet.max.file.size":               str(64 * 1024 * 1024),
        "hoodie.parquet.small.file.limit":            str(32 * 1024 * 1024),
        "hoodie.compact.inline":                      "false",
        "hoodie.cleaner.policy":                      "KEEP_LATEST_COMMITS",
        "hoodie.cleaner.commits.retained":            "5",
        "hoodie.keep.min.commits":                    "8",
        "hoodie.keep.max.commits":                    "12",
        "hoodie.datasource.write.reconcile.schema":   "true",
        # Hive Metastore sync
        "hoodie.datasource.hive_sync.enable":         "true",
        "hoodie.datasource.hive_sync.mode":           "hms",
        "hoodie.datasource.hive_sync.database":       HUDI_DATABASE,
        "hoodie.datasource.hive_sync.table":          table_name,
        "hoodie.datasource.hive_sync.metastore.uris": "thrift://hive-metastore:9083",
    }
    if partition_field:
        opts["hoodie.datasource.write.partitionpath.field"] = partition_field
        opts["hoodie.datasource.hive_sync.partition_fields"] = partition_field
        opts["hoodie.datasource.hive_sync.partition_extractor_class"] = \
            "org.apache.hudi.hive.MultiPartKeysValueExtractor"
    else:
        opts["hoodie.datasource.write.partitionpath.field"] = ""
        opts["hoodie.datasource.hive_sync.partition_fields"] = ""
        opts["hoodie.datasource.write.keygenerator.class"] = \
            "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
        opts["hoodie.datasource.hive_sync.partition_extractor_class"] = \
            "org.apache.hudi.hive.NonPartitionedExtractor"
    return opts


def build_daily_revenue(silver: DataFrame) -> DataFrame:
    return (
        silver
        .withColumn("order_date", F.to_date("event_ts"))
        .groupBy("order_date")
        .agg(
            F.sum("total_price").alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
            F.avg("total_price").alias("avg_order_value"),
            F.sum("freight_value").alias("total_freight"),
            F.avg("freight_value").alias("avg_freight"),
        )
        .withColumn("revenue_per_order",
                    F.round(F.col("total_revenue") / F.col("order_count"), 2))
        .withColumn("agg_updated_ts", F.current_timestamp())
        # Composite key for upsert: one row per date
        .withColumn("date_key", F.date_format("order_date", "yyyyMMdd"))
    )


def build_state_metrics(silver: DataFrame) -> DataFrame:
    return (
        silver
        .withColumn(
            "delivery_delay_hrs",
            F.when(
                F.col("order_delivered_ts").isNotNull()
                & F.col("order_estimated_ts").isNotNull(),
                (
                    F.unix_timestamp(F.col("order_delivered_ts"))
                    - F.unix_timestamp(F.col("order_estimated_ts"))
                ) / 3600.0,
            ),
        )
        .groupBy("customer_state")
        .agg(
            F.sum("total_price").alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
            F.avg("total_price").alias("avg_order_value"),
            F.avg("delivery_delay_hrs").alias("avg_delivery_delay_hrs"),
            F.count(
                F.when(F.col("delivery_delay_hrs") > 0, 1)
            ).alias("late_deliveries"),
            F.count("*").alias("total_records"),
        )
        .withColumn("late_delivery_pct",
                    F.round(F.col("late_deliveries") / F.col("total_records") * 100, 2))
        .withColumn("agg_updated_ts", F.current_timestamp())
    )


def build_status_funnel(silver: DataFrame) -> DataFrame:
    return (
        silver
        .groupBy("order_status")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("total_price").alias("total_revenue"),
            F.avg("total_price").alias("avg_order_value"),
        )
        .withColumn("agg_updated_ts", F.current_timestamp())
    )


def build_category_performance(silver: DataFrame) -> DataFrame:
    return (
        silver
        .filter(F.col("product_category").isNotNull())
        .groupBy("product_category")
        .agg(
            F.sum("total_price").alias("total_revenue"),
            F.sum("quantity").alias("total_qty"),
            F.countDistinct("order_id").alias("order_count"),
            F.avg("unit_price").alias("avg_unit_price"),
            F.avg("total_price").alias("avg_order_value"),
        )
        .withColumn("revenue_rank",
                    F.dense_rank().over(
                        F.Window.orderBy(F.col("total_revenue").desc())))
        .withColumn("agg_updated_ts", F.current_timestamp())
    )


def write_gold_table(df: DataFrame, path: str, opts: dict, label: str):
    log.info("Writing Gold table: %s → %s", label, path)
    row_count = df.count()
    (
        df.write
        .format("hudi")
        .options(**opts)
        .mode("overwrite")
        .save(path)
    )
    log.info("  %s: %d rows written.", label, row_count)


def main():
    parser = argparse.ArgumentParser(description="Gold Layer Aggregation Job")
    parser.add_argument(
        "--mode", default="all",
        choices=["all", "daily", "state", "status", "category"],
        help="Which Gold table(s) to build (default: all)",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("GoldAggregations_Week4")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions",
                "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    start = datetime.now(timezone.utc)

    print("  GOLD LAYER AGGREGATION — Week 4")
    print(f"  Mode: {args.mode}")
    print(f"  Started: {start.isoformat()}")

    # Read Hudi Silver table
    log.info("Reading Hudi Silver table: %s", HUDI_SILVER_PATH)
    try:
        silver = spark.read.format("hudi").load(HUDI_SILVER_PATH)
        silver.cache()
        silver_count = silver.count()
        log.info("  Silver table: %d records", silver_count)
    except Exception as e:
        log.error("Cannot read Hudi Silver table: %s", e)
        log.error("Make sure the Hudi streaming pipeline has run first.")
        spark.stop()
        return

    if silver_count == 0:
        log.warning("Silver table is empty. Nothing to aggregate.")
        spark.stop()
        return

    # Build and write requested Gold tables
    tables_built = 0

    if args.mode in ("all", "daily"):
        df = build_daily_revenue(silver)
        write_gold_table(
            df, GOLD_DAILY_REVENUE_PATH,
            _gold_hudi_opts("gold_daily_revenue", "date_key"),
            "daily_revenue",
        )
        tables_built += 1

    if args.mode in ("all", "state"):
        df = build_state_metrics(silver)
        write_gold_table(
            df, GOLD_STATE_METRICS_PATH,
            _gold_hudi_opts("gold_state_metrics", "customer_state"),
            "state_metrics",
        )
        tables_built += 1

    if args.mode in ("all", "status"):
        df = build_status_funnel(silver)
        write_gold_table(
            df, GOLD_STATUS_FUNNEL_PATH,
            _gold_hudi_opts("gold_status_funnel", "order_status"),
            "status_funnel",
        )
        tables_built += 1

    if args.mode in ("all", "category"):
        df = build_category_performance(silver)
        write_gold_table(
            df, GOLD_CATEGORY_PATH,
            _gold_hudi_opts("gold_category_performance", "product_category"),
            "category_performance",
        )
        tables_built += 1

    silver.unpersist()

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()

    print(f"  Gold tables built: {tables_built}")
    print(f"  Duration: {elapsed:.1f}s")
    print(f"  Tables synced to Hive Metastore (database: {HUDI_DATABASE})")

    try:
        log.info("Verifying Hive tables in database: %s", HUDI_DATABASE)
        spark.sql(f"USE {HUDI_DATABASE}")
        spark.sql("SHOW TABLES").show(truncate=False)
    except Exception as e:
        log.warning("Could not list Hive tables: %s", e)

    spark.stop()


if __name__ == "__main__":
    main()
