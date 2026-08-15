"""
Usage:
    spark-submit ... hudi_maintenance.py --operation all
    spark-submit ... hudi_maintenance.py --operation clean
    spark-submit ... hudi_maintenance.py --operation cluster
    spark-submit ... hudi_maintenance.py --operation stats
"""

import argparse
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline_utils import (
    HUDI_SILVER_PATH, HUDI_TABLE_NAME, HUDI_DATABASE,
    GOLD_DAILY_REVENUE_PATH, GOLD_STATE_METRICS_PATH,
    GOLD_STATUS_FUNNEL_PATH, GOLD_CATEGORY_PATH,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("hudi-maintenance")

# All Hudi table paths to maintain
TABLES = {
    "orders_silver": HUDI_SILVER_PATH,
    "gold_daily_revenue": GOLD_DAILY_REVENUE_PATH,
    "gold_state_metrics": GOLD_STATE_METRICS_PATH,
    "gold_status_funnel": GOLD_STATUS_FUNNEL_PATH,
    "gold_category_performance": GOLD_CATEGORY_PATH,
}


def show_table_stats(spark: SparkSession, table_name: str, path: str):

    print(f"  Table: {table_name}")
    print(f"  Path:  {path}")

    try:
        df = spark.read.format("hudi").load(path)
        total_rows = df.count()
        partitions = (
            df.select("_hoodie_partition_path")
            .distinct()
            .count()
        )
        commits = (
            df.select("_hoodie_commit_time")
            .distinct()
            .count()
        )
        latest_commit = (
            df.select(F.max("_hoodie_commit_time").alias("latest"))
            .collect()[0]["latest"]
        )

        print(f"    Total rows:      {total_rows:>10,}")
        print(f"    Partitions:      {partitions:>10}")
        print(f"    Commits:         {commits:>10}")
        print(f"    Latest commit:   {latest_commit}")

        # File-level stats via Hudi metadata
        file_stats = (
            df.select("_hoodie_file_name")
            .distinct()
            .count()
        )
        print(f"    Data files:      {file_stats:>10}")

        # Per-partition breakdown
        print(f"\n    {'Partition':<30} {'Rows':>10}")
        print(f"    {'─' * 42}")
        partition_counts = (
            df.groupBy("_hoodie_partition_path")
            .count()
            .orderBy(F.col("count").desc())
            .collect()
        )
        for row in partition_counts[:15]:
            ppath = row["_hoodie_partition_path"] or "(non-partitioned)"
            print(f"    {ppath:<30} {row['count']:>10,}")
        if len(partition_counts) > 15:
            print(f"    ... and {len(partition_counts) - 15} more partitions")

    except Exception as e:
        print(f"    (table not available: {e})")


def run_cleaning(spark: SparkSession, table_name: str, path: str):
    """Run Hudi cleaner to remove old file versions."""
    log.info("Running cleaner on %s", table_name)
    try:
        spark.sql(f"""
            CALL run_clean(
                table => '{HUDI_DATABASE}.{table_name}',
                retain_commits => 5
            )
        """)
        log.info("  Cleaning complete for %s", table_name)
    except Exception:
        # Fall back to programmatic cleaning via Hudi write
        log.info("  SQL clean not available; using write-based clean for %s", table_name)
        try:
            df = spark.read.format("hudi").load(path).limit(0)
            (
                df.write
                .format("hudi")
                .option("hoodie.table.name", table_name)
                .option("hoodie.database.name", HUDI_DATABASE)
                .option("hoodie.datasource.write.operation", "upsert")
                .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
                .option("hoodie.clean.automatic", "true")
                .option("hoodie.cleaner.policy", "KEEP_LATEST_COMMITS")
                .option("hoodie.cleaner.commits.retained", "5")
                .mode("append")
                .save(path)
            )
            log.info("  Write-based clean triggered for %s", table_name)
        except Exception as e:
            log.warning("  Could not clean %s: %s", table_name, e)


def run_clustering(spark: SparkSession, table_name: str, path: str):
    """Run inline clustering to optimize file sizes and layout."""
    log.info("Running clustering on %s", table_name)
    try:
        # Use Hudi's call command for clustering
        spark.sql(f"""
            CALL run_clustering(
                table => '{HUDI_DATABASE}.{table_name}',
                order => 'event_ts'
            )
        """)
        log.info("  Clustering complete for %s", table_name)
    except Exception:
        log.info("  SQL clustering not available; using write-based clustering for %s",
                 table_name)
        try:
            df = spark.read.format("hudi").load(path).limit(0)
            (
                df.write
                .format("hudi")
                .option("hoodie.table.name", table_name)
                .option("hoodie.database.name", HUDI_DATABASE)
                .option("hoodie.datasource.write.operation", "upsert")
                .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
                .option("hoodie.clustering.inline", "true")
                .option("hoodie.clustering.inline.max.commits", "1")
                .option("hoodie.clustering.plan.strategy.sort.columns", "event_ts")
                .option("hoodie.clustering.plan.strategy.target.file.max.bytes",
                        str(128 * 1024 * 1024))
                .option("hoodie.clustering.plan.strategy.small.file.limit",
                        str(64 * 1024 * 1024))
                .mode("append")
                .save(path)
            )
            log.info("  Write-based clustering triggered for %s", table_name)
        except Exception as e:
            log.warning("  Could not cluster %s: %s", table_name, e)


def run_compaction(spark: SparkSession, table_name: str, path: str):
    """Run compaction (relevant for MoR tables; no-op for CoW)."""
    log.info("Compaction check for %s", table_name)
    try:
        # Read the table type to determine if compaction applies
        df = spark.read.format("hudi").load(path)
        meta = df.select("_hoodie_commit_time").limit(1).collect()
        if meta:
            log.info("  Table %s is CoW — compaction is automatic on write.", table_name)
            log.info("  (MoR tables would require explicit compaction scheduling.)")
        else:
            log.info("  Table %s is empty — skipping compaction.", table_name)
    except Exception as e:
        log.warning("  Could not check %s: %s", table_name, e)


def main():
    parser = argparse.ArgumentParser(description="Hudi Maintenance Jobs")
    parser.add_argument(
        "--operation", default="all",
        choices=["all", "stats", "clean", "cluster", "compact"],
        help="Maintenance operation to run (default: all)",
    )
    parser.add_argument(
        "--table", default="all",
        help="Specific table to maintain (default: all tables)",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("HudiMaintenance_Week4")
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
    sep = "=" * 72

    print(f"\n{sep}")
    print("  HUDI MAINTENANCE JOBS — Week 4")
    print(f"  Operation: {args.operation}")
    print(f"  Table:     {args.table}")
    print(f"  Started:   {start.isoformat()}")
    print(sep)

    # Select tables to maintain
    if args.table == "all":
        targets = TABLES
    elif args.table in TABLES:
        targets = {args.table: TABLES[args.table]}
    else:
        log.error("Unknown table: %s. Choose from: %s",
                  args.table, ", ".join(TABLES.keys()))
        spark.stop()
        return

    for table_name, path in targets.items():
        if args.operation in ("all", "stats"):
            show_table_stats(spark, table_name, path)

        if args.operation in ("all", "compact"):
            run_compaction(spark, table_name, path)

        if args.operation in ("all", "clean"):
            run_cleaning(spark, table_name, path)

        if args.operation in ("all", "cluster"):
            run_clustering(spark, table_name, path)

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()

    print(f"\n{sep}")
    print(f"  Maintenance complete.")
    print(f"  Duration: {elapsed:.1f}s")
    print(sep)

    # Show Hive table list for verification
    try:
        spark.sql(f"USE {HUDI_DATABASE}")
        print(f"\n  Tables in {HUDI_DATABASE}:")
        spark.sql("SHOW TABLES").show(truncate=False)
    except Exception:
        pass

    spark.stop()


if __name__ == "__main__":
    main()
