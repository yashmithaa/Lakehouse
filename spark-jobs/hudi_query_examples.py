import argparse
import logging
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline_utils import (
    HUDI_SILVER_PATH, HUDI_TABLE_NAME, BRONZE_PATH,
    hudi_time_travel_opts, HUDI_INCREMENTAL_OPTS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("hudi-query-examples")

SEP = "=" * 76


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("HudiQueryExamples_Week3")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .getOrCreate()
    )


#UPSERT CORRECTNESS VERIFICATION
def verify_upserts(spark: SparkSession):
    """
    Prove that Hudi upserts produce exactly one row per record key (order_id).

    Compares:
      - Bronze (Parquet append): may have duplicate order_ids across batches
      - Silver (Hudi CoW):       guaranteed one row per order_id (latest event_time wins)
    """
    print(f"\n{SEP}")
    print("  1. UPSERT CORRECTNESS VERIFICATION")
    print(SEP)

    try:
        hudi_silver = spark.read.format("hudi").load(HUDI_SILVER_PATH)
    except Exception as e:
        print(f"\n  ERROR: Cannot read Hudi silver table at {HUDI_SILVER_PATH}")
        print(f"  {e}")
        print("  Run the Hudi streaming pipeline first.\n")
        return

    hudi_total = hudi_silver.count()
    hudi_unique = hudi_silver.select("order_id").distinct().count()
    hudi_duplicates = hudi_total - hudi_unique

    print(f"\n  Hudi Silver Table ({HUDI_SILVER_PATH})")
    print(f"    Total rows:         {hudi_total:>10,}")
    print(f"    Unique order_ids:   {hudi_unique:>10,}")
    print(f"    Duplicate rows:     {hudi_duplicates:>10,}")
    print(f"    Dedup correct:      {'YES ✓' if hudi_duplicates == 0 else 'NO ✗ — duplicates found!'}")

    try:
        bronze = spark.read.parquet(BRONZE_PATH)
        bronze_total = bronze.count()
        bronze_unique = bronze.select("order_id").distinct().count()
        bronze_duplicates = bronze_total - bronze_unique

        print(f"\n  Bronze Layer (Parquet append — {BRONZE_PATH})")
        print(f"    Total rows:         {bronze_total:>10,}")
        print(f"    Unique order_ids:   {bronze_unique:>10,}")
        print(f"    Duplicate rows:     {bronze_duplicates:>10,}")

        if bronze_duplicates > 0 and hudi_duplicates == 0:
            reduction_pct = round(bronze_duplicates / max(bronze_total, 1) * 100, 2)
            print(f"\n  → Hudi upsert eliminated {bronze_duplicates:,} duplicates "
                  f"({reduction_pct}% of bronze rows) via ACID merge semantics.")
    except Exception:
        print("\n  (Bronze data not readable — skipping comparison)")

    print(f"\n  Sample: Order with multiple status updates in bronze vs Hudi silver")
    print("-" * 76)

    try:
        multi_update_order = (
            bronze
            .groupBy("order_id")
            .agg(F.count("*").alias("cnt"))
            .filter(F.col("cnt") > 1)
            .orderBy(F.col("cnt").desc())
            .limit(1)
            .collect()
        )

        if multi_update_order:
            sample_order_id = multi_update_order[0]["order_id"]
            print(f"\n  Order: {sample_order_id}")

            print("\n  Bronze rows (all versions):")
            (
                bronze
                .filter(F.col("order_id") == sample_order_id)
                .select("order_id", "order_status", "event_time", "ingestion_ts")
                .orderBy("event_time")
                .show(truncate=False)
            )

            print("  Hudi silver row (latest version only — precombine winner):")
            (
                hudi_silver
                .filter(F.col("order_id") == sample_order_id)
                .select("order_id", "order_status", "event_time", "ingestion_ts")
                .show(truncate=False)
            )
        else:
            print("  (No multi-update orders found in bronze)")
    except Exception as e:
        print(f"  (Could not generate sample: {e})")


#TIME-TRAVEL SNAPSHOT QUERIES

def time_travel_queries(spark: SparkSession):
    """
    Demonstrate Hudi time-travel: query the silver table as it looked at
    specific points in time using the commit timeline.
    """
    print(f"\n{SEP}")
    print("  2. TIME-TRAVEL SNAPSHOT QUERIES")
    print(SEP)

    try:
        hudi_silver = spark.read.format("hudi").load(HUDI_SILVER_PATH)
    except Exception as e:
        print(f"\n  ERROR: Cannot read Hudi silver table — {e}\n")
        return

    print("\n  Hudi Commit Timeline:")
    print("-" * 76)

    timeline_df = (
        hudi_silver
        .select("_hoodie_commit_time", "_hoodie_commit_seqno")
        .withColumn("commit_ts", F.col("_hoodie_commit_time"))
        .groupBy("commit_ts")
        .agg(F.count("*").alias("records_in_commit"))
        .orderBy("commit_ts")
    )
    commits = timeline_df.collect()

    if len(commits) < 2:
        print("  Only one commit found — need at least 2 for time-travel demo.")
        print("  Run the pipeline longer and re-run this script.\n")
        timeline_df.show(truncate=False)
        return

    for i, row in enumerate(commits):
        print(f"    Commit {i + 1}: {row['commit_ts']}  ({row['records_in_commit']:,} records)")

    earliest_commit = commits[0]["commit_ts"]
    latest_commit = commits[-1]["commit_ts"]

    print(f"\n  Snapshot at EARLIEST commit ({earliest_commit}):")
    early_df = (
        spark.read
        .format("hudi")
        .option("as.of.instant", earliest_commit)
        .load(HUDI_SILVER_PATH)
    )
    early_count = early_df.count()
    print(f"    Record count: {early_count:,}")

    # Status distribution at earliest commit
    print("    Status distribution:")
    (
        early_df
        .groupBy("order_status")
        .count()
        .orderBy(F.col("count").desc())
        .show(truncate=False)
    )

    print(f"  Snapshot at LATEST commit ({latest_commit}):")
    latest_df = (
        spark.read
        .format("hudi")
        .option("as.of.instant", latest_commit)
        .load(HUDI_SILVER_PATH)
    )
    latest_count = latest_df.count()
    print(f"    Record count: {latest_count:,}")

    print("    Status distribution:")
    (
        latest_df
        .groupBy("order_status")
        .count()
        .orderBy(F.col("count").desc())
        .show(truncate=False)
    )

    delta_count = latest_count - early_count
    print(f"\n  → Table grew by {delta_count:,} records between first and last commit.")
    print(f"  → Time-travel allows auditing any historical state of the table.")


#INCREMENTAL READ BETWEEN COMMITS
def incremental_reads(spark: SparkSession):
    """
    Demonstrate Hudi incremental reads: pull only new/updated records
    since a given commit instant, avoiding a full table scan.
    """
    print(f"\n{SEP}")
    print("  3. INCREMENTAL READS BETWEEN COMMITS")
    print(SEP)

    try:
        hudi_silver = spark.read.format("hudi").load(HUDI_SILVER_PATH)
    except Exception as e:
        print(f"\n  ERROR: Cannot read Hudi silver table — {e}\n")
        return

    # Get commit timeline
    commits = (
        hudi_silver
        .select("_hoodie_commit_time")
        .distinct()
        .orderBy("_hoodie_commit_time")
        .collect()
    )

    if len(commits) < 2:
        print("  Need at least 2 commits for incremental read demo.")
        print(f"  Found {len(commits)} commit(s). Run the pipeline longer.\n")
        return

    # Use the first commit as the baseline
    begin_instant = commits[0]["_hoodie_commit_time"]
    end_instant = commits[-1]["_hoodie_commit_time"]

    print(f"\n  Reading changes between:")
    print(f"    Begin: {begin_instant}")
    print(f"    End:   {end_instant}")
    print("-" * 76)

    import time

    full_start = time.time()
    full_df = spark.read.format("hudi").load(HUDI_SILVER_PATH)
    full_count = full_df.count()
    full_duration = time.time() - full_start

    print(f"\n  Full snapshot read:")
    print(f"    Records:  {full_count:,}")
    print(f"    Duration: {full_duration:.2f}s")

    incr_start = time.time()
    incremental_df = (
        spark.read
        .format("hudi")
        .option("hoodie.datasource.query.type", "incremental")
        .option("hoodie.datasource.read.begin.instanttime", begin_instant)
        .load(HUDI_SILVER_PATH)
    )
    incr_count = incremental_df.count()
    incr_duration = time.time() - incr_start

    print(f"\n  Incremental read (changes after {begin_instant}):")
    print(f"    Records:  {incr_count:,}")
    print(f"    Duration: {incr_duration:.2f}s")

    #Speedup comparison
    if full_duration > 0:
        speedup = full_duration / max(incr_duration, 0.001)
        pct_reduction = round((1 - incr_count / max(full_count, 1)) * 100, 1)
        print(f"\n  → Incremental read processed {pct_reduction}% fewer records.")
        print(f"  → Speedup: {speedup:.1f}x vs full table scan.")
        print(f"  → This enables efficient CDC pipelines for downstream consumers.")

    #Sample of incremental records
    print(f"\n  Sample of incrementally read records:")
    (
        incremental_df
        .select(
            "_hoodie_commit_time",
            "order_id", "order_status", "customer_state",
            "total_price", "event_time",
        )
        .orderBy(F.col("_hoodie_commit_time").desc())
        .limit(10)
        .show(truncate=False)
    )

    #Demonstrate per-commit incremental pulls
    if len(commits) >= 3:
        print("  Per-Commit Incremental Pulls:")
        print("-" * 76)
        for i in range(len(commits) - 1):
            c_begin = commits[i]["_hoodie_commit_time"]
            c_end = commits[i + 1]["_hoodie_commit_time"]
            chunk = (
                spark.read
                .format("hudi")
                .option("hoodie.datasource.query.type", "incremental")
                .option("hoodie.datasource.read.begin.instanttime", c_begin)
                .option("hoodie.datasource.read.end.instanttime", c_end)
                .load(HUDI_SILVER_PATH)
            )
            chunk_count = chunk.count()
            print(f"    {c_begin} → {c_end}: {chunk_count:,} records")


def main():
    parser = argparse.ArgumentParser(description="Hudi Query Examples — Week 3")
    parser.add_argument(
        "--mode", default="all",
        choices=["all", "upsert", "timetravel", "incremental"],
        help="Which demo to run (default: all)",
    )
    args = parser.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{SEP}")
    print("  HUDI QUERY EXAMPLES — Week 3")
    print(f"  Table: {HUDI_SILVER_PATH}")

    if args.mode in ("all", "upsert"):
        verify_upserts(spark)

    if args.mode in ("all", "timetravel"):
        time_travel_queries(spark)

    if args.mode in ("all", "incremental"):
        incremental_reads(spark)

    print("  Query examples complete.")

    spark.stop()


if __name__ == "__main__":
    main()
