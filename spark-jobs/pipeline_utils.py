from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
)

# Event schema 
# Matches the Olist replay producer / data contract.
# All fields nullable=True so from_json returns NULLs instead of dropping
# entire records on malformed payloads; QC checks enforce required fields.

ORDER_SCHEMA = StructType([
    StructField("order_id",           StringType(),  nullable=True),
    StructField("customer_id",        StringType(),  nullable=True),
    StructField("product_id",         StringType(),  nullable=True),
    StructField("product_category",   StringType(),  nullable=True),
    StructField("seller_id",          StringType(),  nullable=True),
    StructField("quantity",           IntegerType(), nullable=True),
    StructField("unit_price",         DoubleType(),  nullable=True),
    StructField("freight_value",      DoubleType(),  nullable=True),
    StructField("total_price",        DoubleType(),  nullable=True),
    StructField("order_status",       StringType(),  nullable=True),
    StructField("customer_state",     StringType(),  nullable=True),
    StructField("customer_city",      StringType(),  nullable=True),
    StructField("payment_type",       StringType(),  nullable=True),
    StructField("payment_value",      DoubleType(),  nullable=True),
    StructField("event_time",         StringType(),  nullable=True),
    StructField("order_delivered_ts", StringType(),  nullable=True),
    StructField("order_estimated_ts", StringType(),  nullable=True),
    StructField("processing_time",    StringType(),  nullable=True),
])

# Quality-check reference values 
VALID_STATUSES = ["created", "confirmed", "shipped", "delivered", "cancelled"]

VALID_STATES = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO",
    "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR",
    "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
]

# Kafka configuration 
KAFKA_BOOTSTRAP = "kafka:29092"
KAFKA_TOPIC     = "orders"

# S3 / MinIO storage paths 
BRONZE_PATH      = "s3a://lakehouse/bronze/orders/"
SILVER_PATH      = "s3a://lakehouse/silver/orders/"
QUARANTINE_PATH  = "s3a://lakehouse/bronze/quarantine/"
METRICS_PATH     = "s3a://lakehouse/metrics/quality/"

HUDI_SILVER_PATH = "s3a://lakehouse/hudi/silver/orders/"

GOLD_PATH                = "s3a://lakehouse/gold/"
GOLD_DAILY_REVENUE_PATH  = "s3a://lakehouse/gold/daily_revenue/"
GOLD_STATE_METRICS_PATH  = "s3a://lakehouse/gold/state_metrics/"
GOLD_STATUS_FUNNEL_PATH  = "s3a://lakehouse/gold/status_funnel/"
GOLD_CATEGORY_PATH       = "s3a://lakehouse/gold/category_performance/"

# Checkpoint paths (separate bucket for clean lifecycle) 
BRONZE_CHECKPOINT     = "s3a://lakehouse-checkpoints/bronze/orders/"
SILVER_CHECKPOINT     = "s3a://lakehouse-checkpoints/silver/orders/"
QUARANTINE_CHECKPOINT = "s3a://lakehouse-checkpoints/silver/quarantine/"
PIPELINE_CHECKPOINT   = "s3a://lakehouse-checkpoints/pipeline/"
HUDI_PIPELINE_CHECKPOINT = "s3a://lakehouse-checkpoints/hudi-pipeline/"

# Matches the data contract: order_id as record key, event_time as
# precombine field, customer_state as partition path.
HUDI_TABLE_NAME = "orders_silver"
HUDI_DATABASE   = "lakehouse_db"

HUDI_COMMON_OPTS = {
    "hoodie.table.name":                          HUDI_TABLE_NAME,
    "hoodie.database.name":                       HUDI_DATABASE,

    "hoodie.datasource.write.recordkey.field":    "order_id",
    "hoodie.datasource.write.precombine.field":   "event_time",
    "hoodie.datasource.write.partitionpath.field": "customer_state",

    "hoodie.datasource.write.operation":          "upsert",
    "hoodie.datasource.write.table.type":         "COPY_ON_WRITE",

    "hoodie.index.type":                          "BLOOM",
    "hoodie.bloom.index.update.partition.path":   "true",

    "hoodie.upsert.shuffle.parallelism":          "4",
    "hoodie.insert.shuffle.parallelism":          "4",

    "hoodie.parquet.max.file.size":               str(128 * 1024 * 1024),  # 128 MB
    "hoodie.parquet.small.file.limit":            str(64 * 1024 * 1024),   # 64 MB

    # Inline compaction (CoW doesn't need MOR log compaction but
    # clustering can be triggered manually via maintenance jobs)
    "hoodie.compact.inline":                      "false",

    # Cleaner — keep 3 prior commits for time-travel queries
    "hoodie.cleaner.policy":                      "KEEP_LATEST_COMMITS",
    "hoodie.cleaner.commits.retained":            "10",

    "hoodie.keep.min.commits":                    "15",
    "hoodie.keep.max.commits":                    "20",

    "hoodie.datasource.write.reconcile.schema":   "true",
    "hoodie.schema.on.read.enable":               "true",

    "hoodie.datasource.hive_sync.enable":         "true",
    "hoodie.datasource.hive_sync.mode":           "hms",
    "hoodie.datasource.hive_sync.database":       "lakehouse_db",
    "hoodie.datasource.hive_sync.table":          "orders_silver",
    "hoodie.datasource.hive_sync.partition_fields": "customer_state",
    "hoodie.datasource.hive_sync.partition_extractor_class":
        "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.metastore.uris": "thrift://hive-metastore:9083",
}

HUDI_INCREMENTAL_OPTS = {
    "hoodie.datasource.query.type": "incremental",
}

def hudi_time_travel_opts(instant_time: str) -> dict:
    """Return Hudi read options for a point-in-time snapshot query."""
    return {
        "hoodie.datasource.query.type":          "snapshot",
        "as.of.instant":                         instant_time,
    }


def parse_kafka_stream(raw_stream: DataFrame) -> DataFrame:
    """
    Parse a raw Kafka DataFrame into typed event columns.

    Keeps the original JSON string (json_str) for auditing in bronze /
    quarantine.  Adds event_ts and processing_ts as proper TimestampType
    columns for downstream analytics.
    """
    return (
        raw_stream
        .selectExpr(
            "CAST(key   AS STRING) AS msg_key",
            "CAST(value AS STRING) AS json_str",
            "topic",
            "partition AS kafka_partition",
            "offset    AS kafka_offset",
            "timestamp AS kafka_ts",
        )
        .withColumn("data", F.from_json(F.col("json_str"), ORDER_SCHEMA))
        .select(
            "msg_key", "json_str",
            "kafka_ts", "topic", "kafka_partition", "kafka_offset",
            "data.*",
        )
        .withColumn(
            "event_ts",
            F.to_timestamp("event_time", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        )
        .withColumn(
            "processing_ts",
            F.to_timestamp("processing_time", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
        )
    )


def apply_quality_checks(df: DataFrame) -> DataFrame:
    """
    Append six boolean QC columns plus an aggregate ``is_valid`` flag.

    Rules follow the data contract:
      qc_has_order_id    — order_id IS NOT NULL
      qc_valid_status    — order_status IN (created, confirmed, …)
      qc_valid_state     — customer_state IN (SP, RJ, MG, …)
      qc_positive_qty    — quantity > 0
      qc_positive_price  — total_price > 0
      qc_has_event_time  — event_ts parses to a valid timestamp
    """
    return (
        df
        .withColumn("qc_has_order_id",   F.col("order_id").isNotNull())
        .withColumn("qc_valid_status",   F.col("order_status").isin(VALID_STATUSES))
        .withColumn("qc_valid_state",    F.col("customer_state").isin(VALID_STATES))
        .withColumn("qc_positive_qty",   F.col("quantity") > 0)
        .withColumn("qc_positive_price", F.col("total_price") > 0)
        .withColumn("qc_has_event_time", F.col("event_ts").isNotNull())
        .withColumn(
            "is_valid",
            (
                F.col("qc_has_order_id")
                & F.col("qc_valid_status")
                & F.col("qc_valid_state")
                & F.col("qc_positive_qty")
                & F.col("qc_positive_price")
                & F.col("qc_has_event_time")
            ),
        )
    )
