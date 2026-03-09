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

# Checkpoint paths (separate bucket for clean lifecycle) 
BRONZE_CHECKPOINT     = "s3a://lakehouse-checkpoints/bronze/orders/"
SILVER_CHECKPOINT     = "s3a://lakehouse-checkpoints/silver/orders/"
QUARANTINE_CHECKPOINT = "s3a://lakehouse-checkpoints/silver/quarantine/"
PIPELINE_CHECKPOINT   = "s3a://lakehouse-checkpoints/pipeline/"


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
