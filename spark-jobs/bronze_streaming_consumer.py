"""
Bronze Streaming Consumer — Kafka → Console (Week 1)
=====================================================
Reads order events from Kafka (replayed from the Brazilian E-Commerce / Olist
dataset), parses JSON, applies basic quality checks, and prints validated +
quarantined records to the console.

Submit:
    /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
        /opt/spark-jobs/bronze_streaming_consumer.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType,
)

# ── Schema matching the Olist replay event contract ─────────────────────────
ORDER_SCHEMA = StructType([
    StructField("order_id",           StringType(),  nullable=False),
    StructField("customer_id",        StringType(),  nullable=False),
    StructField("product_id",         StringType(),  nullable=False),
    StructField("product_category",   StringType(),  nullable=True),
    StructField("seller_id",          StringType(),  nullable=True),
    StructField("quantity",           IntegerType(), nullable=False),
    StructField("unit_price",         DoubleType(),  nullable=False),
    StructField("freight_value",      DoubleType(),  nullable=True),
    StructField("total_price",        DoubleType(),  nullable=False),
    StructField("order_status",       StringType(),  nullable=False),
    StructField("customer_state",     StringType(),  nullable=False),
    StructField("customer_city",      StringType(),  nullable=True),
    StructField("payment_type",       StringType(),  nullable=True),
    StructField("payment_value",      DoubleType(),  nullable=True),
    StructField("event_time",         StringType(),  nullable=False),
    StructField("order_delivered_ts", StringType(),  nullable=True),
    StructField("order_estimated_ts", StringType(),  nullable=True),
    StructField("processing_time",    StringType(),  nullable=True),
])

# ── Quality-check reference values ──────────────────────────────────────────
VALID_STATUSES = ["created", "confirmed", "shipped", "delivered", "cancelled"]

VALID_STATES = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO",
    "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR",
    "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
]


def main():
    spark = (
        SparkSession.builder
        .appName("LakehouseBronzeConsumer")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── Read from Kafka ──────────────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "orders")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse JSON payload ───────────────────────────────────────────────────
    parsed = (
        raw_stream
        .selectExpr(
            "CAST(key AS STRING) AS msg_key",
            "CAST(value AS STRING) AS json_str",
            "topic", "partition", "offset",
            "timestamp AS kafka_ts",
        )
        .withColumn("data", F.from_json(F.col("json_str"), ORDER_SCHEMA))
        .select("msg_key", "kafka_ts", "topic", "partition", "offset", "data.*")
    )

    # ── Cast event_time to timestamp ─────────────────────────────────────────
    parsed = parsed.withColumn(
        "event_ts",
        F.to_timestamp("event_time", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
    )

    # ── Quality checks ───────────────────────────────────────────────────────
    quality_checked = (
        parsed
        .withColumn("qc_has_order_id",      F.col("order_id").isNotNull())
        .withColumn("qc_valid_status",      F.col("order_status").isin(VALID_STATUSES))
        .withColumn("qc_valid_state",       F.col("customer_state").isin(VALID_STATES))
        .withColumn("qc_positive_qty",      F.col("quantity") > 0)
        .withColumn("qc_positive_price",    F.col("total_price") > 0)
        .withColumn("qc_has_event_time",    F.col("event_ts").isNotNull())
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

    # ── Separate valid and quarantined records ───────────────────────────────
    valid_records = quality_checked.filter(F.col("is_valid") == True)
    quarantine_records = quality_checked.filter(F.col("is_valid") == False)

    # ── Write valid records to console ───────────────────────────────────────
    valid_query = (
        valid_records
        .select(
            "order_id", "customer_id", "product_id", "product_category",
            "quantity", "total_price", "order_status", "customer_state",
            "payment_type", "event_ts",
        )
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 20)
        .trigger(processingTime="10 seconds")
        .queryName("valid-orders")
        .start()
    )

    # ── Write quarantined records to console ─────────────────────────────────
    quarantine_query = (
        quarantine_records
        .select(
            "order_id", "json_str",
            "qc_has_order_id", "qc_valid_status", "qc_valid_state",
            "qc_positive_qty", "qc_positive_price", "qc_has_event_time",
        )
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 10)
        .trigger(processingTime="10 seconds")
        .queryName("quarantine-orders")
        .start()
    )

    # ── Await termination ────────────────────────────────────────────────────
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
