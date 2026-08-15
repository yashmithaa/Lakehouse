import argparse
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("analytics-queries")

DATABASE = "lakehouse_db"

QUERIES = {
    "daily_revenue_trend": {
        "title": "Daily Revenue Trend with 7-Day Moving Average",
        "description": "Revenue, order count, and rolling 7-day average by date.",
        "spark_sql": f"""
            SELECT
                order_date,
                ROUND(total_revenue, 2)          AS revenue,
                order_count,
                ROUND(avg_order_value, 2)        AS aov,
                ROUND(AVG(total_revenue) OVER (
                    ORDER BY order_date
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ), 2) AS revenue_7d_ma
            FROM {DATABASE}.gold_daily_revenue
            ORDER BY order_date
        """,
        "trino_sql": f"""
            SELECT
                order_date,
                ROUND(total_revenue, 2)          AS revenue,
                order_count,
                ROUND(avg_order_value, 2)        AS aov,
                ROUND(AVG(total_revenue) OVER (
                    ORDER BY order_date
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ), 2) AS revenue_7d_ma
            FROM hive.{DATABASE}.gold_daily_revenue
            ORDER BY order_date;
        """,
    },

    "top_states_revenue": {
        "title": "Top 10 States by Revenue & Late Delivery Rate",
        "description": "Revenue ranking with delivery performance per state.",
        "spark_sql": f"""
            SELECT
                customer_state,
                ROUND(total_revenue, 2)             AS revenue,
                order_count,
                ROUND(avg_order_value, 2)           AS aov,
                ROUND(avg_delivery_delay_hrs, 1)    AS avg_delay_hrs,
                late_deliveries,
                ROUND(late_delivery_pct, 1)         AS late_pct
            FROM {DATABASE}.gold_state_metrics
            ORDER BY total_revenue DESC
            LIMIT 10
        """,
        "trino_sql": f"""
            SELECT
                customer_state,
                ROUND(total_revenue, 2)             AS revenue,
                order_count,
                ROUND(avg_order_value, 2)           AS aov,
                ROUND(avg_delivery_delay_hrs, 1)    AS avg_delay_hrs,
                late_deliveries,
                ROUND(late_delivery_pct, 1)         AS late_pct
            FROM hive.{DATABASE}.gold_state_metrics
            ORDER BY total_revenue DESC
            LIMIT 10;
        """,
    },

    "status_funnel": {
        "title": "Order Status Conversion Funnel",
        "description": "Order counts and revenue by status for funnel analysis.",
        "spark_sql": f"""
            SELECT
                order_status,
                order_count,
                ROUND(total_revenue, 2)        AS revenue,
                ROUND(avg_order_value, 2)      AS aov,
                ROUND(order_count * 100.0 / SUM(order_count) OVER (), 1)
                    AS pct_of_total
            FROM {DATABASE}.gold_status_funnel
            ORDER BY order_count DESC
        """,
        "trino_sql": f"""
            SELECT
                order_status,
                order_count,
                ROUND(total_revenue, 2)        AS revenue,
                ROUND(avg_order_value, 2)      AS aov,
                ROUND(order_count * 100.0 / SUM(order_count) OVER (), 1)
                    AS pct_of_total
            FROM hive.{DATABASE}.gold_status_funnel
            ORDER BY order_count DESC;
        """,
    },

    "top_categories": {
        "title": "Top 20 Product Categories by Revenue",
        "description": "Best-selling product categories with quantity and avg price.",
        "spark_sql": f"""
            SELECT
                product_category,
                revenue_rank,
                ROUND(total_revenue, 2)    AS revenue,
                total_qty,
                order_count,
                ROUND(avg_unit_price, 2)   AS avg_price
            FROM {DATABASE}.gold_category_performance
            WHERE revenue_rank <= 20
            ORDER BY revenue_rank
        """,
        "trino_sql": f"""
            SELECT
                product_category,
                revenue_rank,
                ROUND(total_revenue, 2)    AS revenue,
                total_qty,
                order_count,
                ROUND(avg_unit_price, 2)   AS avg_price
            FROM hive.{DATABASE}.gold_category_performance
            WHERE revenue_rank <= 20
            ORDER BY revenue_rank;
        """,
    },

    "payment_method_breakdown": {
        "title": "Revenue by Payment Method",
        "description": "Total revenue and order count by payment type.",
        "spark_sql": f"""
            SELECT
                payment_type,
                COUNT(DISTINCT order_id)      AS order_count,
                ROUND(SUM(total_price), 2)    AS total_revenue,
                ROUND(AVG(total_price), 2)    AS avg_order_value,
                ROUND(SUM(total_price) * 100.0 / (
                    SELECT SUM(total_price) FROM {DATABASE}.orders_silver
                ), 1) AS revenue_pct
            FROM {DATABASE}.orders_silver
            GROUP BY payment_type
            ORDER BY total_revenue DESC
        """,
        "trino_sql": f"""
            SELECT
                payment_type,
                COUNT(DISTINCT order_id)      AS order_count,
                ROUND(SUM(total_price), 2)    AS total_revenue,
                ROUND(AVG(total_price), 2)    AS avg_order_value
            FROM hive.{DATABASE}.orders_silver
            GROUP BY payment_type
            ORDER BY total_revenue DESC;
        """,
    },

    "hourly_distribution": {
        "title": "Hourly Order Distribution",
        "description": "Order volume by hour of day — activity heatmap data.",
        "spark_sql": f"""
            SELECT
                HOUR(event_ts)                    AS order_hour,
                COUNT(DISTINCT order_id)          AS order_count,
                ROUND(SUM(total_price), 2)        AS total_revenue,
                ROUND(AVG(total_price), 2)        AS avg_order_value
            FROM {DATABASE}.orders_silver
            GROUP BY HOUR(event_ts)
            ORDER BY order_hour
        """,
        "trino_sql": f"""
            SELECT
                HOUR(event_ts)                    AS order_hour,
                COUNT(DISTINCT order_id)          AS order_count,
                ROUND(SUM(total_price), 2)        AS total_revenue,
                ROUND(AVG(total_price), 2)        AS avg_order_value
            FROM hive.{DATABASE}.orders_silver
            GROUP BY HOUR(event_ts)
            ORDER BY order_hour;
        """,
    },

    "customer_concentration": {
        "title": "Customer Concentration by State",
        "description": "Top states by order volume with cumulative % (Pareto).",
        "spark_sql": f"""
            SELECT
                customer_state,
                order_count,
                ROUND(total_revenue, 2) AS revenue,
                ROUND(SUM(order_count) OVER (
                    ORDER BY order_count DESC
                ) * 100.0 / SUM(order_count) OVER (), 1) AS cumulative_pct
            FROM {DATABASE}.gold_state_metrics
            ORDER BY order_count DESC
        """,
        "trino_sql": f"""
            SELECT
                customer_state,
                order_count,
                ROUND(total_revenue, 2) AS revenue,
                ROUND(SUM(order_count) OVER (
                    ORDER BY order_count DESC
                ) * 100.0 / SUM(order_count) OVER (), 1) AS cumulative_pct
            FROM hive.{DATABASE}.gold_state_metrics
            ORDER BY order_count DESC;
        """,
    },

    "delivery_sla": {
        "title": "Delivery Performance SLA",
        "description": "% of orders delivered within estimated date, by state.",
        "spark_sql": f"""
            SELECT
                customer_state,
                total_records  AS total_orders,
                late_deliveries,
                (total_records - late_deliveries) AS on_time,
                ROUND((total_records - late_deliveries) * 100.0
                      / total_records, 1)         AS on_time_pct,
                ROUND(avg_delivery_delay_hrs, 1)  AS avg_delay_hrs
            FROM {DATABASE}.gold_state_metrics
            ORDER BY on_time_pct DESC
        """,
        "trino_sql": f"""
            SELECT
                customer_state,
                total_records  AS total_orders,
                late_deliveries,
                (total_records - late_deliveries) AS on_time,
                ROUND((total_records - late_deliveries) * 100.0
                      / total_records, 1)         AS on_time_pct,
                ROUND(avg_delivery_delay_hrs, 1)  AS avg_delay_hrs
            FROM hive.{DATABASE}.gold_state_metrics
            ORDER BY on_time_pct DESC;
        """,
    },
}


def run_spark_queries(spark: SparkSession):

    print("  BI ANALYTICS QUERY SUITE — Spark SQL")
    print(f"  Database: {DATABASE}")
    print(f"  Engine: Spark SQL + Hive Metastore")

    # Ensure database exists
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
        spark.sql(f"USE {DATABASE}")
    except Exception as e:
        log.warning("Could not set database: %s", e)

    # Show available tables
    print(f"\n  Available tables in {DATABASE}:")
    try:
        spark.sql("SHOW TABLES").show(truncate=False)
    except Exception:
        print("  (could not list tables)")

    for key, q in QUERIES.items():
        print(f"\n{'─' * 72}")
        print(f"  {q['title']}")
        print(f"  {q['description']}")
        print(f"{'─' * 72}")

        try:
            result = spark.sql(q["spark_sql"])
            result.show(50, truncate=False)
        except Exception as e:
            print(f"  Query failed: {e}")
            print(f"  (Table may not exist yet — run gold_aggregations.py first)\n")

    print("  All queries complete.")


def print_trino_queries():

    print("  BI ANALYTICS QUERY SUITE — Trino SQL")
    print(f"  Connection: trino --server localhost:8085 --catalog hive")
    print(f"\n-- Connect to Trino:")
    print(f"--   trino --server localhost:8085 --catalog hive --schema {DATABASE}")
    print(f"-- Or via Docker:")
    print(f"--   docker exec -it lakehouse-trino trino --schema {DATABASE}")
    print()

    for key, q in QUERIES.items():
        print(f"-- {q['title']}")
        print(f"-- {q['description']}")
        print(q["trino_sql"])

    print("  Copy-paste the above queries into Trino CLI or any JDBC client.")

def main():
    parser = argparse.ArgumentParser(description="BI Analytics Query Suite")
    parser.add_argument(
        "--engine", default="spark", choices=["spark", "trino"],
        help="spark: execute via Spark SQL; trino: print Trino SQL (default: spark)",
    )
    args = parser.parse_args()

    if args.engine == "trino":
        print_trino_queries()
        return

    spark = (
        SparkSession.builder
        .appName("AnalyticsQueries_Week4")
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

    run_spark_queries(spark)
    spark.stop()


if __name__ == "__main__":
    main()
