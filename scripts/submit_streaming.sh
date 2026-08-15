#!/usr/bin/env bash

# submit_streaming.sh 
# Usage:
#   ./scripts/submit_streaming.sh              # foreground
#   ./scripts/submit_streaming.sh --bg         # background
#   ./scripts/submit_streaming.sh --hudi       # start the Hudi pipeline (foreground)
#   ./scripts/submit_streaming.sh --hudi-bg    # start the Hudi pipeline (background)
#   ./scripts/submit_streaming.sh --dashboard  # run quality dashboard
#   ./scripts/submit_streaming.sh --queries    # run Hudi query examples 
#   ./scripts/submit_streaming.sh --schema     # run schema evolution demo
#   ./scripts/submit_streaming.sh --gold       # build Gold layer aggregations 
#   ./scripts/submit_streaming.sh --analytics  # run BI analytics queries  
#   ./scripts/submit_streaming.sh --trino-sql  # print Trino-compatible SQL 
#   ./scripts/submit_streaming.sh --maintenance # run Hudi maintenance jobs 
#   ./scripts/submit_streaming.sh --stop       # stop the running pipeline

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

MASTER="spark://spark-master:7077"
CONTAINER="lakehouse-spark-master"

# Verify cluster is running 
check_cluster() {
    if ! docker inspect --format='{{.State.Status}}' "$CONTAINER" 2>/dev/null | grep -q running; then
        echo -e "${RED}Error: Spark cluster is not running.${NC}"
        echo -e "  Start it first:  ${CYAN}./scripts/start.sh${NC}"
        exit 1
    fi
}

# Submit the streaming pipeline 
submit_pipeline() {
    local mode=$1  # "fg" or "bg"

    check_cluster

    echo -e "${CYAN}╔════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║  Streaming Pipeline: Kafka → Lakehouse      ║${NC}"
    echo -e "${CYAN}╚════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "  ${YELLOW}Outputs:${NC}"
    echo -e "    Bronze:     s3a://lakehouse/bronze/orders/"
    echo -e "    Silver:     s3a://lakehouse/silver/orders/"
    echo -e "    Quarantine: s3a://lakehouse/bronze/quarantine/"
    echo -e "    Metrics:    s3a://lakehouse/metrics/quality/"
    echo ""

    if [ "$mode" = "bg" ]; then
        echo -e "  ${YELLOW}Mode:${NC} background (detached)"
        docker exec -d "$CONTAINER" \
            /opt/spark/bin/spark-submit \
                --master "$MASTER" \
                --deploy-mode client \
                --driver-memory 512m \
                --executor-memory 1g \
                --executor-cores 2 \
                --conf spark.sql.shuffle.partitions=4 \
                --py-files /opt/spark-jobs/pipeline_utils.py \
                /opt/spark-jobs/streaming_pipeline.py
        echo -e "\n  ${GREEN}Pipeline submitted in background.${NC}"
        echo -e "  View logs:   ${CYAN}docker logs -f $CONTAINER${NC}"
        echo -e "  Spark UI:    ${CYAN}http://localhost:4040${NC}"
        echo -e "  Stop:        ${CYAN}./scripts/submit_streaming.sh --stop${NC}"
    else
        echo -e "  ${YELLOW}Mode:${NC} foreground (Ctrl+C to stop)"
        echo ""
        docker exec "$CONTAINER" \
            /opt/spark/bin/spark-submit \
                --master "$MASTER" \
                --deploy-mode client \
                --driver-memory 512m \
                --executor-memory 1g \
                --executor-cores 2 \
                --conf spark.sql.shuffle.partitions=4 \
                --py-files /opt/spark-jobs/pipeline_utils.py \
                /opt/spark-jobs/streaming_pipeline.py
    fi
}

run_dashboard() {
    check_cluster

    echo -e "${CYAN}╔════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║   Quality Dashboard                                    ║${NC}"
    echo -e "${CYAN}╚════════════════════════════════════════════════════════╝${NC}"
    echo ""

    docker exec "$CONTAINER" \
        /opt/spark/bin/spark-submit \
            --master "$MASTER" \
            --deploy-mode client \
            --driver-memory 512m \
            --conf spark.sql.shuffle.partitions=4 \
            /opt/spark-jobs/quality_dashboard.py
}

stop_pipeline() {
    echo -e "${YELLOW}Stopping streaming pipeline…${NC}"
    docker exec "$CONTAINER" pkill -f streaming_pipeline.py 2>/dev/null || true
    docker exec "$CONTAINER" pkill -f hudi_streaming_pipeline.py 2>/dev/null || true
    echo -e "${GREEN}Pipeline stopped.${NC}"
}

submit_hudi_pipeline() {
    local mode=$1  # "fg" or "bg"

    check_cluster

    echo -e "${CYAN}╔════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║          Hudi ACID Streaming Pipeline                  ║${NC}"
    echo -e "${CYAN}╚════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "  ${YELLOW}Outputs:${NC}"
    echo -e "    Bronze:        s3a://lakehouse/bronze/orders/        (Parquet)"
    echo -e "    Silver (Hudi): s3a://lakehouse/hudi/silver/orders/   (CoW upsert)"
    echo -e "    Quarantine:    s3a://lakehouse/bronze/quarantine/     (Parquet)"
    echo -e "    Metrics:       s3a://lakehouse/metrics/quality/"
    echo ""

    if [ "$mode" = "bg" ]; then
        echo -e "  ${YELLOW}Mode:${NC} background (detached)"
        docker exec -d "$CONTAINER" \
            /opt/spark/bin/spark-submit \
                --master "$MASTER" \
                --deploy-mode client \
                --driver-memory 512m \
                --executor-memory 1g \
                --executor-cores 2 \
                --conf spark.sql.shuffle.partitions=4 \
                --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
                --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
                --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
                --py-files /opt/spark-jobs/pipeline_utils.py \
                /opt/spark-jobs/hudi_streaming_pipeline.py
        echo -e "\n  ${GREEN}Hudi pipeline submitted in background.${NC}"
        echo -e "  View logs:   ${CYAN}docker logs -f $CONTAINER${NC}"
        echo -e "  Spark UI:    ${CYAN}http://localhost:4040${NC}"
        echo -e "  Stop:        ${CYAN}./scripts/submit_streaming.sh --stop${NC}"
    else
        echo -e "  ${YELLOW}Mode:${NC} foreground (Ctrl+C to stop)"
        echo ""
        docker exec "$CONTAINER" \
            /opt/spark/bin/spark-submit \
                --master "$MASTER" \
                --deploy-mode client \
                --driver-memory 512m \
                --executor-memory 1g \
                --executor-cores 2 \
                --conf spark.sql.shuffle.partitions=4 \
                --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
                --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
                --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
                --py-files /opt/spark-jobs/pipeline_utils.py \
                /opt/spark-jobs/hudi_streaming_pipeline.py
    fi
}

run_hudi_queries() {
    check_cluster

    local mode="${1:-all}"

    echo -e "${CYAN}╔════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║   Hudi Query Examples                                  ║${NC}"
    echo -e "${CYAN}╚════════════════════════════════════════════════════════╝${NC}"
    echo ""

    docker exec "$CONTAINER" \
        /opt/spark/bin/spark-submit \
            --master "$MASTER" \
            --deploy-mode client \
            --driver-memory 512m \
            --conf spark.sql.shuffle.partitions=4 \
            --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
            --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
            --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
            --py-files /opt/spark-jobs/pipeline_utils.py \
            /opt/spark-jobs/hudi_query_examples.py --mode "$mode"
}

run_schema_evolution() {
    check_cluster

    echo -e "${CYAN}╔════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║   Schema Evolution Demo                                ║${NC}"
    echo -e "${CYAN}╚════════════════════════════════════════════════════════╝${NC}"
    echo ""

    docker exec "$CONTAINER" \
        /opt/spark/bin/spark-submit \
            --master "$MASTER" \
            --deploy-mode client \
            --driver-memory 512m \
            --conf spark.sql.shuffle.partitions=4 \
            --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
            --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
            --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
            --py-files /opt/spark-jobs/pipeline_utils.py \
            /opt/spark-jobs/schema_evolution.py
}

run_gold_aggregations() {
    local mode="${1:-all}"

    check_cluster

    echo -e "${CYAN}╔════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║   Gold Layer Aggregation                               ║${NC}"
    echo -e "${CYAN}╚════════════════════════════════════════════════════════╝${NC}"
    echo ""

    docker exec "$CONTAINER" \
        /opt/spark/bin/spark-submit \
            --master "$MASTER" \
            --deploy-mode client \
            --driver-memory 512m \
            --executor-memory 1g \
            --executor-cores 2 \
            --conf spark.sql.shuffle.partitions=4 \
            --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
            --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
            --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
            --conf "spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083" \
            --py-files /opt/spark-jobs/pipeline_utils.py \
            /opt/spark-jobs/gold_aggregations.py --mode "$mode"
}

run_analytics_queries() {
    local engine="${1:-spark}"

    check_cluster

    echo -e "${CYAN}╔════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║   BI Analytics Queries                                 ║${NC}"
    echo -e "${CYAN}╚════════════════════════════════════════════════════════╝${NC}"
    echo ""

    docker exec "$CONTAINER" \
        /opt/spark/bin/spark-submit \
            --master "$MASTER" \
            --deploy-mode client \
            --driver-memory 512m \
            --conf spark.sql.shuffle.partitions=4 \
            --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
            --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
            --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
            --conf "spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083" \
            --py-files /opt/spark-jobs/pipeline_utils.py \
            /opt/spark-jobs/analytics_queries.py --engine "$engine"
}

run_maintenance() {
    local operation="${1:-all}"

    check_cluster

    echo -e "${CYAN}╔════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║   Hudi Maintenance Jobs                                ║${NC}"
    echo -e "${CYAN}╚════════════════════════════════════════════════════════╝${NC}"
    echo ""

    docker exec "$CONTAINER" \
        /opt/spark/bin/spark-submit \
            --master "$MASTER" \
            --deploy-mode client \
            --driver-memory 512m \
            --conf spark.sql.shuffle.partitions=4 \
            --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
            --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
            --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
            --conf "spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083" \
            --py-files /opt/spark-jobs/pipeline_utils.py \
            /opt/spark-jobs/hudi_maintenance.py --operation "$operation"
}

case "${1:-}" in
    --bg)
        submit_pipeline "bg"
        ;;
    --hudi)
        submit_hudi_pipeline "fg"
        ;;
    --hudi-bg)
        submit_hudi_pipeline "bg"
        ;;
    --dashboard)
        run_dashboard
        ;;
    --queries)
        run_hudi_queries "${2:-all}"
        ;;
    --schema)
        run_schema_evolution
        ;;
    --gold)
        run_gold_aggregations "${2:-all}"
        ;;
    --analytics)
        run_analytics_queries "spark"
        ;;
    --trino-sql)
        run_analytics_queries "trino"
        ;;
    --maintenance)
        run_maintenance "${2:-all}"
        ;;
    --stop)
        stop_pipeline
        ;;
    --help|-h)
        echo "Usage: $0 [OPTION]"
        echo ""
        echo "   (Parquet pipeline):"
        echo "    (default)     Submit streaming pipeline in foreground"
        echo "    --bg          Submit streaming pipeline in background"
        echo ""
        echo "   (Hudi ACID pipeline):"
        echo "    --hudi        Submit Hudi streaming pipeline in foreground"
        echo "    --hudi-bg     Submit Hudi streaming pipeline in background"
        echo "    --queries     Run Hudi query examples (upsert, time-travel, incremental)"
        echo "    --schema      Run schema evolution demo"
        echo ""
        echo "   (Gold layer & Analytics):"
        echo "    --gold [mode]      Build Gold layer aggregations (all|daily|state|status|category)"
        echo "    --analytics        Run BI analytics queries (Spark SQL)"
        echo "    --trino-sql        Print Trino-compatible SQL queries"
        echo "    --maintenance [op] Run Hudi maintenance (all|stats|clean|cluster|compact)"
        echo ""
        echo "  Common:"
        echo "    --dashboard   Run the quality dashboard (reads metrics from MinIO)"
        echo "    --stop        Stop any running streaming pipeline"
        ;;
    *)
        submit_pipeline "fg"
        ;;
esac
