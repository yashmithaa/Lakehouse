#!/usr/bin/env bash

# submit_streaming.sh 
# Usage:
#   ./scripts/submit_streaming.sh              # start the pipeline (foreground)
#   ./scripts/submit_streaming.sh --bg         # start the pipeline (background)
#   ./scripts/submit_streaming.sh --dashboard  # run quality dashboard
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
    echo -e "${GREEN}Pipeline stopped.${NC}"
}

case "${1:-}" in
    --bg)
        submit_pipeline "bg"
        ;;
    --dashboard)
        run_dashboard
        ;;
    --stop)
        stop_pipeline
        ;;
    --help|-h)
        echo "Usage: $0 [--bg | --dashboard | --stop | --help]"
        echo ""
        echo "  (default)     Submit streaming pipeline in foreground"
        echo "  --bg          Submit streaming pipeline in background"
        echo "  --dashboard   Run the quality dashboard (reads metrics from MinIO)"
        echo "  --stop        Stop the running streaming pipeline"
        ;;
    *)
        submit_pipeline "fg"
        ;;
esac
