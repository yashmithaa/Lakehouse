#!/usr/bin/env bash
# start.sh — One-command startup for the Incremental Data Lakehouse

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║   Incremental Data Lakehouse — Starting Services      ║${NC}"
echo -e "${CYAN}╚════════════════════════════════════════════════════════╝${NC}"

cd "$PROJECT_DIR"

# Build & start all services 
echo -e "\n${YELLOW}[1/3]${NC} Building Docker images and starting services…"
docker compose up -d --build

# Wait for health checks 
echo -e "\n${YELLOW}[2/3]${NC} Waiting for services to become healthy…"

wait_for_service() {
    local service=$1
    local max_wait=$2
    local elapsed=0

    printf "  %-20s" "$service"
    while [ $elapsed -lt $max_wait ]; do
        status=$(docker inspect --format='{{.State.Health.Status}}' "lakehouse-$service" 2>/dev/null || echo "missing")
        if [ "$status" = "healthy" ]; then
            echo -e " ${GREEN}✓ healthy${NC}"
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    echo -e "still starting (waited ${max_wait}s)"
    return 0
}

wait_for_service "kafka"        90
wait_for_service "minio"        30
wait_for_service "spark-master" 60

#  Create Kafka topic explicitly (idempotent)
echo -e "\n${YELLOW}[3/3]${NC} Ensuring Kafka topic 'orders' exists…"
docker exec lakehouse-kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic orders \
    --partitions 3 \
    --replication-factor 1 2>/dev/null || true

echo -e "  orders                ${GREEN}✓ ready${NC}"

echo -e "\n${GREEN}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║   All services are up!                                 ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  ${CYAN}Kafka${NC}          → localhost:9092"
echo -e "  ${CYAN}MinIO API${NC}      → http://localhost:9000  (minioadmin / minioadmin)"
echo -e "  ${CYAN}MinIO Console${NC}  → http://localhost:9001"
echo -e "  ${CYAN}Spark Master${NC}   → http://localhost:8080"
echo -e "  ${CYAN}Spark Worker${NC}   → http://localhost:8081"
echo ""
echo -e "  ${YELLOW}Next steps:${NC}"
echo -e "    1. Download dataset:     ./scripts/download_dataset.sh"
echo -e "    2. Start producer:       cd producer && pip install -r requirements.txt && python event_producer.py"
echo -e ""
echo -e "  ${YELLOW} Console consumer (debug):${NC}"
echo -e "    3a. docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-jobs/bronze_streaming_consumer.py"
echo -e ""
echo -e "  ${YELLOW} Streaming pipeline (Kafka → Bronze + Silver + Quarantine):${NC}"
echo -e "    3b. ./scripts/submit_streaming.sh            # foreground"
echo -e "    3b. ./scripts/submit_streaming.sh --bg       # background"
echo -e "    4.  ./scripts/submit_streaming.sh --dashboard  # quality metrics"
echo ""
