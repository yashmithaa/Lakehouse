#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${CYAN}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║   Incremental Data Lakehouse — Service Status         ║${NC}"
echo -e "${CYAN}╚════════════════════════════════════════════════════════╝${NC}"
echo ""

check_service() {
    local container=$1
    local label=$2

    state=$(docker inspect --format='{{.State.Status}}' "$container" 2>/dev/null || echo "not found")
    health=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "n/a")

    if [ "$state" = "running" ] && [ "$health" = "healthy" ]; then
        printf "  %-20s ${GREEN}● running (healthy)${NC}\n" "$label"
    elif [ "$state" = "running" ]; then
        printf "  %-20s ${YELLOW}● running (health: %s)${NC}\n" "$label" "$health"
    else
        printf "  %-20s ${RED}○ %s${NC}\n" "$label" "$state"
    fi
}

check_service "lakehouse-kafka"          "Kafka"
check_service "lakehouse-minio"          "MinIO"
check_service "lakehouse-spark-master"   "Spark Master"
check_service "lakehouse-spark-worker"   "Spark Worker"
check_service "lakehouse-metastore-db"   "Metastore DB"
check_service "lakehouse-hive-metastore" "Hive Metastore"
check_service "lakehouse-trino"          "Trino"

echo ""

echo -e "  ${CYAN}Kafka topics:${NC}"
docker exec lakehouse-kafka kafka-topics \
    --bootstrap-server localhost:9092 --list 2>/dev/null | while read -r topic; do
    echo "    - $topic"
done || echo "    (could not connect)"

echo ""
