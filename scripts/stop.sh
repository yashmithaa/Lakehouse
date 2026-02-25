#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# stop.sh — One-command teardown for the Incremental Data Lakehouse
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${CYAN}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║   Incremental Data Lakehouse — Stopping Services      ║${NC}"
echo -e "${CYAN}╚════════════════════════════════════════════════════════╝${NC}"

cd "$PROJECT_DIR"

echo ""
echo -e "  Stopping and removing containers + volumes…"
docker compose down -v

echo ""
echo -e "${RED}All services stopped and volumes removed.${NC}"
echo -e "  Run ${CYAN}./scripts/start.sh${NC} to start again."
echo ""
