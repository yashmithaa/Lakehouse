#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# download_dataset.sh — Download the Brazilian E-Commerce (Olist) dataset
#
# Source: Kaggle — https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
#
# Download uses the Kaggle API and requires a valid token file:
#   ~/.kaggle/kaggle.json
# This script automatically uses either:
#   - kaggle (from PATH), or
#   - .venv/bin/kaggle (project virtualenv)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_DIR="$PROJECT_DIR/data/olist"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${CYAN}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║   Downloading Brazilian E-Commerce (Olist) Dataset    ║${NC}"
echo -e "${CYAN}╚════════════════════════════════════════════════════════╝${NC}"

# ── Check for required CSV files ─────────────────────────────────────────────
REQUIRED_FILES=(
    "olist_orders_dataset.csv"
    "olist_order_items_dataset.csv"
    "olist_order_payments_dataset.csv"
    "olist_customers_dataset.csv"
    "olist_products_dataset.csv"
)

all_present=true
if [ -d "$DATA_DIR" ]; then
    for f in "${REQUIRED_FILES[@]}"; do
        if [ ! -f "$DATA_DIR/$f" ]; then
            all_present=false
            break
        fi
    done
else
    all_present=false
fi

if [ "$all_present" = true ]; then
    echo -e "\n${GREEN}Dataset already present in $DATA_DIR — skipping download.${NC}"
    echo -e "  Files:"
    ls -1 "$DATA_DIR"/*.csv | while read -r f; do echo "    $(basename "$f")"; done
    exit 0
fi

# ── Resolve Kaggle CLI ───────────────────────────────────────────────────────
mkdir -p "$DATA_DIR"

KAGGLE_CMD=""
if command -v kaggle &> /dev/null; then
    KAGGLE_CMD="kaggle"
elif [ -x "$PROJECT_DIR/.venv/bin/kaggle" ]; then
    KAGGLE_CMD="$PROJECT_DIR/.venv/bin/kaggle"
fi

if [ -n "$KAGGLE_CMD" ]; then
    if [ -f "$HOME/.kaggle/kaggle.json" ]; then
        chmod 600 "$HOME/.kaggle/kaggle.json" || true
    fi

    echo -e "\n${YELLOW}[1/2]${NC} Downloading via Kaggle CLI…"
    "$KAGGLE_CMD" datasets download -d olistbr/brazilian-ecommerce -p "$DATA_DIR" --unzip
else
    echo -e "\n${YELLOW}Kaggle CLI not found.${NC}"
    echo ""
    echo -e "  To install it:  ${CYAN}python -m pip install kaggle${NC}"
    echo -e "  Then set up your API token: ${CYAN}~/.kaggle/kaggle.json${NC}"
    echo -e "  See: https://www.kaggle.com/docs/api#authentication"
    exit 1
fi

# ── Verify ───────────────────────────────────────────────────────────────────
echo -e "\n${YELLOW}[2/2]${NC} Verifying downloaded files…"

missing=0
for f in "${REQUIRED_FILES[@]}"; do
    if [ -f "$DATA_DIR/$f" ]; then
        rows=$(wc -l < "$DATA_DIR/$f")
        printf "  %-45s ${GREEN}✓${NC}  %s rows\n" "$f" "$rows"
    else
        printf "  %-45s ✗ missing\n" "$f"
        missing=$((missing + 1))
    fi
done

if [ $missing -gt 0 ]; then
    echo -e "\n${YELLOW}Warning: $missing required file(s) missing.${NC}"
    exit 1
fi

echo -e "\n${GREEN}✓ Dataset ready at ${DATA_DIR}${NC}"
echo ""
