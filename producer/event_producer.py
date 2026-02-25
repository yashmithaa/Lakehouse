#!/usr/bin/env python3
"""
Olist Order Event Replay Producer
===================================
Reads the Brazilian E-Commerce (Olist) public dataset CSV files, joins
orders + items + payments + customers + products into enriched order events,
and replays them into Kafka as a realistic event stream.

The dataset contains ~100k real orders from 2016–2018. Events are streamed
in chronological order by order_purchase_timestamp to simulate real-time
ingestion, with configurable replay speed.

Dataset: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Usage:
    python event_producer.py                          # defaults
    python event_producer.py --rate 100 --topic orders
    python event_producer.py --rate 50 --loop          # loop forever

Environment variables (overridden by CLI args):
    KAFKA_BOOTSTRAP  — Kafka broker address (default: localhost:9092)
    TOPIC            — Target topic (default: orders)
    DATA_DIR         — Path to Olist CSV directory (default: ../data/olist)
"""

import argparse
import csv
import json
import logging
import os
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("olist-producer")

# ── Graceful shutdown ────────────────────────────────────────────────────────
running = True


def _shutdown(signum, frame):
    global running
    log.info("Shutdown signal received — finishing current batch…")
    running = False


signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)


# ── CSV loader helpers ───────────────────────────────────────────────────────

def load_csv(path: Path) -> list[dict]:
    """Load a CSV file into a list of dicts."""
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def index_by(rows: list[dict], key: str) -> dict[str, list[dict]]:
    """Group rows by a key column into a dict of lists."""
    idx: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        idx[row[key]].append(row)
    return idx


# ── Dataset loader ───────────────────────────────────────────────────────────

def load_olist_dataset(data_dir: Path) -> list[dict]:
    """
    Load and join Olist CSVs into enriched order events, sorted by
    order_purchase_timestamp. Each order-item combination becomes one event.
    """
    log.info("Loading Olist dataset from %s …", data_dir)

    orders     = load_csv(data_dir / "olist_orders_dataset.csv")
    items      = load_csv(data_dir / "olist_order_items_dataset.csv")
    payments   = load_csv(data_dir / "olist_order_payments_dataset.csv")
    customers  = load_csv(data_dir / "olist_customers_dataset.csv")
    products   = load_csv(data_dir / "olist_products_dataset.csv")

    log.info(
        "  Loaded: orders=%d  items=%d  payments=%d  customers=%d  products=%d",
        len(orders), len(items), len(payments), len(customers), len(products),
    )

    # Build lookup indexes
    items_by_order    = index_by(items, "order_id")
    payments_by_order = index_by(payments, "order_id")
    customers_by_id   = {c["customer_id"]: c for c in customers}
    products_by_id    = {p["product_id"]: p for p in products}

    # Map Olist status to our normalized status
    STATUS_MAP = {
        "created":      "created",
        "approved":     "confirmed",
        "invoiced":     "confirmed",
        "processing":   "confirmed",
        "shipped":      "shipped",
        "delivered":    "delivered",
        "unavailable":  "cancelled",
        "canceled":     "cancelled",
    }

    events = []

    for order in orders:
        order_id    = order["order_id"]
        customer_id = order["customer_id"]
        status_raw  = order.get("order_status", "created")
        purchase_ts = order.get("order_purchase_timestamp", "")

        if not purchase_ts:
            continue

        customer = customers_by_id.get(customer_id, {})
        order_items = items_by_order.get(order_id, [])
        order_payments = payments_by_order.get(order_id, [])

        # Customer location
        customer_state = customer.get("customer_state", "unknown")

        # Total payment value
        total_payment = sum(
            float(p.get("payment_value", 0)) for p in order_payments
        )

        # Build one event per order-item line
        for item in order_items:
            product_id = item.get("product_id", "unknown")
            product = products_by_id.get(product_id, {})

            unit_price = float(item.get("price", 0))
            freight    = float(item.get("freight_value", 0))
            quantity   = 1  # Olist items are one row per unit

            event = {
                "order_id":           order_id,
                "customer_id":        customer_id,
                "product_id":         product_id,
                "product_category":   product.get("product_category_name", "unknown"),
                "seller_id":          item.get("seller_id", "unknown"),
                "quantity":           quantity,
                "unit_price":         round(unit_price, 2),
                "freight_value":      round(freight, 2),
                "total_price":        round(unit_price + freight, 2),
                "order_status":       STATUS_MAP.get(status_raw, status_raw),
                "customer_state":     customer_state,
                "customer_city":      customer.get("customer_city", "unknown"),
                "payment_type":       order_payments[0].get("payment_type", "unknown") if order_payments else "unknown",
                "payment_value":      round(total_payment, 2),
                "event_time":         purchase_ts.replace(" ", "T") + "Z",
                "order_delivered_ts": order.get("order_delivered_customer_date", ""),
                "order_estimated_ts": order.get("order_estimated_delivery_date", ""),
                "processing_time":    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            events.append(event)

    # Sort by event_time for chronological replay
    events.sort(key=lambda e: e["event_time"])
    log.info("  Built %d order-item events (sorted chronologically).", len(events))
    return events


# ── Main loop ────────────────────────────────────────────────────────────────

def run(bootstrap: str, topic: str, rate: int, data_dir: Path, loop: bool):
    events = load_olist_dataset(data_dir)
    if not events:
        log.error("No events loaded — check dataset path: %s", data_dir)
        sys.exit(1)

    log.info(
        "Connecting to Kafka at %s  →  topic=%s  rate=%d msg/s  loop=%s",
        bootstrap, topic, rate, loop,
    )

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        linger_ms=10,
        batch_size=32_768,
    )

    interval = 1.0 / rate if rate > 0 else 0
    total_sent = 0
    pass_num = 0

    try:
        while running:
            pass_num += 1
            log.info("=== Pass %d: replaying %d events ===", pass_num, len(events))
            start = time.monotonic()

            for event in events:
                if not running:
                    break

                # Stamp processing_time with current wall clock
                event["processing_time"] = datetime.now(timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                )

                producer.send(topic, key=event["order_id"], value=event)
                total_sent += 1

                # Log progress every 5000 events
                if total_sent % 5000 == 0:
                    elapsed = time.monotonic() - start
                    eps = total_sent / max(elapsed, 0.001)
                    log.info("  Sent %d events  (%.0f/s)", total_sent, eps)

                # Throttle
                if interval > 0:
                    time.sleep(interval)

            elapsed = time.monotonic() - start
            log.info(
                "Pass %d complete — %d events in %.1fs (%.0f/s)",
                pass_num, len(events), elapsed, len(events) / max(elapsed, 1),
            )

            if not loop:
                break

    except Exception:
        log.exception("Producer error")
    finally:
        producer.flush()
        producer.close()
        log.info("DONE — %d total events sent across %d pass(es).", total_sent, pass_num)


# ── CLI ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Olist order event replay producer")
    p.add_argument(
        "--bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    p.add_argument(
        "--topic",
        default=os.getenv("TOPIC", "orders"),
        help="Kafka topic (default: orders)",
    )
    p.add_argument(
        "--rate",
        type=int,
        default=50,
        help="Events per second (default: 50)",
    )
    p.add_argument(
        "--data-dir",
        default=os.getenv("DATA_DIR", str(Path(__file__).resolve().parent.parent / "data" / "olist")),
        help="Path to Olist CSV directory (default: ../data/olist)",
    )
    p.add_argument(
        "--loop",
        action="store_true",
        help="Loop over the dataset continuously (default: one pass then stop)",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.bootstrap, args.topic, args.rate, Path(args.data_dir), args.loop)
