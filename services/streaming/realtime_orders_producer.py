from __future__ import annotations

import json
import os
import random
import time
import uuid
from datetime import UTC, datetime

from kafka import KafkaProducer

from services.shared.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_ORDERS

PRODUCTS = [
    {"product_id": "P-100", "name": "smartphone", "base_price": 699.0, "category": "electronics"},
    {"product_id": "P-101", "name": "laptop", "base_price": 1299.0, "category": "electronics"},
    {"product_id": "P-102", "name": "headphones", "base_price": 119.0, "category": "electronics"},
    {"product_id": "P-103", "name": "office-chair", "base_price": 249.0, "category": "furniture"},
    {"product_id": "P-104", "name": "coffee-machine", "base_price": 89.0, "category": "home"},
]

COUNTRIES = ["SN", "FR", "CI", "MA", "TN"]
PAYMENT_METHODS = ["card", "mobile_money", "paypal", "bank_transfer"]
DEVICES = ["mobile", "desktop", "tablet"]


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
        retries=10,
    )


def generate_order() -> dict:
    product = random.choice(PRODUCTS)
    quantity = random.choice([1, 1, 1, 2, 2, 3, 4])
    unit_price = round(product["base_price"] * random.uniform(0.85, 1.20), 2)
    stock_after_order = random.randint(0, 250)
    anomaly_roll = random.random()

    anomaly_hint = "none"
    if anomaly_roll < 0.08:
        quantity = random.randint(8, 15)
        anomaly_hint = "suspicious_quantity"
    elif anomaly_roll < 0.12:
        unit_price = round(product["base_price"] * random.uniform(3.0, 5.5), 2)
        anomaly_hint = "price_spike"
    elif anomaly_roll < 0.16:
        stock_after_order = random.randint(-5, -1)
        anomaly_hint = "negative_stock"

    total_amount = round(quantity * unit_price, 2)
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(UTC).isoformat(),
        "order_id": f"ORD-{random.randint(100000, 999999)}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "product_id": product["product_id"],
        "product_category": product["category"],
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": total_amount,
        "payment_method": random.choice(PAYMENT_METHODS),
        "country_code": random.choice(COUNTRIES),
        "device_type": random.choice(DEVICES),
        "stock_after_order": stock_after_order,
        "anomaly_hint": anomaly_hint,
    }


def main() -> None:
    interval = float(os.getenv("EMIT_INTERVAL_SECONDS", "2"))
    producer = build_producer()

    while True:
        order = generate_order()
        producer.send(KAFKA_TOPIC_ORDERS, order)
        producer.flush()
        print(f"sent order {order['order_id']} ({order['anomaly_hint']})")
        time.sleep(interval)


if __name__ == "__main__":
    main()

