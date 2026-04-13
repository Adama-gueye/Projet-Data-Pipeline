from __future__ import annotations

import json
from datetime import UTC, datetime

from kafka import KafkaConsumer, KafkaProducer
from psycopg2.extras import Json

from services.shared.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_ANOMALIES,
    KAFKA_TOPIC_ORDERS,
)
from services.shared.database import get_connection


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC_ORDERS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="orders-stream-consumer",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
        retries=10,
    )


def detect_anomalies(event: dict) -> list[str]:
    anomalies: list[str] = []
    if event["quantity"] >= 8:
        anomalies.append("high_quantity")
    if event["unit_price"] >= 2500:
        anomalies.append("price_spike")
    if event["stock_after_order"] < 0:
        anomalies.append("negative_stock")
    if event["total_amount"] >= 5000:
        anomalies.append("high_order_value")
    return anomalies


def persist_fact_order(cursor, event: dict, anomalies: list[str]) -> None:
    cursor.execute(
        """
        insert into streaming.fact_orders (
            event_id,
            event_time,
            order_id,
            customer_id,
            product_id,
            product_category,
            quantity,
            unit_price,
            total_amount,
            payment_method,
            country_code,
            device_type,
            stock_after_order,
            is_anomalous,
            anomaly_reasons,
            raw_payload
        ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (event_id) do nothing
        """,
        (
            event["event_id"],
            datetime.fromisoformat(event["event_time"].replace("Z", "+00:00")),
            event["order_id"],
            event["customer_id"],
            event["product_id"],
            event["product_category"],
            event["quantity"],
            event["unit_price"],
            event["total_amount"],
            event["payment_method"],
            event["country_code"],
            event["device_type"],
            event["stock_after_order"],
            bool(anomalies),
            anomalies,
            Json(event),
        ),
    )


def main() -> None:
    connection = get_connection()
    producer = build_producer()
    consumer = build_consumer()

    for message in consumer:
        event = message.value
        anomalies = detect_anomalies(event)

        with connection.cursor() as cursor:
            persist_fact_order(cursor, event, anomalies)
            connection.commit()

        if anomalies:
            anomaly_event = {
                "event_id": event["event_id"],
                "detected_at": datetime.now(UTC).isoformat(),
                "order_id": event["order_id"],
                "severity": "high" if len(anomalies) >= 2 else "medium",
                "reasons": anomalies,
                "payload": event,
            }
            producer.send(KAFKA_TOPIC_ANOMALIES, anomaly_event)
            producer.flush()
            print(f"forwarded anomaly for {event['order_id']}: {anomalies}")
        else:
            print(f"processed order {event['order_id']}")


if __name__ == "__main__":
    main()

