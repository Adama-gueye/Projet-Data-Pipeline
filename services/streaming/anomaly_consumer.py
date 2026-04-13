from __future__ import annotations

import json
from datetime import datetime

from kafka import KafkaConsumer
from psycopg2.extras import Json

from services.shared.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_ANOMALIES
from services.shared.database import get_connection


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC_ANOMALIES,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="anomaly-audit-consumer",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def main() -> None:
    connection = get_connection()
    consumer = build_consumer()

    for message in consumer:
        event = message.value
        with connection.cursor() as cursor:
            cursor.execute(
                """
                insert into streaming.fact_order_anomalies (
                    event_id,
                    detected_at,
                    order_id,
                    severity,
                    reasons,
                    payload
                ) values (%s, %s, %s, %s, %s, %s)
                on conflict (event_id) do nothing
                """,
                (
                    event["event_id"],
                    datetime.fromisoformat(event["detected_at"]),
                    event["order_id"],
                    event["severity"],
                    event["reasons"],
                    Json(event["payload"]),
                ),
            )
            connection.commit()
        print(f"persisted anomaly {event['order_id']}")


if __name__ == "__main__":
    main()
