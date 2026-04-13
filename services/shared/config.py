from __future__ import annotations

import os


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


KAFKA_BOOTSTRAP_SERVERS = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_ORDERS = env("KAFKA_TOPIC_ORDERS", "orders_raw")
KAFKA_TOPIC_ANOMALIES = env("KAFKA_TOPIC_ANOMALIES", "orders_anomalies")

POSTGRES_HOST = env("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(env("POSTGRES_PORT", "5432"))
POSTGRES_DB = env("POSTGRES_DB", "warehouse")
POSTGRES_USER = env("POSTGRES_USER", "de_user")
POSTGRES_PASSWORD = env("POSTGRES_PASSWORD", "de_password")

