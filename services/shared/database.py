from __future__ import annotations

import time

import psycopg2

from services.shared.config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)


def get_connection(max_attempts: int = 30, sleep_seconds: int = 3):
    for attempt in range(1, max_attempts + 1):
        try:
            connection = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
            )
            connection.autocommit = False
            return connection
        except psycopg2.OperationalError:
            if attempt == max_attempts:
                raise
            time.sleep(sleep_seconds)
