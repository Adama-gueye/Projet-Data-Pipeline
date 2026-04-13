from __future__ import annotations

import json
import os
from pathlib import Path

import boto3
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

PROJECT_DATA_DIR = Path("/opt/project/data")

WAREHOUSE_HOST = os.getenv("WAREHOUSE_HOST", "postgres")
WAREHOUSE_PORT = int(os.getenv("WAREHOUSE_PORT", "5432"))
WAREHOUSE_DB = os.getenv("WAREHOUSE_DB", "warehouse")
WAREHOUSE_USER = os.getenv("WAREHOUSE_USER", "de_user")
WAREHOUSE_PASSWORD = os.getenv("WAREHOUSE_PASSWORD", "de_password")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_BUCKET_RAW = os.getenv("MINIO_BUCKET_RAW", "raw")


def get_pg_connection():
    return psycopg2.connect(
        host=WAREHOUSE_HOST,
        port=WAREHOUSE_PORT,
        dbname=WAREHOUSE_DB,
        user=WAREHOUSE_USER,
        password=WAREHOUSE_PASSWORD,
    )


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def upload_sources_to_minio(**context):
    run_date = context["ds"]
    s3 = get_s3_client()
    existing_buckets = {bucket["Name"] for bucket in s3.list_buckets().get("Buckets", [])}
    if MINIO_BUCKET_RAW not in existing_buckets:
        s3.create_bucket(Bucket=MINIO_BUCKET_RAW)
    uploads = [
        (PROJECT_DATA_DIR / "batch" / "daily_inventory.csv", f"{run_date}/daily_inventory.csv"),
        (PROJECT_DATA_DIR / "batch" / "supplier_deliveries.csv", f"{run_date}/supplier_deliveries.csv"),
        (PROJECT_DATA_DIR / "support" / "support_tickets.jsonl", f"{run_date}/support_tickets.jsonl"),
    ]
    for source_path, target_key in uploads:
        s3.upload_file(str(source_path), MINIO_BUCKET_RAW, target_key)


def load_inventory_snapshot():
    inventory_df = pd.read_csv(PROJECT_DATA_DIR / "batch" / "daily_inventory.csv")
    with get_pg_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("truncate table staging.inventory_snapshot")
            for row in inventory_df.itertuples(index=False):
                cursor.execute(
                    """
                    insert into staging.inventory_snapshot (
                        snapshot_date,
                        product_id,
                        warehouse_code,
                        stock_on_hand,
                        reserved_stock,
                        reorder_threshold
                    ) values (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        row.snapshot_date,
                        row.product_id,
                        row.warehouse_code,
                        int(row.stock_on_hand),
                        int(row.reserved_stock),
                        int(row.reorder_threshold),
                    ),
                )
        connection.commit()


def load_supplier_deliveries():
    deliveries_df = pd.read_csv(PROJECT_DATA_DIR / "batch" / "supplier_deliveries.csv")
    with get_pg_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("truncate table staging.supplier_deliveries")
            for row in deliveries_df.itertuples(index=False):
                cursor.execute(
                    """
                    insert into staging.supplier_deliveries (
                        delivery_date,
                        supplier_id,
                        product_id,
                        delivered_quantity,
                        delivery_status
                    ) values (%s, %s, %s, %s, %s)
                    """,
                    (
                        row.delivery_date,
                        row.supplier_id,
                        row.product_id,
                        int(row.delivered_quantity),
                        row.delivery_status,
                    ),
                )
        connection.commit()


def load_support_tickets():
    with open(PROJECT_DATA_DIR / "support" / "support_tickets.jsonl", "r", encoding="utf-8") as handle:
        records = [json.loads(line) for line in handle if line.strip()]

    with get_pg_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("truncate table staging.support_tickets")
            for record in records:
                cursor.execute(
                    """
                    insert into staging.support_tickets (
                        ticket_id,
                        opened_at,
                        customer_id,
                        channel,
                        category,
                        priority,
                        status
                    ) values (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        record["ticket_id"],
                        record["opened_at"],
                        record["customer_id"],
                        record["channel"],
                        record["category"],
                        record["priority"],
                        record["status"],
                    ),
                )
        connection.commit()


def refresh_analytics_tables():
    with get_pg_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("truncate table analytics.daily_inventory_health")
            cursor.execute(
                """
                insert into analytics.daily_inventory_health (
                    snapshot_date,
                    product_id,
                    warehouse_code,
                    stock_on_hand,
                    reserved_stock,
                    available_stock,
                    reorder_threshold,
                    stock_status
                )
                select
                    snapshot_date,
                    product_id,
                    warehouse_code,
                    stock_on_hand,
                    reserved_stock,
                    stock_on_hand - reserved_stock as available_stock,
                    reorder_threshold,
                    case
                        when stock_on_hand - reserved_stock <= reorder_threshold then 'reorder'
                        when stock_on_hand - reserved_stock <= reorder_threshold * 1.5 then 'watch'
                        else 'healthy'
                    end as stock_status
                from staging.inventory_snapshot
                """
            )

            cursor.execute("truncate table analytics.daily_support_kpis")
            cursor.execute(
                """
                insert into analytics.daily_support_kpis (
                    opened_date,
                    channel,
                    total_tickets,
                    urgent_tickets,
                    open_tickets
                )
                select
                    cast(opened_at as date) as opened_date,
                    channel,
                    count(*) as total_tickets,
                    count(*) filter (where priority in ('high', 'critical')) as urgent_tickets,
                    count(*) filter (where status <> 'resolved') as open_tickets
                from staging.support_tickets
                group by cast(opened_at as date), channel
                """
            )
        connection.commit()


with DAG(
    dag_id="ecommerce_batch_pipeline",
    description="Daily batch pipeline for the DSIA e-commerce exam project",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["dsia", "ecommerce", "batch"],
) as dag:
    upload_sources = PythonOperator(
        task_id="upload_sources_to_minio",
        python_callable=upload_sources_to_minio,
    )

    load_inventory = PythonOperator(
        task_id="load_inventory_snapshot",
        python_callable=load_inventory_snapshot,
    )

    load_deliveries = PythonOperator(
        task_id="load_supplier_deliveries",
        python_callable=load_supplier_deliveries,
    )

    load_tickets = PythonOperator(
        task_id="load_support_tickets",
        python_callable=load_support_tickets,
    )

    refresh_analytics = PythonOperator(
        task_id="refresh_analytics_tables",
        python_callable=refresh_analytics_tables,
    )

    upload_sources >> [load_inventory, load_deliveries, load_tickets] >> refresh_analytics
