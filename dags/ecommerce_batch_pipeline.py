"""E-commerce Batch Pipeline."""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import datetime as dt
import pandas as pd
import json

DATA_PATH = "/opt/airflow/data/external"


def extract_inventory(**context):
    file_path = f"{DATA_PATH}/batch/daily_inventory.csv"
    df = pd.read_csv(file_path)
    print(f"Inventory loaded: {len(df)} records")
    context["ti"].xcom_push(key="inventory_data", value=df.to_dict())
    return len(df)


def extract_deliveries(**context):
    file_path = f"{DATA_PATH}/batch/supplier_deliveries.csv"
    df = pd.read_csv(file_path)
    print(f"Deliveries loaded: {len(df)} records")
    context["ti"].xcom_push(key="deliveries_data", value=df.to_dict())
    return len(df)


def extract_tickets(**context):
    file_path = f"{DATA_PATH}/support/support_tickets.jsonl"
    tickets = []
    with open(file_path, "r") as f:
        for line in f:
            tickets.append(json.loads(line))
    df = pd.DataFrame(tickets)
    print(f"Tickets loaded: {len(df)} records")
    context["ti"].xcom_push(key="tickets_data", value=df.to_dict())
    return len(df)


def load_to_postgres(**context):
    ti = context["ti"]
    inventory_data = ti.xcom_pull(task_ids="extract_inventory", key="inventory_data")
    deliveries_data = ti.xcom_pull(task_ids="extract_deliveries", key="deliveries_data")
    tickets_data = ti.xcom_pull(task_ids="extract_tickets", key="tickets_data")
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()
    if inventory_data:
        df = pd.DataFrame(inventory_data)
        df.to_sql("daily_inventory", engine, if_exists="replace", index=False)
        print(f"Loaded {len(df)} inventory records")
    if deliveries_data:
        df = pd.DataFrame(deliveries_data)
        df.to_sql("supplier_deliveries", engine, if_exists="replace", index=False)
        print(f"Loaded {len(df)} delivery records")
    if tickets_data:
        df = pd.DataFrame(tickets_data)
        df.to_sql("support_tickets", engine, if_exists="replace", index=False)
        print(f"Loaded {len(df)} ticket records")
    return "Data loaded to PostgreSQL"


def save_to_bronze(**context):
    ti = context["ti"]
    inventory_data = ti.xcom_pull(task_ids="extract_inventory", key="inventory_data")
    deliveries_data = ti.xcom_pull(task_ids="extract_deliveries", key="deliveries_data")
    tickets_data = ti.xcom_pull(task_ids="extract_tickets", key="tickets_data")
    import boto3
    from io import StringIO
    s3_client = boto3.client("s3", endpoint_url="http://minio:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin123", config=boto3.session.Config(signature_version="s3v4"))
    bucket_bronze = "ecommerce-bronze"
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    if inventory_data:
        df = pd.DataFrame(inventory_data)
        csv_buffer = StringIO(df.to_csv(index=False))
        s3_client.put_object(Bucket=bucket_bronze, Key=f"inventory/date={timestamp}/inventory.csv", Body=csv_buffer.getvalue())
        print(f"Saved inventory to Bronze")
    if deliveries_data:
        df = pd.DataFrame(deliveries_data)
        csv_buffer = StringIO(df.to_csv(index=False))
        s3_client.put_object(Bucket=bucket_bronze, Key=f"deliveries/date={timestamp}/deliveries.csv", Body=csv_buffer.getvalue())
        print(f"Saved deliveries to Bronze")
    if tickets_data:
        df = pd.DataFrame(tickets_data)
        json_buffer = StringIO(df.to_json(orient="records", lines=True))
        s3_client.put_object(Bucket=bucket_bronze, Key=f"tickets/date={timestamp}/tickets.jsonl", Body=json_buffer.getvalue())
        print(f"Saved tickets to Bronze")
    return "Data saved to Bronze layer"


def save_to_silver(**context):
    import boto3
    from io import StringIO
    s3_client = boto3.client("s3", endpoint_url="http://minio:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin123", config=boto3.session.Config(signature_version="s3v4"))
    bucket_bronze = "ecommerce-bronze"
    bucket_silver = "ecommerce-silver"
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_bronze, Prefix="inventory/")
        if "Contents" in response:
            latest_inv = sorted(response["Contents"], key=lambda x: x["Key"])[-1]
            obj = s3_client.get_object(Bucket=bucket_bronze, Key=latest_inv["Key"])
            df_inv = pd.read_csv(obj["Body"])
            df_inv_clean = df_inv.drop_duplicates().dropna(subset=["product_id", "product_name"])
            csv_buffer = StringIO(df_inv_clean.to_csv(index=False))
            s3_client.put_object(Bucket=bucket_silver, Key=f"inventory/date={timestamp}/inventory_clean.csv", Body=csv_buffer.getvalue())
            print(f"Saved cleaned inventory to Silver: {len(df_inv_clean)} records")
    except Exception as e:
        print(f"Silver inventory error: {e}")
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_bronze, Prefix="deliveries/")
        if "Contents" in response:
            latest_del = sorted(response["Contents"], key=lambda x: x["Key"])[-1]
            obj = s3_client.get_object(Bucket=bucket_bronze, Key=latest_del["Key"])
            df_del = pd.read_csv(obj["Body"])
            df_del_clean = df_del.dropna(subset=["delivery_id", "supplier_id"])
            csv_buffer = StringIO(df_del_clean.to_csv(index=False))
            s3_client.put_object(Bucket=bucket_silver, Key=f"deliveries/date={timestamp}/deliveries_clean.csv", Body=csv_buffer.getvalue())
            print(f"Saved cleaned deliveries to Silver: {len(df_del_clean)} records")
    except Exception as e:
        print(f"Silver deliveries error: {e}")
    return "Data saved to Silver layer"


def save_to_gold(**context):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()
    import boto3
    from io import StringIO
    s3_client = boto3.client("s3", endpoint_url="http://minio:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin123", config=boto3.session.Config(signature_version="s3v4"))
    bucket_gold = "ecommerce-gold"
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    with engine.connect() as conn:
        low_stock = pd.read_sql("SELECT product_id, product_name, quantity_in_stock, reorder_level FROM daily_inventory", conn)
        suppliers = pd.read_sql("SELECT supplier_name, COUNT(*) as total_deliveries, AVG(delivery_days) as avg_delivery_days FROM supplier_deliveries GROUP BY supplier_name", conn)
        tickets = pd.read_sql("SELECT priority, status, COUNT(*) as count FROM support_tickets GROUP BY priority, status", conn)
    if not low_stock.empty:
        csv_buffer = StringIO(low_stock.to_csv(index=False))
        s3_client.put_object(Bucket=bucket_gold, Key=f"inventory_summary/date={timestamp}/low_stock.csv", Body=csv_buffer.getvalue())
        print(f"Saved inventory summary to Gold")
    if not suppliers.empty:
        csv_buffer = StringIO(suppliers.to_csv(index=False))
        s3_client.put_object(Bucket=bucket_gold, Key=f"supplier_summary/date={timestamp}/suppliers.csv", Body=csv_buffer.getvalue())
        print(f"Saved supplier summary to Gold")
    if not tickets.empty:
        csv_buffer = StringIO(tickets.to_csv(index=False))
        s3_client.put_object(Bucket=bucket_gold, Key=f"tickets_summary/date={timestamp}/tickets.csv", Body=csv_buffer.getvalue())
        print(f"Saved tickets summary to Gold")
    return "Aggregates saved to Gold layer"


def generate_report(**context):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()
    print("=" * 60)
    print("RAPPORT ANALYTIQUE E-COMMERCE")
    print("=" * 60)
    with engine.connect() as conn:
        low_stock = pd.read_sql("SELECT product_id, product_name, quantity_in_stock, reorder_level FROM daily_inventory WHERE quantity_in_stock < reorder_level", conn)
        print(f"PRODUITS EN RUPTURE ({len(low_stock)}):")
        if not low_stock.empty:
            print(low_stock.to_string(index=False))
        suppliers = pd.read_sql("SELECT supplier_name, COUNT(*) as total, AVG(delivery_days) as avg_days FROM supplier_deliveries GROUP BY supplier_name", conn)
        print("PERFORMANCE FOURNISSEURS:")
        if not suppliers.empty:
            print(suppliers.to_string(index=False))
        tickets = pd.read_sql("SELECT priority, status, COUNT(*) as count FROM support_tickets GROUP BY priority, status", conn)
        print("TICKETS DE SUPPORT:")
        if not tickets.empty:
            print(tickets.to_string(index=False))
    print("=" * 60)
    return {"low_stock": len(low_stock), "suppliers": len(suppliers), "tickets": len(tickets)}


dag = DAG(
    dag_id="ecommerce_batch_pipeline",
    description="Pipeline ETL E-commerce avec Medallion Architecture",
    start_date=dt.datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["ecommerce", "etl", "xcom", "medallion", "minio"],
)

extract_inv = PythonOperator(task_id="extract_inventory", python_callable=extract_inventory, dag=dag)
extract_deliv = PythonOperator(task_id="extract_deliveries", python_callable=extract_deliveries, dag=dag)
extract_tickets = PythonOperator(task_id="extract_tickets", python_callable=extract_tickets, dag=dag)
load_postgres = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres, dag=dag)
save_bronze = PythonOperator(task_id="save_to_bronze", python_callable=save_to_bronze, dag=dag)
save_silver = PythonOperator(task_id="save_to_silver", python_callable=save_to_silver, dag=dag)
save_gold = PythonOperator(task_id="save_to_gold", python_callable=save_to_gold, dag=dag)
generate = PythonOperator(task_id="generate_report", python_callable=generate_report, dag=dag)

[extract_inv, extract_deliv, extract_tickets] >> load_postgres >> save_bronze >> save_silver >> save_gold >> generate
