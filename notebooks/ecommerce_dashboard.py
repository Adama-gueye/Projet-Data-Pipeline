import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # E-commerce Data Pipeline Dashboard

    Ce notebook permet d'explorer et surveiller l'ensemble du pipeline de donnees e-commerce :

    1. **Donnees streaming** - Commandes temps reel depuis Kafka
    2. **Anomalies detectees** - Surveillance des commandes suspectes
    3. **Donnees batch** - Inventaire, livraisons, tickets support
    4. **Analytique** - KPIs et resumés operationnels

    ## Architecture

    ```
    Producer → Kafka (orders_raw) → Consumer → PostgreSQL (streaming.fact_orders)
                                              → Kafka (orders_anomalies) → Anomaly Consumer
    Batch Files → Airflow DAG → PostgreSQL (staging/analytics) + MinIO
    ```

    ## Services Disponibles

    - **Kafka**: localhost:9092
    - **Kafbat UI**: http://localhost:8090
    - **PostgreSQL**: localhost:5432 (warehouse/de_user/de_password)
    - **Airflow**: http://localhost:8080
    - **MinIO Console**: http://localhost:9001
    """)
    return


@app.cell
def _():
    import os
    import json
    import psycopg2
    import pandas as pd
    from datetime import datetime

    # Configuration PostgreSQL
    POSTGRES_CONFIG = {
        'host': os.getenv('POSTGRES_HOST', 'postgres'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'user': os.getenv('POSTGRES_USER', 'de_user'),
        'password': os.getenv('POSTGRES_PASSWORD', 'de_password'),
        'database': os.getenv('POSTGRES_DB', 'warehouse')
    }

    print(f"Configuration chargee")
    print(f"PostgreSQL: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}")
    return POSTGRES_CONFIG, datetime, json, pd, psycopg2


@app.cell
def _(POSTGRES_CONFIG, pd, psycopg2):
    def get_connection():
        """Creer une connexion a PostgreSQL"""
        return psycopg2.connect(**POSTGRES_CONFIG)

    def query_sql(sql):
        """Executer une requete SQL et retourner un DataFrame"""
        conn = get_connection()
        df = pd.read_sql_query(sql, conn)
        conn.close()
        return df

    # Test de connexion
    try:
        test_df = query_sql("SELECT 1 as test")
        print(f"Connexion PostgreSQL reussie")
    except Exception as e:
        print(f"Erreur connexion PostgreSQL: {e}")
    return get_connection, query_sql


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 📊 Streaming - Commandes en Temps Reel
    """)
    return


@app.cell
def _(mo, query_sql):
    # Recuperer les commandes recentes
    orders_df = query_sql("""
        SELECT
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
            is_anomalous,
            anomaly_reasons
        FROM streaming.fact_orders
        ORDER BY event_time DESC
        LIMIT 50
    """)

    total_orders = len(orders_df)
    anomalous_orders = orders_df['is_anomalous'].sum() if 'is_anomalous' in orders_df.columns else 0

    mo.md(f"""
    ### Commandes Recentes

    **Total commandes affichees**: {total_orders}
    **Dont anomalnes**: {anomalous_orders}
    """)
    return orders_df


@app.cell
def _(mo, orders_df):
    mo.ui.table(orders_df)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## ⚠️ Anomalies Detectees
    """)
    return


@app.cell
def _(mo, query_sql):
    anomalies_df = query_sql("""
        SELECT
            detected_at,
            order_id,
            severity,
            reasons,
            payload
        FROM streaming.fact_order_anomalies
        ORDER BY detected_at DESC
        LIMIT 20
    """)

    total_anomalies = query_sql("SELECT COUNT(*) as cnt FROM streaming.fact_order_anomalies")['cnt'][0]
    high_severity = query_sql("SELECT COUNT(*) as cnt FROM streaming.fact_order_anomalies WHERE severity = 'high'")['cnt'][0]

    mo.md(f"""
    ### Anomalies Recentes

    **Total anomalies**: {total_anomalies}
    **Severity haute**: {high_severity}

    **Types d'anomalies**:
    - `high_quantity`: Quantite >= 8
    - `price_spike`: Prix unitaire >= 2500
    - `negative_stock`: Stock negatif
    - `high_order_value`: Montant commande >= 5000
    """)
    return anomalies_df, high_severity, total_anomalies


@app.cell
def _(anomalies_df, mo):
    mo.ui.table(anomalies_df)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 📦 Batch - Inventaire et Sante des Stocks
    """)
    return


@app.cell
def _(mo, query_sql):
    inventory_df = query_sql("""
        SELECT
            snapshot_date,
            product_id,
            warehouse_code,
            stock_on_hand,
            reserved_stock,
            available_stock,
            reorder_threshold,
            stock_status
        FROM analytics.daily_inventory_health
        ORDER BY snapshot_date DESC, stock_status, product_id
    """)

    healthy = len(inventory_df[inventory_df['stock_status'] == 'healthy']) if 'stock_status' in inventory_df.columns else 0
    reorder = len(inventory_df[inventory_df['stock_status'] == 'reorder']) if 'stock_status' in inventory_df.columns else 0
    watch = len(inventory_df[inventory_df['stock_status'] == 'watch']) if 'stock_status' in inventory_df.columns else 0

    mo.md(f"""
    ### Inventaire Actuel

    **Entrepots sains (healthy)**: {healthy}
    **En reapprovisionnement (reorder)**: {reorder}
    **En surveillance (watch)**: {watch}

    **Regles de statut**:
    - `healthy`: stock disponible > 1.5 x seuil de reapprovisionnement
    - `watch`: stock disponible <= 1.5 x seuil
    - `reorder`: stock disponible <= seuil
    """)
    return inventory_df, reorder, watch


@app.cell
def _(inventory_df, mo):
    mo.ui.table(inventory_df)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 🎫 Support - KPIs des Tickets
    """)
    return


@app.cell
def _(mo, query_sql):
    support_df = query_sql("""
        SELECT
            opened_date,
            channel,
            total_tickets,
            urgent_tickets,
            open_tickets
        FROM analytics.daily_support_kpis
        ORDER BY opened_date DESC, urgent_tickets DESC
    """)

    total_tickets = support_df['total_tickets'].sum() if 'total_tickets' in support_df.columns else 0
    urgent = support_df['urgent_tickets'].sum() if 'urgent_tickets' in support_df.columns else 0

    mo.md(f"""
    ### KPIs Support

    **Total tickets**: {total_tickets}
    **Tickets urgents (high/critical)**: {urgent}
    """)
    return support_df, total_tickets, urgent


@app.cell
def _(mo, support_df):
    mo.ui.table(support_df)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 💰 Analytique - Resumé des Commandes
    """)
    return


@app.cell
def _(mo, query_sql):
    summary_df = query_sql("""
        SELECT
            order_day,
            product_category,
            payment_method,
            total_orders,
            gross_revenue,
            anomalous_orders
        FROM analytics.v_orders_summary
        ORDER BY order_day DESC, gross_revenue DESC
        LIMIT 20
    """)

    total_revenue = summary_df['gross_revenue'].sum() if 'gross_revenue' in summary_df.columns else 0
    total_orders_summary = summary_df['total_orders'].sum() if 'total_orders' in summary_df.columns else 0

    mo.md(f"""
    ### Resumé des Ventes

    **Revenus bruts totaux**: {total_revenue:,.2f}
    **Total commandes**: {total_orders_summary}
    """)
    return summary_df, total_revenue, total_orders_summary


@app.cell
def _(mo, summary_df):
    mo.ui.table(summary_df)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 📡 Acces aux Services
    - **Kafbat UI** (Kafka): http://localhost:8090
    - **Airflow**: http://localhost:8080 (airflow/airflow)
    - **MinIO Console**: http://localhost:9001 (minio/minio123)
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 📓 Notebooks - Cliquer pour ouvrir dans une nouvelle fenetre
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    <style>
    .notebook-links a {
        display: inline-block;
        padding: 15px 25px;
        margin: 10px;
        background: #2d2d2d;
        color: #ffffff;
        text-decoration: none;
        border-radius: 8px;
        font-size: 16px;
        font-weight: bold;
        border: 2px solid #4a90d9;
        transition: all 0.3s;
    }
    .notebook-links a:hover {
        background: #4a90d9;
        transform: scale(1.05);
    }
    </style>

    <div class="notebook-links">
        <a href="http://localhost:3838/streaming/realtime_orders.py" target="_blank">
            📊 Streaming - Analyse Temps Reel
        </a>
        <br>
        <a href="http://localhost:3838/analytics/sales_analytics.py" target="_blank">
            📈 Analytics - Analyse des Ventes
        </a>
        <br>
        <a href="http://localhost:3838/batch/batch_analytics.py" target="_blank">
            📦 Batch - Analyse des Donnees Batch
        </a>
    </div>
    """)
    return


if __name__ == "__main__":
    app.run()
