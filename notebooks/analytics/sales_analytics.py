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
    # Analytics - Analyse des Ventes et Performance

    Ce notebook permet d'analyser les performances de vente et la qualite des donnees :

    1. **Resume des ventes** par jour, categorie, methode de paiement
    2. **Surveillance des anomalies** - Tendances et patterns
    3. **KPIs operationnels** - Vue d'ensemble du pipeline

    ## Vues Analytiques

    - `analytics.v_orders_summary` - Resume des commandes par dimension
    - `analytics.v_anomaly_monitoring` - Surveillance des anomalies
    - `analytics.daily_inventory_health` - Sante des stocks
    - `analytics.daily_support_kpis` - KPIs support client
    """)
    return


@app.cell
def _():
    import os
    import psycopg2
    import pandas as pd

    POSTGRES_CONFIG = {
        'host': os.getenv('WAREHOUSE_HOST', 'postgres'),
        'port': int(os.getenv('WAREHOUSE_PORT', '5432')),
        'user': os.getenv('WAREHOUSE_USER', 'de_user'),
        'password': os.getenv('WAREHOUSE_PASSWORD', 'de_password'),
        'database': os.getenv('WAREHOUSE_DB', 'warehouse')
    }

    def get_connection():
        return psycopg2.connect(**POSTGRES_CONFIG)

    def query_sql(sql):
        conn = get_connection()
        df = pd.read_sql_query(sql, conn)
        conn.close()
        return df

    print(f"Connected to: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}")
    return (query_sql,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 💰 Resume des Ventes
    """)
    return


@app.cell
def _(mo, query_sql):
    sales_df = query_sql("""
        SELECT
            order_day,
            product_category,
            payment_method,
            total_orders,
            gross_revenue,
            anomalous_orders
        FROM analytics.v_orders_summary
        ORDER BY order_day DESC, gross_revenue DESC
    """)

    total_revenue = sales_df['gross_revenue'].sum() if not sales_df.empty else 0
    total_orders = sales_df['total_orders'].sum() if not sales_df.empty else 0
    anomaly_rate = (sales_df['anomalous_orders'].sum() / total_orders * 100) if total_orders > 0 else 0

    mo.md(f"""
    ### KPIs de Vente

    - **Revenu brut total**: {total_revenue:,.2f}
    - **Total commandes**: {total_orders}
    - **Taux d'anomalies**: {anomaly_rate:.2f}%
    """)
    return (sales_df,)


@app.cell
def _(mo, sales_df):
    mo.ui.table(sales_df.head(20))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## ⚠️ Surveillance des Anomalies
    """)
    return


@app.cell
def _(mo, query_sql):
    anomalies_df = query_sql("""
        SELECT
            detected_at,
            order_id,
            severity,
            reasons
        FROM streaming.fact_order_anomalies
        ORDER BY detected_at DESC
        LIMIT 50
    """)

    total_anomalies = len(anomalies_df)
    high_severity = len(anomalies_df[anomalies_df['severity'] == 'high']) if not anomalies_df.empty else 0

    mo.md(f"""
    ### Anomalies Detectees

    - **Total**: {total_anomalies}
    - **Severity haute**: {high_severity}
    """)
    return (anomalies_df,)


@app.cell
def _(anomalies_df, mo):
    mo.ui.table(anomalies_df)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 📦 Sante des Stocks
    """)
    return


@app.cell
def _(mo, query_sql):
    inventory_df = query_sql("""
        SELECT
            snapshot_date,
            product_id,
            warehouse_code,
            available_stock,
            reorder_threshold,
            stock_status
        FROM analytics.daily_inventory_health
        WHERE stock_status <> 'healthy'
        ORDER BY snapshot_date DESC, available_stock ASC
    """)

    products_to_reorder = len(inventory_df) if not inventory_df.empty else 0

    mo.md(f"""
    ### Produits a Reapprovisionner

    - **Produits en statut non-sain**: {products_to_reorder}
    """)
    return (inventory_df,)


@app.cell
def _(inventory_df, mo):
    mo.ui.table(inventory_df)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 🎫 KPIs Support Client
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

    total_tickets = support_df['total_tickets'].sum() if not support_df.empty else 0
    urgent_tickets = support_df['urgent_tickets'].sum() if not support_df.empty else 0

    mo.md(f"""
    ### KPIs Support

    - **Total tickets**: {total_tickets}
    - **Tickets urgents**: {urgent_tickets}
    """)
    return (support_df,)


@app.cell
def _(mo, support_df):
    mo.ui.table(support_df)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
