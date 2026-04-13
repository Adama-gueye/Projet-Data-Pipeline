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
    # Batch Analytics - Analyse des Donnees Batch

    Ce notebook permet d'analyser les donnees chargees par le DAG Airflow :

    1. **Inventaire** - Sante des stocks par entrepot
    2. **Livraisons** - Statut des livraisons fournisseurs
    3. **Support** - KPIs des tickets par canal

    ## Traitement Batch Quotidien

    Le DAG `ecommerce_batch_pipeline` execute chaque jour a minuit et :
    1. Depose les fichiers sources dans MinIO (bucket `raw`)
    2. Charge les donnees dans les tables `staging.*`
    3. Genere les tables analytiques dans `analytics.*`

    ## Tables Disponibles

    - `staging.inventory_snapshot` - Snapshot quotidien des stocks
    - `staging.supplier_deliveries` - Livraisons fournisseurs
    - `staging.support_tickets` - Tickets support
    - `analytics.daily_inventory_health` - Sante des stocks
    - `analytics.daily_support_kpis` - KPIs support
    """)
    return


@app.cell
def _():
    import os
    import psycopg2
    import pandas as pd

    POSTGRES_CONFIG = {
        'host': os.getenv('WAREHOUSE_HOST', 'postgres'),
        'port': os.getenv('WAREHOUSE_PORT', '5432'),
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

    print(f"PostgreSQL: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
