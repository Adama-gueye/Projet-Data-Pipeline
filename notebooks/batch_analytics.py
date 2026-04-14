import marimo

__generated_with = "0.23.1"
app = marimo.App()


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import os
    import json

    DATA_PATH = "/opt/airflow/data/external"
    return mo, pd, json, DATA_PATH


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # TP Batch - Analyse des Donnees E-commerce

    Ce notebook analyse les donnees batch du pipeline ETL.

    ## Objectifs
    1. Charger les donnees depuis les fichiers CSV
    2. Analyser l'inventaire
    3. Analyser les performances fournisseurs
    4. Analyser les tickets de support

    ## Configuration
    - Host: postgres
    - Database: airflow
    - Data Path: /opt/airflow/data/external
    """)
    return


@app.cell
def _(pd, DATA_PATH):
    # Charger inventory
    inventory = pd.read_csv(f"{DATA_PATH}/batch/daily_inventory.csv")
    print(f"Inventory: {len(inventory)} produits")
    return (inventory,)


@app.cell
def _(pd, DATA_PATH):
    # Charger deliveries
    deliveries = pd.read_csv(f"{DATA_PATH}/batch/supplier_deliveries.csv")
    print(f"Deliveries: {len(deliveries)} enregistrements")
    return (deliveries,)


@app.cell
def _(json, DATA_PATH):
    # Charger tickets
    tickets_list = []
    with open(f"{DATA_PATH}/support/support_tickets.jsonl", "r") as f:
        for line in f:
            tickets_list.append(json.loads(line))
    tickets = pd.DataFrame(tickets_list)
    print(f"Tickets: {len(tickets)} enregistrements")
    return (tickets,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## Exercice 2 : Analyse de l'Inventaire

    Identifiez les produits en rupture de stock.
    """)
    return


@app.cell
def _(inventory):
    low_stock = inventory[inventory["quantity_in_stock"] < inventory["reorder_level"]]
    print(f"PRODUITS EN RUPTURE ({len(low_stock)}):")
    return (low_stock,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## Exercice 3 : Performance des Fournisseurs

    Analysez les delais de livraison par fournisseur.
    """)
    return


@app.cell
def _(deliveries):
    supplier_stats = deliveries.groupby("supplier_name").agg({
        "delivery_days": ["mean", "min", "max"],
        "quantity": "sum"
    }).round(2)
    supplier_stats.columns = ["avg_delivery", "min_delivery", "max_delivery", "total_quantity"]
    print("PERFORMANCE FOURNISSEURS:")
    return (supplier_stats,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## Exercice 4 : Analyse des Tickets

    Analysez les tickets par statut et priorite.
    """)
    return


@app.cell
def _(tickets):
    by_status = tickets.groupby("status").size()
    print("TICKETS PAR STATUT:")
    return (by_status,)


@app.cell
def _(tickets):
    by_priority = tickets.groupby("priority").size()
    print("TICKETS PAR PRIORITE:")
    return (by_priority,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## Commandes Utiles

    ```bash
    # Executer le DAG Airflow manuellement
    docker-compose exec airflow-webserver airflow dags trigger ecommerce_batch_pipeline

    # Verifier les tables dans PostgreSQL
    docker-compose exec postgres psql -U airflow -d airflow -c "\\dt"
    ```
    """)
    return


if __name__ == "__main__":
    app.run()
