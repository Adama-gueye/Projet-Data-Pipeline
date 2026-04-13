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
    # Streaming - Analyse des Commandes Temps Reel

    Ce notebook permet de :
    1. **Consommer les commandes** depuis Kafka (topic `orders_raw`)
    2. **Detector les anomalies** en temps reel
    3. **Visualiser les statistiques** des commandes

    ## Architecture

    ```
    Producer → Kafka (orders_raw) → Consumer → PostgreSQL (streaming.fact_orders)
                                              ↘
                                               Kafka (orders_anomalies) → Anomaly Consumer
    ```

    ## Configuration

    Verifiez que les services sont demarres :
    - Kafka : localhost:9092
    - Kafbat UI : http://localhost:8090
    - PostgreSQL : localhost:5432
    """)
    return


@app.cell
def _():
    import os
    import json
    import psycopg2
    import pandas as pd
    from datetime import datetime
    from kafka import KafkaConsumer

    # Configuration
    KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    KAFKA_TOPIC_ORDERS = os.getenv('KAFKA_TOPIC_ORDERS', 'orders_raw')

    POSTGRES_CONFIG = {
        'host': os.getenv('POSTGRES_HOST', 'postgres'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'user': os.getenv('POSTGRES_USER', 'de_user'),
        'password': os.getenv('POSTGRES_PASSWORD', 'de_password'),
        'database': os.getenv('POSTGRES_DB', 'warehouse')
    }

    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    print(f"Topic: {KAFKA_TOPIC_ORDERS}")
    print(f"PostgreSQL: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}")
    return KAFKA_BOOTSTRAP, KAFKA_TOPIC_ORDERS, POSTGRES_CONFIG, datetime, json, pd, psycopg2


@app.cell
def _(KAFKA_BOOTSTRAP, KAFKA_TOPIC_ORDERS):
    def create_consumer(group_id='notebook-consumer'):
        """Creer un consumer Kafka"""
        consumer = KafkaConsumer(
            KAFKA_TOPIC_ORDERS,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        return consumer

    def consume_orders(max_events=20, timeout=3.0):
        """Consommer des commandes depuis Kafka"""
        consumer = create_consumer()
        orders = []

        start = datetime.now()
        while len(orders) < max_events:
            if (datetime.now() - start).total_seconds() > timeout * max_events / 10:
                break
            msg = consumer.poll(timeout=timeout)
            for topic_partition, messages in msg.items():
                for message in messages:
                    orders.append({
                        'offset': message.offset,
                        'partition': message.partition,
                        'timestamp': message.timestamp,
                        **message.value
                    })

        consumer.close()
        return orders

    print(f"Consumer configure pour le topic: {KAFKA_TOPIC_ORDERS}")
    return consume_orders, create_consumer


@app.cell
def _(consume_orders, mo):
    orders = consume_orders(max_events=30, timeout=5.0)

    mo.md(f"""
    ### Commandes Recuperees

    **Total**: {len(orders)} commandes
    """)
    return (orders,)


@app.cell
def _(mo, orders, pd):
    if orders:
        orders_df = pd.DataFrame(orders)
        mo.ui.table(orders_df)
    else:
        mo.md("Aucune commande recuperee. Verifiez que le producer envoie des messages.")
    return orders_df


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 🚨 Detection d'Anomalies
    """)
    return


@app.cell
def _(mo):
    def detect_anomalies(event):
        """Detecter les anomalies selon les regles du projet"""
        anomalies = []
        if event.get('quantity', 0) >= 8:
            anomalies.append('high_quantity')
        if event.get('unit_price', 0) >= 2500:
            anomalies.append('price_spike')
        if event.get('stock_after_order', 0) < 0:
            anomalies.append('negative_stock')
        if event.get('total_amount', 0) >= 5000:
            anomalies.append('high_order_value')
        return anomalies

    mo.md("""
    **Regles d'anomalies** :
    - `high_quantity`: Quantite >= 8
    - `price_spike`: Prix unitaire >= 2500
    - `negative_stock`: Stock negatif
    - `high_order_value`: Montant total >= 5000
    """)
    return (detect_anomalies,)


@app.cell
def _(detect_anomalies, mo, orders, pd):
    if orders:
        for order in orders:
            order['detected_anomalies'] = detect_anomalies(order)

        anomalous = [o for o in orders if o.get('detected_anomalies')]
        normal = [o for o in orders if not o.get('detected_anomalies')]

        mo.md(f"""
        ### Resultats Detection

        **Commandes normales**: {len(normal)}
        **Commandes anomalnes**: {len(anomalous)}
        """)
    return anomalous, normal


@app.cell
def _(anomalous, mo, orders_df):
    if anomalous:
        anomalous_df = pd.DataFrame(anomalous)
        mo.ui.table(anomalous_df[['order_id', 'quantity', 'unit_price', 'total_amount', 'detected_anomalies']])
    else:
        mo.md("Aucune anomalie detectee dans les commandes actuelles.")
    return anomalous_df


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 📊 Statistiques
    """)
    return


@app.cell
def _(mo, orders, orders_df, pd):
    if orders:
        stats = {
            'Total commandes': len(orders),
            'Revenu total': sum(o.get('total_amount', 0) for o in orders),
            'Quantite totale articles': sum(o.get('quantity', 0) for o in orders),
            'Prix moyen': pd.DataFrame(orders)['unit_price'].mean() if orders else 0,
            'Commande la plus haute': max(o.get('total_amount', 0) for o in orders) if orders else 0,
        }

        stats_df = pd.DataFrame([stats]).T
        stats_df.columns = ['Valeur']
        mo.ui.table(stats_df)
    return stats


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 🔗 Liens Utiles

    - **Kafbat UI**: http://localhost:8090 (visualiser les topics Kafka)
    - **Topic orders_raw**: Commandes e-commerce
    - **Topic orders_anomalies**: Commandes detectees comme anomalnes
    """)
    return


if __name__ == "__main__":
    app.run()
