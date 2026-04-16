import marimo

__generated_with = "0.23.1"
app = marimo.App()


@app.cell
def _():
    import marimo as mo
    import json
    from confluent_kafka import Producer, Consumer
    from faker import Faker

    return Consumer, Faker, Producer, json, mo


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # TP Streaming - Kafka Producer & Consumer - E-commerce Orders

    Ce notebook permet de produire et consommer des commandes e-commerce en temps reel.

    ## Objectifs
    1. Creer un producer Kafka pour envoyer des commandes
    2. Creer un consumer pour recevoir les commandes

    ## Configuration
    - Broker: kafka:9092
    - Topic: realtime-orders
    """)
    return


@app.cell
def _():
    BOOTSTRAP_SERVERS = "kafka:9092"
    ORDERS_TOPIC = "realtime-orders"
    return BOOTSTRAP_SERVERS, ORDERS_TOPIC


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## Exercice 1 : Generer une Commande

    Utilisez Faker pour generer une fausse commande e-commerce.
    """)
    return


@app.cell
def _(Faker, json):
    def generate_order():
        fake = Faker()
        order = {
            "order_id": fake.uuid4(),
            "customer_id": f"CUST{fake.random_number(digits=6)}",
            "customer_name": fake.name(),
            "product": fake.catch_phrase(),
            "quantity": fake.random_int(min=1, max=5),
            "price": round(fake.random.uniform(10.0, 500.0), 2),
            "timestamp": fake.date_time_this_year().isoformat()
        }
        order["total"] = round(order["price"] * order["quantity"], 2)
        return order

    sample = generate_order()
    print("Exemple de commande:")
    print(json.dumps(sample, indent=2))
    return (generate_order,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## Exercice 2 : Creer le Producer

    Creez un producer Kafka avec confluent-kafka.
    """)
    return


@app.cell
def _(BOOTSTRAP_SERVERS, ORDERS_TOPIC, Producer, generate_order, json):
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "ecommerce-producer"
    })

    def delivery_callback(err, msg):
        if err is not None:
            print(f"Erreur livraison: {err}")
        else:
            print(f"Livre a {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

    def send_order(order):
        producer.produce(
            topic=ORDERS_TOPIC,
            key=order["order_id"],
            value=json.dumps(order).encode("utf-8"),
            callback=delivery_callback
        )
        producer.poll(0)
        producer.flush()

    # Tester l'envoi
    test_order = generate_order()
    print(f"Envoi de la commande {test_order['order_id']}...")
    send_order(test_order)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## Exercice 3 : Creer le Consumer

    Creez un consumer pour lire les commandes du topic.
    """)
    return


@app.cell
def _(BOOTSTRAP_SERVERS, Consumer, ORDERS_TOPIC):
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "ecommerce-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True
    })
    consumer.subscribe([ORDERS_TOPIC])
    print(f"Consumer subscribe au topic: {ORDERS_TOPIC}")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## Commandes Utiles

    ```bash
    # Voir les topics
    docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

    # Voir les messages
    docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic realtime-orders --from-beginning --max-messages 5
    ```
    """)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
