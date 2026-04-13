# Projet Data Pipelines DSIA - Cas e-commerce

Ce dossier contient une proposition complete de rendu pour l'examen Data Pipelines DSIA, basee sur une activite `e-commerce`. L'objectif est de construire une plateforme capable d'ingerer des flux temps reel, d'orchestrer des traitements batch quotidiens, de detecter des anomalies et d'exposer les donnees de facon exploitable.

## 1. Objectif du projet

Nous jouons le role de `Data Engineer` dans une entreprise e-commerce. La plateforme doit repondre aux besoins suivants :

- ingerer des commandes en temps reel ;
- charger des fichiers batch quotidiens ;
- detecter les anomalies sur le flux temps reel et rediriger les evenements suspects vers un topic dedie ;
- exposer les donnees dans PostgreSQL et MinIO pour des usages analytiques.

## 2. Sources de donnees

1. `Source temps reel` : flux de commandes e-commerce simule en continu, publie dans Kafka sur le topic `orders_raw`.
2. `Source batch` : snapshot quotidien des stocks (`daily_inventory.csv`).
3. `Source batch` : livraisons fournisseurs (`supplier_deliveries.csv`).
4. `Source batch` : tickets support client (`support_tickets.jsonl`).
5. `Source reference` : catalogue produits (`products.csv`).

## 3. Architecture

```
                         ┌──────────────────┐
                         │   Kafka          │
    Producer ──────────► │  orders_raw      │
    (Python)             │                  │
                         │  orders_anomalies│◄──────── Anomaly Consumer
                         └────────┬─────────┘
                                  │
                         ┌────────▼─────────┐     ┌─────────────┐
                         │  Realtime        │────►│  PostgreSQL │
                         │  Consumer        │     │ streaming.* │
                         └──────────────────┘     └─────────────┘

    ┌───────────────────────────────────────────────────────────────┐
    │                     Airflow DAG                               │
    │  (ecommerce_batch_pipeline - quotidien a minuit)             │
    └───────────────────────────────────────────────────────────────┘
                                  │
                    ┌─────────────┼─────────────┐
                    ▼             ▼             ▼
               ┌────────┐   ┌────────┐   ┌────────┐
               │ MinIO  │   │ staging │   │analytics│
               │ raw/   │   │  .*    │   │   .*   │
               └────────┘   └────────┘   └────────┘
```

## 4. Stack technique

| Composant | Version | Role |
|-----------|---------|------|
| Docker Compose | latest | Orchestration de la stack |
| Kafka + Zookeeper | 7.5.0 | Ingestion temps reel |
| Kafbat UI | latest | Visualisation Kafka |
| PostgreSQL | 16-alpine | Entrepot de donnees |
| MinIO | RELEASE.2025-02 | Object storage / Data lake |
| Airflow | 2.9 | Orchestration batch |
| Marimo | 0.23 | Notebooks interactifs |
| Python | 3.11 | Services, producers, consumers |

## 5. Services disponibles

| Service | URL | Compte |
|---------|-----|--------|
| Kafbat UI | http://localhost:8090 | - |
| Marimo Notebooks | http://localhost:3838 | - |
| Airflow | http://localhost:8080 | airflow / airflow |
| PostgreSQL | localhost:5432 | de_user / de_password |
| MinIO API | http://localhost:9000 | minio / minio123 |
| MinIO Console | http://localhost:9001 | minio / minio123 |

## 6. Demarrage rapide

```bash
# Depuis le dossier exam/
docker compose up --build
```

Attendre que tous les services soienthealthy (environ 1-2 minutes).

## 7. Arret et nettoyage

```bash
# Arreter les services
docker compose down

# Arreter et supprimer les volumes (reinitialisation complete)
docker compose down -v
```

## 8. Schemas PostgreSQL

### Schema `streaming`
- `fact_orders` - Commandes temps reel avec flag anomalie
- `fact_order_anomalies` - Anomalies detectees avec severite et payload

### Schema `staging`
- `inventory_snapshot` - Snapshot quotidien des stocks
- `supplier_deliveries` - Livraisons fournisseurs
- `support_tickets` - Tickets support client

### Schema `analytics`
- `daily_inventory_health` - Sante des stocks (healthy/watch/reorder)
- `daily_support_kpis` - KPIs support par canal et date
- `v_orders_summary` - Vue resumee des commandes
- `v_anomaly_monitoring` - Vue surveillance des anomalies

## 9. Regles de detection d'anomalies

| Regle | Condition | Topic Kafka |
|-------|-----------|-------------|
| high_quantity | quantity >= 8 | orders_anomalies |
| price_spike | unit_price >= 2500 | orders_anomalies |
| negative_stock | stock_after_order < 0 | orders_anomalies |
| high_order_value | total_amount >= 5000 | orders_anomalies |

## 10. Structure du projet

```
exam/
|-- airflow/dags/
|   `-- ecommerce_batch_pipeline.py   # DAG batch quotidien
|-- data/
|   |-- batch/                        # Fichiers CSV pour Airflow
|   |-- reference/                    # Catalogue produits
|   `-- support/                      # Tickets JSONL
|-- notebooks/                        # Notebooks Marimo
|   |-- ecommerce_dashboard.py        # Dashboard central
|   |-- streaming/realtime_orders.py  # Analyse streaming
|   |-- analytics/sales_analytics.py  # Analyse analytique
|   `-- batch/batch_analytics.py      # Analyse batch
|-- postgres/
|   `-- init.sql                      # Schema et donnees initiales
|-- services/
|   |-- shared/                       # Config et database helper
|   `-- streaming/
|       |-- realtime_orders_producer.py   # Producteur Kafka
|       |-- realtime_orders_consumer.py   # Consommateur + detection
|       `-- anomaly_consumer.py           # Consommateur anomalies
|-- sql/
|   `-- sample_queries.sql            # Requetes analytiques
|-- docker-compose.yml
|-- Dockerfile.app                   # Pour producer/consumer/marimo
|-- Dockerfile.airflow               # Pour Airflow
|-- requirements.app.txt
|-- requirements.airflow.txt
`-- README.md
```

## 11. Execution du DAG Airflow

Le DAG `ecommerce_batch_pipeline` s'execute automatiquement chaque jour a minuit.

Pour le declencher manuellement :
1. Ouvrir http://localhost:8080
2. Se connecter avec `airflow / airflow`
3. Cliquer sur le DAG `ecommerce_batch_pipeline`
4. Cliquer sur "Trigger DAG"

## 12. Consultation des logs

```bash
# Logs Airflow
docker compose logs airflow-webserver
docker compose logs airflow-scheduler

# Logs Kafka
docker compose logs kafka

# Logs producers/consumers
docker compose logs realtime-producer
docker compose logs realtime-consumer
docker compose logs anomaly-consumer

# Logs Marimo
docker compose logs marimo-notebook
```

## 13. Bases de donnees initiales

Le fichier `postgres/init.sql` cree automatiquement :
- Les schemas `streaming`, `staging`, `analytics`
- Les tables et vues analytiques
- Des donnees de demonstration (produits, stocks initiaux)

## 14. Points d'attention pour l'evaluation

- **3 sources minimum** : 5 sources implementees (3 batch + 1 streaming + 1 reference)
- **Kafka** : producer/consumer operationnels
- **Anomalies** : redirection vers topic dedie `orders_anomalies`
- **Airflow** : DAG fonctionnel avec upload MinIO et chargement PostgreSQL
- **PostgreSQL** : schemas separes avec vues analytiques
- **Documentation** : README complet et code documente
- **Reproductibilite** : projet entierement containerise

## 15. Axes d'amelioration

- Regles d'anomalies basees sur un modele ML plutot que statiques
- Partitionnement historique des tables batch
- Integration dbt pour transformations plus poussees
- Tests unitaires et integration
- Dashboard BI sur les vues analytiques (Metabase, Superset)