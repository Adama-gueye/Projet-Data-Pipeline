# E-commerce Data Intelligence & Analytics - Projet Exam

## Architecture du Projet

Ce projet implémente une plateforme d'analyse e-commerce avec **Architecture Medallion** (Bronze/Silver/Gold) sur MinIO.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    E-COMMERCE DATA PLATFORM                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐              │
│   │   CSV/JSON   │   │   Kafka      │   │   Marimo     │              │
│   │  (Batch)     │   │  (Streaming) │   │ Notebooks    │              │
│   └──────┬───────┘   └──────┬───────┘   └──────────────┘              │
│          │                  │                                            │
│          ▼                  ▼                                            │
│   ┌─────────────────────────────────────┐                               │
│   │         AIRFLOW (Orchestration)      │                               │
│   │   Extract → Load → Bronze → Silver → Gold → Report                  │
│   └─────────────────┬───────────────────┘                               │
│                     │                                                   │
│          ┌──────────┴──────────┐                                        │
│          ▼                     ▼                                        │
│   ┌─────────────┐      ┌─────────────┐                                 │
│   │ PostgreSQL  │      │   MinIO     │                                 │
│   │ (Analytics) │      │ (Lakehouse) │                                 │
│   └─────────────┘      │Bronze/Silver│                                 │
│                        │   /Gold      │                                 │
│                        └─────────────┘                                 │
└─────────────────────────────────────────────────────────────────────────┘
```

## Services Disponibles

| Service | URL | Identifiants | Description |
|---------|-----|-------------|-------------|
| **Airflow** | http://localhost:8081 | airflow/airflow | Orchestration ETL |
| **Marimo** | http://localhost:7860 | - | Notebooks interactifs |
| **Kafka UI** | http://localhost:8080 | - | Interface Kafka |
| **MinIO** | http://localhost:9001 | minioadmin/minioadmin123 | Data Lake |

## Démarrage Rapide

```bash
# 1. Se placer dans le dossier exam
cd exam

# 2. Démarrer tous les services
docker-compose up -d

# 3. Attendre 2-3 minutes puis vérifier
docker-compose ps

# 4. Vérifier les logs airflow-init
docker-compose logs airflow-init

# 5. Accéder à Airflow et déclencher le DAG manuellement
docker-compose exec airflow-webserver airflow dags trigger ecommerce_batch_pipeline
```

## Structure du Projet

```
exam/
├── docker-compose.yml      # Configuration Docker Compose (tous services)
├── Dockerfile             # Image Airflow personnalisée
├── requirements.txt       # Dépendances Python
│
├── dags/                  # DAGs Airflow
│   └── ecommerce_batch_pipeline.py   # Pipeline ETL complet
│
├── notebooks/             # Notebooks Marimo interactifs
│   ├── batch_analytics.py           # Analyse données batch
│   └── realtime_orders.py            # Producer/Consumer Kafka
│
├── data/                  # Données sources
│   ├── batch/
│   │   ├── daily_inventory.csv      # Inventaire produits
│   │   └── supplier_deliveries.csv   # Livraisons fournisseurs
│   └── support/
│       └── support_tickets.jsonl      # Tickets support client
│
├── postgres/
│   └── init.sql          # Schéma base de données
│
└── sql/
    └── sample_queries.sql # Requêtes SQL analytiques
```

## Pipeline ETL - Déroulement

Le DAG `ecommerce_batch_pipeline` exécute les étapes suivantes :

### Étape 1 : Extraction (Tasks en parallèle)
```
extract_inventory   → Lit daily_inventory.csv
extract_deliveries  → Lit supplier_deliveries.csv
extract_tickets     → Lit support_tickets.jsonl
```
Chaque task push ses données via **XCom** pour les partager.

### Étape 2 : Chargement PostgreSQL
```
load_to_postgres → Pull XCom → Charge dans PostgreSQL
```
Les données sont stockées dans les tables :
- `daily_inventory`
- `supplier_deliveries`
- `support_tickets`

### Étape 3 : Architecture Medallion (MinIO)

#### 🥉 Bronze Layer (Données Raw)
```
save_to_bronze → Envoie les données brutes vers MinIO
```
- `ecommerce-bronze/inventory/date=YYYYMMDD_HHMMSS/inventory.csv`
- `ecommerce-bronze/deliveries/date=YYYYMMDD_HHMMSS/deliveries.csv`
- `ecommerce-bronze/tickets/date=YYYYMMDD_HHMMSS/tickets.jsonl`

#### 🥈 Silver Layer (Données Nettoyées)
```
save_to_silver → Lit Bronze → Nettoie (dedup, not null) → Envoie vers Silver
```
- Supprime doublons
- Valide champs requis
- Supprime valeurs nulles

#### 🥇 Gold Layer (Agrégats)
```
save_to_gold → Lit PostgreSQL → Crée agrégats → Envoie vers Gold
```
- Résumé inventaire (produits en rupture)
- Performance fournisseurs (délais moyens)
- Statistiques tickets (par priorité/statut)

### Étape 4 : Rapport Analytique
```
generate_report → Lit PostgreSQL → Affiche rapport dans logs
```

## XCom - Partage de Données entre Tasks

XCom permet aux tasks de partager des données :

```
Task A (push)                 Task B (pull)
─────────────                 ─────────────
df.to_dict()      ──────►     pd.DataFrame(dict)
```

Dans le code :
```python
# Task A - Push
context["ti"].xcom_push(key="data", value=df.to_dict())

# Task B - Pull
data = context["ti"].xcom_pull(task_ids="task_a", key="data")
```

## Architecture Medallion Expliquée

L'architecture Medallion organise les données en couches de qualité croissante :

| Couche | Description | Type de données |
|--------|-------------|-----------------|
| **Bronze** | Données brutes, immutable | CSV, JSON, Parquet raw |
| **Silver** | Données nettoyées, validées | Sans doublons, sans nulls |
| **Gold** | Agrégats business, prêts BI | Summaries, metrics |

### Pourquoi 3 couches ?
1. **Traçabilité** : On garde l'historique des données brutes
2. **Qualité** : Silver nettoie les problèmes de données
3. **Performance** : Gold pré-calcule les agrégats pour les dashboards

## Notebooks Marimo

Les notebooks sont accessibles sur **http://localhost:7860**

### batch_analytics.py
Analyse des données batch du pipeline ETL :
- Chargement des CSV et JSONL
- Analyse inventaire (produits en rupture)
- Performance fournisseurs
- Statistiques tickets

### realtime_orders.py
Exercices guidés pour Kafka :
- Créer un Producer
- Générer des commandes avec Faker
- Envoyer vers Kafka
- Créer un Consumer

## Commandes Utiles

```bash
# Airflow
docker-compose exec airflow-webserver airflow dags list
docker-compose exec airflow-webserver airflow dags trigger ecommerce_batch_pipeline
docker-compose exec airflow-webserver airflow tasks list ecommerce_batch_pipeline

# PostgreSQL
docker-compose exec postgres psql -U airflow -d airflow -c "\dt"

# Kafka
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic realtime-orders --from-beginning

# MinIO (via mc)
docker-compose exec minio-init mc ls local/
docker-compose exec minio-init mc ls local/ecommerce-bronze/
docker-compose exec minio-init mc ls local/ecommerce-gold/
```

## Schéma de la Base PostgreSQL

### Table daily_inventory
| Colonne | Type | Description |
|---------|------|-------------|
| product_id | VARCHAR(50) | ID produit (PK) |
| product_name | VARCHAR(255) | Nom du produit |
| quantity_in_stock | INTEGER | Quantité en stock |
| warehouse_location | VARCHAR(100) | Entrepôt |
| reorder_level | INTEGER | Seuil de réappro |

### Table supplier_deliveries
| Colonne | Type | Description |
|---------|------|-------------|
| delivery_id | VARCHAR(50) | ID livraison (PK) |
| supplier_id | VARCHAR(50) | ID fournisseur |
| supplier_name | VARCHAR(255) | Nom fournisseur |
| product_id | VARCHAR(50) | ID produit |
| quantity | INTEGER | Quantité livrée |
| delivery_days | INTEGER | Jours de livraison |

### Table support_tickets
| Colonne | Type | Description |
|---------|------|-------------|
| ticket_id | VARCHAR(50) | ID ticket (PK) |
| customer_id | VARCHAR(50) | ID client |
| subject | VARCHAR(500) | Sujet |
| status | VARCHAR(50) | open/in_progress/closed |
| priority | VARCHAR(50) | high/medium/low |

## Dépannage

### Erreur "No such file or directory"
Les données doivent être mountées. Vérifier que `./data` est bien présent dans les volumes du docker-compose.yml.

### Airflow init bloque
```bash
# Vérifier que PostgreSQL est healthy
docker-compose ps postgres

# Relancer airflow-init
docker-compose restart airflow-init
```

### MinIO buckets non créés
```bash
# Recréer les buckets manuellement
docker-compose exec minio-init mc mb local/ecommerce-bronze
docker-compose exec minio-init mc mb local/ecommerce-silver
docker-compose exec minio-init mc mb local/ecommerce-gold
```

