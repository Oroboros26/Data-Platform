# Plateforme de Données BCE/KBO — Architecture Medallion

## Vue d'ensemble

Ce projet implémente une **architecture Medallion** (Bronze → Silver → Gold) sur les données ouvertes du **Registre Belge des Entreprises (BCE/KBO)**, snapshot du 27-06-2026.

L'infrastructure repose sur :
- **Apache Hadoop HDFS** — stockage distribué des couches Bronze et Silver (Parquet)
- **Apache Spark (PySpark)** — transformation et jointure des données
- **MongoDB** — stockage de la couche Gold (documents JSON, prête à requêter)
- **Docker Compose** — orchestration complète de la stack

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DONNÉES SOURCE                                │
│   enterprise  denomination  address  activity  contact          │
│   establishment  branch  code  (9 CSV, ~7,5M lignes)           │
└──────────────────────────┬──────────────────────────────────────┘
                           │ spark-submit ingest_bronze.py
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  🟤  COUCHE BRONZE  (HDFS /datalake/bronze/*)                   │
│  CSV bruts → Parquet  |  normalisation des clés BCE             │
│  Tracabilité : _bronze_source + _bronze_loaded_at               │
└──────────────────────────┬──────────────────────────────────────┘
                           │ spark-submit transform_silver.py
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  ⚪  COUCHE SILVER  (HDFS /datalake/silver/*)                   │
│  enterprise_profile     — 1 ligne par entreprise, enrichie      │
│  establishment_profile  — 1 ligne par établissement             │
│  all_activities         — toutes activités NACE                 │
│  branch_profile         — unités légales branches               │
└──────────────────────────┬──────────────────────────────────────┘
                           │ spark-submit load_gold.py
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  🟡  COUCHE GOLD  (HDFS /datalake/gold/ + MongoDB bce_gold)     │
│  company_directory   — profil complet enrichi                   │
│  activity_stats      — classement des secteurs NACE             │
│  establishment_stats — multi-établissements                     │
│  geo_stats           — densité géographique                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## Données source

| Fichier             | Clé principale          | Contenu                                   | ~Lignes    |
|---------------------|-------------------------|-------------------------------------------|------------|
| `enterprise.csv`    | `EnterpriseNumber`      | Table centrale : statut, forme juridique  | 1 200 000  |
| `denomination.csv`  | `EntityNumber`          | Noms FR/NL, officiels et commerciaux      | 2 800 000  |
| `address.csv`       | `EntityNumber`          | Adresses siège (REGO) et correspondance   | 2 400 000  |
| `activity.csv`      | `EntityNumber`          | Codes NACE 2003/2008/2025                 | 1 960 000  |
| `contact.csv`       | `EntityNumber`          | TEL, EMAIL, WEB, FAX                      | 706 000    |
| `establishment.csv` | `EstablishmentNumber`   | Unités d'exploitation + entreprise parente| 1 600 000  |
| `branch.csv`        | `Id`                    | Unités légales type branche               | 350 000    |
| `code.csv`          | `Category + Code`       | Référentiel de codes (toutes catégories)  | 21 000     |

> Détail complet des jointures et des règles de déduplication : voir [JOINS.md](JOINS.md)

---

## Structure du projet

```
TP2/
├── docker-compose.yml         ← Stack complète (Hadoop + Spark + MongoDB + Jupyter)
├── config/
│   └── hadoop.env             ← Variables d'environnement Hadoop/YARN
├── bronze/
│   └── ingest_bronze.py       ← CSV → HDFS Parquet (normalisation clés BCE)
├── silver/
│   └── transform_silver.py    ← Jointures + déduplication + enrichissement codes
├── gold/
│   └── load_gold.py           ← Agrégations + chargement MongoDB
├── scripts/
│   ├── init_hdfs.sh           ← Création de la structure /datalake sur HDFS
│   └── run_pipeline.sh        ← Exécution du pipeline complet
├── notebooks/
│   └── BCE_medallion.ipynb    ← Exploration interactive PySpark
├── README.md                  ← Ce fichier
├── JOINS.md                   ← Documentation détaillée des jointures
└── SUIVI.md                   ← Journal de suivi du projet
```

---

## Démarrage rapide

### 1. Prérequis

- Docker Desktop ≥ 4.x avec Docker Compose v2
- 8 Go de RAM alloués à Docker (recommandé : 12 Go)
- Les CSV sont dans `../TP1/data/` (relatif à ce dossier)

### 2. Démarrer la stack

```bash
cd /workspaces/Data-Platform/TP2
docker compose up -d
```

Attendre ~60 secondes le démarrage complet. Vérifier l'état :
```bash
docker compose ps
```

### 3. Exécuter le pipeline

```bash
# Pipeline complet Bronze → Silver → Gold
./scripts/run_pipeline.sh

# Ou étape par étape
./scripts/run_pipeline.sh bronze
./scripts/run_pipeline.sh silver
./scripts/run_pipeline.sh gold
```

### 4. Interfaces d'accès

| Service         | URL                           | Identifiants          |
|-----------------|-------------------------------|------------------------|
| Spark Web UI    | http://localhost:8080         | —                      |
| HDFS NameNode   | http://localhost:9870         | —                      |
| Mongo Express   | http://localhost:8082         | sans authentification  |
| Jupyter Lab     | http://localhost:8888         | token : `bce2026`      |
| MongoDB         | localhost:27017               | admin / bce_password   |

### 5. Arrêter la stack

```bash
docker compose down          # arrêt sans supprimer les volumes
docker compose down -v       # arrêt + suppression des données HDFS et MongoDB
```

---

## Requêtes exemples

### Spark / HDFS (depuis Jupyter ou spark-shell)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BCE_Explore") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# Profil d'une entreprise
ep = spark.read.parquet("hdfs://namenode:9000/datalake/silver/enterprise_profile")
ep.filter(ep.EnterpriseNumber == "0878065378").show(1, truncate=False)

# Top 10 secteurs NACE
act = spark.read.parquet("hdfs://namenode:9000/datalake/gold/activity_stats")
act.orderBy("NbEntreprises", ascending=False).show(10)
```

### MongoDB (depuis mongosh ou Mongo Express)

```javascript
use bce_gold

// Profil Google Belgium
db.company_directory.findOne({ EnterpriseNumber: "0878065378" })

// Top 10 communes par nombre d'entreprises
db.geo_stats.find().sort({ NbEntreprises: -1 }).limit(10).pretty()

// Secteurs avec + de 10 000 entreprises
db.activity_stats.find({ NbEntreprises: { $gt: 10000 } }).sort({ NbEntreprises: -1 })
```

---

## Choix technologiques

| Technologie     | Rôle                          | Justification                                           |
|-----------------|-------------------------------|---------------------------------------------------------|
| HDFS            | Stockage distribué Bronze/Silver | Scalabilité horizontale, tolérance aux pannes, format natif Spark |
| Parquet         | Format de fichier             | Compression columnar, pushdown predicates, lecture rapide |
| Apache Spark    | Moteur de traitement          | Traitement en mémoire, API DataFrame, intégration HDFS native |
| MongoDB         | Gold layer document store     | Flexibilité du schéma, indexation, requêtes riches, API REST facile |
| Docker Compose  | Orchestration locale          | Reproductibilité totale, isolation des services          |

---

## Sources de données

- **BCE/KBO Open Data** : [economie.fgov.be](https://economie.fgov.be/fr/themes/entreprises/banque-carrefour-des/donnees-ouvertes-de-la-bce)
- **Codes NACE** : [statbel.fgov.be](https://statbel.fgov.be/fr/themes/emploi-et-conditions-de-travail/structure-et-distribution-des-salaires/nace-bel)
- **Documentation format BCE** : Fichier `meta.csv` inclus dans le dataset
