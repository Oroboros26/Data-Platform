# SUIVI — Plateforme de Données BCE/KBO (TP2)

## Journal de bord

### 2026-07-01 — Initialisation du projet

**Contexte** : Construction d'une architecture Medallion (Bronze/Silver/Gold) sur les données
BCE/KBO utilisées dans TP1. Stack : Hadoop HDFS + Apache Spark + MongoDB + Docker Compose.

---

## État d'avancement

| Tâche                                     | Statut | Date       | Notes                                       |
|-------------------------------------------|--------|------------|---------------------------------------------|
| Analyse des CSV et schéma relationnel     | ✅     | 2026-07-01 | 9 CSV, ~7,5M lignes, 8 clés identifiées     |
| Conception de l'architecture Medallion    | ✅     | 2026-07-01 | 3 couches : Bronze/Silver/Gold              |
| `docker-compose.yml`                      | ✅     | 2026-07-01 | Hadoop + Spark + MongoDB + Jupyter          |
| `config/hadoop.env`                       | ✅     | 2026-07-01 | Variables HDFS/YARN                         |
| `bronze/ingest_bronze.py`                 | ✅     | 2026-07-01 | 9 CSV → HDFS Parquet, normalisation clés    |
| `silver/transform_silver.py`              | ✅     | 2026-07-01 | 4 tables Silver, jointures complètes        |
| `gold/load_gold.py`                       | ✅     | 2026-07-01 | 4 collections MongoDB + Parquet Gold        |
| `scripts/init_hdfs.sh`                    | ✅     | 2026-07-01 | Création structure /datalake                |
| `scripts/run_pipeline.sh`                 | ✅     | 2026-07-01 | Pipeline complet + exécution par étape      |
| `notebooks/BCE_medallion.ipynb`           | ✅     | 2026-07-01 | Exploration interactive PySpark             |
| `README.md`                               | ✅     | 2026-07-01 | Documentation complète en français          |
| `JOINS.md`                                | ✅     | 2026-07-01 | Schéma + règles de jointure détaillées      |

---

## Décisions techniques

### Normalisation des numéros BCE
Les numéros d'entreprise dans les CSV ont le format `0878.065.378` (avec points).
La couche Bronze supprime systématiquement les points → `0878065378` (10 chiffres, zéro initial préservé).
Raison : homogénéité pour les jointures entre tables.

### Gestion des duplicats dans denomination.csv
La même entreprise peut avoir plusieurs entrées (FR + NL + différents types).
Règle : `TypeOfDenomination = '001'` (officiel) + préférence `Language = '1'` (FR).
Implémentée via Window Function `row_number()` partitionné par `EntityNumber`.

### Adresse REGO vs COOR
Seul le type `REGO` (siège social enregistré) est utilisé pour les profils Silver.
Le type `COOR` (correspondance) est ignoré car non systématiquement renseigné.

### EstablishmentNumber = EntityNumber dans address/denomination
Les établissements utilisent leur `EstablishmentNumber` comme `EntityNumber` dans les tables
`address.csv` et `denomination.csv`. Ce n'est pas documenté explicitement dans `meta.csv`
mais confirmé par les données : format `2.000.000.xxx` pour les établissements.

### Limite MongoDB MONGO_BATCH_LIMIT = 200 000
Sur 1,2M d'entreprises, charger tout MongoDB en une passe provoquerait un OOM (`toPandas()`).
La limite est configurable via variable d'environnement.
Alternative recommandée pour production : Spark MongoDB Connector natif.

### Choix de Parquet pour Bronze et Silver
Format columnar compressé. Réduit le stockage de ~70% vs CSV et accélère les jointures
Spark (pushdown predicates). Standard de facto pour les data lakes HDFS.

---

## Structure HDFS après pipeline

```
/datalake/
  bronze/
    enterprise/           ← ~50 MB
    denomination/         ← ~80 MB
    address/              ← ~120 MB
    activity/             ← ~50 MB
    contact/              ← ~20 MB
    establishment/        ← ~30 MB
    branch/               ← ~10 MB
    code/                 ← ~1 MB
    meta/                 ← <1 MB
  silver/
    enterprise_profile/   ← ~200 MB  (table principale jointurée)
    establishment_profile/← ~100 MB
    all_activities/       ← ~60 MB
    branch_profile/       ← ~15 MB
  gold/
    company_directory/    ← ~250 MB
    activity_stats/       ← ~1 MB
    establishment_stats/  ← ~80 MB
    geo_stats/            ← ~1 MB
```

---

## Collections MongoDB (bce_gold)

| Collection             | Documents | Index                                    |
|------------------------|-----------|------------------------------------------|
| `company_directory`    | ≤200 000  | EnterpriseNumber, NaceCode, Commune, Status |
| `activity_stats`       | ~8 000    | NbEntreprises DESC, NaceCode             |
| `establishment_stats`  | ≤50 000   | EnterpriseNumber, NbEtablissements DESC  |
| `geo_stats`            | ~2 500    | CodePostal, NbEntreprises DESC           |

---

## Points d'attention

| Point                           | Impact | Action requise                                    |
|---------------------------------|--------|---------------------------------------------------|
| MONGO_BATCH_LIMIT (200k)        | Moyen  | Remplacer `toPandas()` par Spark MongoDB Connector natif pour production |
| Réplication HDFS dfs.replication=1 | Faible | Normal pour dev local. Passer à 3 en production  |
| Mémoire Docker (8 Go min)       | Élevé  | Spark + Hadoop + MongoDB en simultané = ~6 Go RAM |
| `branch.csv` — rôle à préciser  | Faible | Table `Id` non documentée clairement dans meta.csv |
| Codes NACE multi-versions       | Faible | Versions 2003/2008/2025 coexistent. Silver garde la plus récente |

---

## Commandes utiles de débogage

```bash
# Vérifier les logs Spark
docker logs bce_spark_master

# Accéder au shell HDFS
docker exec -it bce_namenode bash
hdfs dfs -ls /datalake/silver/

# Vérifier MongoDB
docker exec -it bce_mongodb mongosh -u admin -p bce_password
use bce_gold
db.company_directory.countDocuments()

# Redémarrer un service
docker compose restart spark-master

# Voir l'utilisation mémoire
docker stats
```
