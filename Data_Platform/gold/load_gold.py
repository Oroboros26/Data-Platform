"""
BCE/KBO Medallion Architecture — Gold Layer
Silver (Parquet) → Gold (Parquet agrégé + MongoDB documents)

Collections MongoDB produites dans bce_gold :
  • company_directory   — profil complet par entreprise (index sur EnterpriseNumber, NaceCode, Commune)
  • activity_stats      — classement des secteurs d'activité par nombre d'entreprises
  • establishment_stats — entreprises avec le plus d'établissements
  • geo_stats           — densité d'entreprises par code postal / commune
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, collect_list, struct,
    avg, round as spark_round, current_timestamp, lit, desc
)
import pymongo
import os

# ── Configuration ─────────────────────────────────────────────────────────────
HDFS_URL    = os.getenv("HDFS_URL", "hdfs://namenode:9000")
SILVER_PATH = f"{HDFS_URL}/datalake/silver"
GOLD_PATH   = f"{HDFS_URL}/datalake/gold"
MONGO_URI   = os.getenv("MONGO_URI", "mongodb://admin:bce_password@mongodb:27017/")
DB_NAME     = "bce_gold"

# Limite de documents pour l'import MongoDB (évite OOM sur très gros datasets)
MONGO_BATCH_LIMIT = 200_000

# ── Session Spark ──────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("BCE_Gold_Load") \
    .config("spark.hadoop.fs.defaultFS", HDFS_URL) \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  BCE/KBO — GOLD LAYER — Agrégations & MongoDB")
print("=" * 60)
print(f"  Source : {SILVER_PATH}")
print(f"  HDFS   : {GOLD_PATH}")
print(f"  Mongo  : {MONGO_URI}")
print()

# ── 0. Chargement Silver ───────────────────────────────────────────────────────
print("📂 Chargement des tables Silver...")

ep  = spark.read.parquet(f"{SILVER_PATH}/enterprise_profile")
sp  = spark.read.parquet(f"{SILVER_PATH}/establishment_profile")
act = spark.read.parquet(f"{SILVER_PATH}/all_activities")

print(f"  ✅ enterprise_profile   : {ep.count():>10,}")
print(f"  ✅ establishment_profile: {sp.count():>10,}")
print(f"  ✅ all_activities       : {act.count():>10,}")
print()


# ── GOLD TABLE 1 : company_directory ──────────────────────────────────────────
print("🥇 [1/4] company_directory — profil enrichi par entreprise...")

estab_counts = sp.groupBy("EnterpriseNumber") \
    .agg(count("EstablishmentNumber").alias("NbEtablissements"))

activity_counts = act.groupBy("EntityNumber") \
    .agg(
        count("NaceCode").alias("NbActivites"),
        countDistinct("NaceCode").alias("NbActivitesUniques")
    )

company_directory = ep \
    .join(estab_counts, "EnterpriseNumber", "left") \
    .join(activity_counts, ep.EnterpriseNumber == activity_counts.EntityNumber, "left") \
    .drop("EntityNumber", "_silver_loaded_at") \
    .withColumn("_gold_loaded_at", current_timestamp())

company_directory.write.mode("overwrite").parquet(f"{GOLD_PATH}/company_directory")
cd_count = company_directory.count()
print(f"  ✅ {cd_count:,} lignes  →  {GOLD_PATH}/company_directory")


# ── GOLD TABLE 2 : activity_stats ─────────────────────────────────────────────
print("🥇 [2/4] activity_stats — classement secteurs d'activité...")

activity_stats = act.groupBy("NaceCode", "DescriptionNace") \
    .agg(
        countDistinct("EntityNumber").alias("NbEntreprises"),
        count("EntityNumber").alias("NbOccurrences"),
    ) \
    .orderBy(desc("NbEntreprises")) \
    .withColumn("_gold_loaded_at", current_timestamp())

activity_stats.write.mode("overwrite").parquet(f"{GOLD_PATH}/activity_stats")
as_count = activity_stats.count()
print(f"  ✅ {as_count:,} lignes  →  {GOLD_PATH}/activity_stats")


# ── GOLD TABLE 3 : establishment_stats ────────────────────────────────────────
print("🥇 [3/4] establishment_stats — top entreprises multi-établissements...")

establishment_stats = sp.groupBy("EnterpriseNumber", "NomEntrepriseParent") \
    .agg(
        count("EstablishmentNumber").alias("NbEtablissements"),
        collect_list(
            struct("EstablishmentNumber", "EstabCodePostal", "EstabCommune", "EstabRue")
        ).alias("ListeEtablissements"),
    ) \
    .orderBy(desc("NbEtablissements")) \
    .withColumn("_gold_loaded_at", current_timestamp())

establishment_stats.write.mode("overwrite").parquet(f"{GOLD_PATH}/establishment_stats")
es_count = establishment_stats.count()
print(f"  ✅ {es_count:,} lignes  →  {GOLD_PATH}/establishment_stats")


# ── GOLD TABLE 4 : geo_stats ──────────────────────────────────────────────────
print("🥇 [4/4] geo_stats — densité géographique par commune...")

geo_stats = ep.filter(col("CodePostal").isNotNull()) \
    .groupBy("CodePostal", "Commune") \
    .agg(
        countDistinct("EnterpriseNumber").alias("NbEntreprises"),
        countDistinct("NaceCode").alias("NbSecteurs"),
    ) \
    .orderBy(desc("NbEntreprises")) \
    .withColumn("_gold_loaded_at", current_timestamp())

geo_stats.write.mode("overwrite").parquet(f"{GOLD_PATH}/geo_stats")
gs_count = geo_stats.count()
print(f"  ✅ {gs_count:,} lignes  →  {GOLD_PATH}/geo_stats")


# ── Chargement MongoDB ────────────────────────────────────────────────────────
print()
print("📤 Chargement vers MongoDB (bce_gold)...")

def load_to_mongo(df, collection_name, indexes=None, limit=None):
    """Charge un DataFrame Spark vers MongoDB via pandas."""
    target_df = df.limit(limit) if limit else df
    docs = target_df.toPandas().fillna("").to_dict(orient="records")

    db = mongo_client[DB_NAME]
    db[collection_name].drop()

    if docs:
        # Insertion par batch de 10 000
        batch_size = 10_000
        for i in range(0, len(docs), batch_size):
            db[collection_name].insert_many(docs[i:i + batch_size], ordered=False)

    if indexes:
        for idx in indexes:
            db[collection_name].create_index(idx)

    print(f"  ✅ bce_gold.{collection_name:<25} {len(docs):>8,} documents insérés")
    return len(docs)


mongo_client = pymongo.MongoClient(MONGO_URI)

try:
    # company_directory : limité à MONGO_BATCH_LIMIT pour éviter OOM
    load_to_mongo(
        company_directory.orderBy("EnterpriseNumber"),
        "company_directory",
        indexes=[
            [("EnterpriseNumber", pymongo.ASCENDING)],
            [("NaceCode", pymongo.ASCENDING)],
            [("Commune", pymongo.ASCENDING)],
            [("Status", pymongo.ASCENDING)],
        ],
        limit=MONGO_BATCH_LIMIT,
    )

    # activity_stats : petit dataset, tout charger
    load_to_mongo(
        activity_stats,
        "activity_stats",
        indexes=[
            [("NbEntreprises", pymongo.DESCENDING)],
            [("NaceCode", pymongo.ASCENDING)],
        ],
    )

    # establishment_stats : top 50 000 entreprises multi-établissements
    load_to_mongo(
        establishment_stats,
        "establishment_stats",
        indexes=[
            [("EnterpriseNumber", pymongo.ASCENDING)],
            [("NbEtablissements", pymongo.DESCENDING)],
        ],
        limit=50_000,
    )

    # geo_stats : complet (peu de lignes)
    load_to_mongo(
        geo_stats,
        "geo_stats",
        indexes=[
            [("CodePostal", pymongo.ASCENDING)],
            [("NbEntreprises", pymongo.DESCENDING)],
        ],
    )

    print()
    print("  Indices MongoDB créés.")
    print("  Accès UI : http://localhost:8082 (Mongo Express)")

finally:
    mongo_client.close()


# ── Résumé ────────────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("  GOLD LAYER — Résumé")
print("=" * 60)
print(f"  company_directory   : {cd_count:>10,} lignes")
print(f"  activity_stats      : {as_count:>10,} lignes")
print(f"  establishment_stats : {es_count:>10,} lignes")
print(f"  geo_stats           : {gs_count:>10,} lignes")
print()
print("🎉 Gold layer terminé — HDFS + MongoDB chargés.")
spark.stop()
