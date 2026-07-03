"""
BCE/KBO Medallion Architecture — Bronze Layer
Ingestion des CSV bruts → HDFS (Parquet)

Chaque CSV est chargé tel quel (dtype=str) avec normalisation
minimale des clés (suppression des points dans les numéros).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, trim, lit, current_timestamp
import sys
import os

# ── Configuration ─────────────────────────────────────────────────────────────
HDFS_URL    = os.getenv("HDFS_URL", "hdfs://namenode:9000")
DATA_PATH   = os.getenv("DATA_PATH", "file:///data/")   # file:// = disque local du conteneur
BRONZE_PATH = f"{HDFS_URL}/datalake/bronze"

# Colonnes ID à normaliser (suppression des points séparateurs BCE)
ID_COLUMNS = ["EnterpriseNumber", "EntityNumber", "EstablishmentNumber", "Id"]

# Tables à ingérer avec leurs colonnes ID
TABLES = {
    "enterprise":    ["EnterpriseNumber"],
    "denomination":  ["EntityNumber"],
    "address":       ["EntityNumber"],
    "activity":      ["EntityNumber"],
    "contact":       ["EntityNumber"],
    "establishment": ["EstablishmentNumber", "EnterpriseNumber"],
    "branch":        ["EnterpriseNumber"],
    "code":          [],
    "meta":          [],
}

# ── Session Spark ──────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("BCE_Bronze_Ingestion") \
    .config("spark.hadoop.fs.defaultFS", HDFS_URL) \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  BCE/KBO — BRONZE LAYER — Ingestion CSV → HDFS Parquet")
print("=" * 60)
print(f"  Source    : {DATA_PATH}")
print(f"  HDFS dest : {BRONZE_PATH}")
print()

total_rows = 0
report = []

for table_name, id_cols in TABLES.items():
    src = f"{DATA_PATH}{table_name}.csv"
    dst = f"{BRONZE_PATH}/{table_name}"

    print(f"📥 {table_name} ...", end=" ", flush=True)

    try:
        df = spark.read.csv(
            src,
            header=True,
            inferSchema=False,
            encoding="UTF-8",
            quote='"',
            escape='"',
        )

        # Normalisation des colonnes ID : strip dots + whitespace
        for id_col in id_cols:
            if id_col in df.columns:
                df = df.withColumn(
                    id_col,
                    regexp_replace(trim(col(id_col)), r"[.\s]", "")
                )

        # Ajout métadonnées de traçabilité
        df = df \
            .withColumn("_bronze_source", lit(f"{table_name}.csv")) \
            .withColumn("_bronze_loaded_at", current_timestamp())

        row_count = df.count()
        total_rows += row_count

        df.write \
            .mode("overwrite") \
            .parquet(dst)

        print(f"✅  {row_count:>12,} lignes  →  {dst}")
        report.append((table_name, row_count, "OK"))

    except Exception as e:
        print(f"❌  ERREUR: {e}")
        report.append((table_name, 0, str(e)))

print()
print("=" * 60)
print(f"  TOTAL : {total_rows:,} lignes ingérées")
print()
print("  Rapport par table:")
for name, cnt, status in report:
    marker = "✅" if status == "OK" else "❌"
    print(f"    {marker}  {name:<20} {cnt:>10,} lignes")

print()
print("🎉 Bronze ingestion terminée.")
spark.stop()
