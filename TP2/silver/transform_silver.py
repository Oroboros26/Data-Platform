"""
BCE/KBO Medallion Architecture — Silver Layer
Bronze (Parquet) → Silver (Parquet jointé, nettoyé, enrichi)

Optimisations mémoire :
  - broadcast() sur les petites tables (code 21K, dénoms dédupliquées)
  - cache() sur les intermédiaires réutilisés
  - unpersist() après usage pour libérer la mémoire
  - pas de count() inutile au chargement
  - shuffle.partitions=20 pour réduire le fan-out
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, row_number, when, first, lit,
    coalesce, current_timestamp, broadcast
)
from pyspark.sql.window import Window
import os

# ── Configuration ─────────────────────────────────────────────────────────────
HDFS_URL    = os.getenv("HDFS_URL", "hdfs://namenode:9000")
BRONZE_PATH = f"{HDFS_URL}/datalake/bronze"
SILVER_PATH = f"{HDFS_URL}/datalake/silver"

# ── Session Spark ──────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("BCE_Silver_Transform") \
    .config("spark.hadoop.fs.defaultFS", HDFS_URL) \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("spark.sql.autoBroadcastJoinThreshold", "50mb") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "3g") \
    .config("spark.memory.fraction", "0.8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  BCE/KBO — SILVER LAYER — Jointures & Enrichissement")
print("=" * 60)
print(f"  Source : {BRONZE_PATH}")
print(f"  Dest   : {SILVER_PATH}")
print()

# ── 0. Chargement des tables Bronze ───────────────────────────────────────────
print("📂 Chargement des tables Bronze...")

enterprise    = spark.read.parquet(f"{BRONZE_PATH}/enterprise")
denomination  = spark.read.parquet(f"{BRONZE_PATH}/denomination")
address       = spark.read.parquet(f"{BRONZE_PATH}/address")
activity      = spark.read.parquet(f"{BRONZE_PATH}/activity")
contact       = spark.read.parquet(f"{BRONZE_PATH}/contact")
establishment = spark.read.parquet(f"{BRONZE_PATH}/establishment")
branch        = spark.read.parquet(f"{BRONZE_PATH}/branch")
code          = spark.read.parquet(f"{BRONZE_PATH}/code")

# code.csv est petit (21K lignes) — on le broadcast pour éviter les shuffles
code.cache()
print("  ✅ Tables chargées (code en cache)")
print()


# ── Fonctions utilitaires ──────────────────────────────────────────────────────

def code_lookup_broadcast(category, code_col_in, label_col_out, lang="1"):
    """Retourne un broadcast DataFrame filtré du référentiel code.csv."""
    return broadcast(
        code.filter(
            (col("Category") == category) & (col("Language") == lang)
        ).select(
            col("Code").alias(code_col_in),
            col("Description").alias(label_col_out)
        )
    )

def window_first(partition_col, order_expr):
    return Window.partitionBy(partition_col).orderBy(order_expr)


# ── 1. Dénomination principale ────────────────────────────────────────────────
print("🔧 [1/8] Dénominations principales...")

denom_main = denomination.filter(col("TypeOfDenomination") == "001") \
    .select("EntityNumber", "Language", "Denomination")

w_denom = window_first(
    "EntityNumber",
    when(col("Language") == "1", 0).when(col("Language") == "2", 1).otherwise(2)
)

denom_primary = denom_main \
    .withColumn("_rn", row_number().over(w_denom)) \
    .filter(col("_rn") == 1) \
    .select(
        col("EntityNumber").alias("_denom_key"),
        col("Denomination").alias("NomPrincipal"),
    ).cache()

denom_commercial = denomination.filter(col("TypeOfDenomination") == "002") \
    .select("EntityNumber", "Language", "Denomination")
w_trade = window_first(
    "EntityNumber",
    when(col("Language") == "1", 0).when(col("Language") == "2", 1).otherwise(2)
)
denom_commercial = denom_commercial \
    .withColumn("_rn", row_number().over(w_trade)) \
    .filter(col("_rn") == 1) \
    .select(
        col("EntityNumber").alias("_denom_comm_key"),
        col("Denomination").alias("NomCommercial"),
    ).cache()

print("  ✅ OK")


# ── 2. Adresse REGO ───────────────────────────────────────────────────────────
print("🔧 [2/8] Adresses REGO...")

address_rego = address.filter(col("TypeOfAddress") == "REGO") \
    .select("EntityNumber", "Zipcode", "MunicipalityFR", "MunicipalityNL",
            "StreetFR", "StreetNL", "HouseNumber", "Box", "CountryFR", "CountryNL")

w_addr = window_first("EntityNumber", lit(1))
address_rego_unique = address_rego \
    .withColumn("_rn", row_number().over(w_addr)) \
    .filter(col("_rn") == 1) \
    .select(
        col("EntityNumber").alias("_addr_key"),
        col("Zipcode").alias("CodePostal"),
        coalesce(col("MunicipalityFR"), col("MunicipalityNL")).alias("Commune"),
        coalesce(col("StreetFR"), col("StreetNL")).alias("Rue"),
        col("HouseNumber").alias("Numero"),
        col("Box").alias("Boite"),
        coalesce(col("CountryFR"), col("CountryNL")).alias("Pays"),
    ).cache()

print("  ✅ OK")


# ── 3. Activité principale NACE ───────────────────────────────────────────────
print("🔧 [3/8] Activités principales NACE...")

activity_main = activity.filter(col("Classification") == "MAIN") \
    .select("EntityNumber", "NaceCode", "NaceVersion", "ActivityGroup")

w_act = window_first("EntityNumber", col("NaceVersion").desc())
activity_main_unique = activity_main \
    .withColumn("_rn", row_number().over(w_act)) \
    .filter(col("_rn") == 1) \
    .select(
        col("EntityNumber").alias("_act_key"),
        col("NaceCode"),
        col("NaceVersion"),
    )

# Description NACE — petite table broadcast
nace_desc = code.filter(
    col("Category").isin(["Nace2003", "Nace2008", "Nace2025"]) &
    (col("Language") == "1")
).select(col("Code").alias("NaceCode"), col("Description").alias("DescriptionNace"))

w_nace = window_first("NaceCode", lit(1))
nace_desc_unique = broadcast(
    nace_desc
    .withColumn("_rn", row_number().over(w_nace))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

activity_main_enriched = activity_main_unique.join(nace_desc_unique, "NaceCode", "left").cache()

print("  ✅ OK")


# ── 4. Contacts ENT (pivot) ───────────────────────────────────────────────────
print("🔧 [4/8] Pivot contacts...")

contact_pivot = contact.filter(col("EntityContact") == "ENT") \
    .select("EntityNumber", "ContactType", "Value") \
    .groupBy("EntityNumber") \
    .pivot("ContactType", ["TEL", "EMAIL", "WEB", "FAX"]) \
    .agg(first("Value")) \
    .select(
        col("EntityNumber").alias("_contact_key"),
        col("TEL"), col("EMAIL"), col("WEB"), col("FAX"),
    ).cache()

print("  ✅ OK")


# ── 5. Lookups codes (tous broadcast) ────────────────────────────────────────
print("🔧 [5/8] Lookups codes (broadcast)...")

form_lkp    = code_lookup_broadcast("JuridicalForm",      "JuridicalForm",      "FormeJuridique")
status_lkp  = code_lookup_broadcast("Status",             "Status",             "StatutLibelle")
type_lkp    = code_lookup_broadcast("TypeOfEnterprise",   "TypeOfEnterprise",   "TypeEntreprise")
sitjur_lkp  = code_lookup_broadcast("JuridicalSituation", "JuridicalSituation", "SituationJuridique")

print("  ✅ OK")


# ── 6. SILVER TABLE 1 : enterprise_profile ────────────────────────────────────
print("\n🔨 [6/8] enterprise_profile...")

enterprise_clean = enterprise.select(
    "EnterpriseNumber", "Status", "JuridicalSituation",
    "TypeOfEnterprise", "JuridicalForm", "StartDate"
).repartition(20, "EnterpriseNumber")

enterprise_profile = enterprise_clean \
    .join(denom_primary,
          enterprise_clean.EnterpriseNumber == denom_primary._denom_key, "left") \
    .join(denom_commercial,
          enterprise_clean.EnterpriseNumber == denom_commercial._denom_comm_key, "left") \
    .join(address_rego_unique,
          enterprise_clean.EnterpriseNumber == address_rego_unique._addr_key, "left") \
    .join(activity_main_enriched,
          enterprise_clean.EnterpriseNumber == activity_main_enriched._act_key, "left") \
    .join(contact_pivot,
          enterprise_clean.EnterpriseNumber == contact_pivot._contact_key, "left") \
    .join(form_lkp,   "JuridicalForm",        "left") \
    .join(status_lkp, "Status",               "left") \
    .join(type_lkp,   "TypeOfEnterprise",      "left") \
    .join(sitjur_lkp, "JuridicalSituation",   "left") \
    .select(
        enterprise_clean.EnterpriseNumber,
        col("NomPrincipal"),
        col("NomCommercial"),
        enterprise_clean.Status,
        col("StatutLibelle"),
        enterprise_clean.JuridicalSituation,
        col("SituationJuridique"),
        enterprise_clean.JuridicalForm,
        col("FormeJuridique"),
        enterprise_clean.TypeOfEnterprise,
        col("TypeEntreprise"),
        enterprise_clean.StartDate,
        col("CodePostal"),
        col("Commune"),
        col("Rue"),
        col("Numero"),
        col("Boite"),
        col("Pays"),
        col("NaceCode"),
        col("NaceVersion"),
        col("DescriptionNace"),
        col("TEL"),
        col("EMAIL"),
        col("WEB"),
        col("FAX"),
        current_timestamp().alias("_silver_loaded_at"),
    )

enterprise_profile.write.mode("overwrite").parquet(f"{SILVER_PATH}/enterprise_profile")
ep_count = enterprise_profile.count()
print(f"  ✅ enterprise_profile : {ep_count:,} lignes")

# Libération mémoire
denom_primary.unpersist()
denom_commercial.unpersist()
address_rego_unique.unpersist()
activity_main_enriched.unpersist()
contact_pivot.unpersist()


# ── 7. SILVER TABLE 2 : establishment_profile ─────────────────────────────────
print("\n🔨 [7/8] establishment_profile...")

estab_addr = address.filter(col("TypeOfAddress") == "REGO") \
    .select(
        col("EntityNumber").alias("_estab_addr_key"),
        col("Zipcode").alias("EstabCodePostal"),
        coalesce(col("MunicipalityFR"), col("MunicipalityNL")).alias("EstabCommune"),
        coalesce(col("StreetFR"), col("StreetNL")).alias("EstabRue"),
        col("HouseNumber").alias("EstabNumero"),
        coalesce(col("CountryFR"), col("CountryNL")).alias("EstabPays"),
    )
w_ea = window_first("_estab_addr_key", lit(1))
estab_addr_unique = estab_addr \
    .withColumn("_rn", row_number().over(w_ea)) \
    .filter(col("_rn") == 1) \
    .drop("_rn")

estab_denom = denomination.filter(col("TypeOfDenomination") == "001") \
    .select("EntityNumber", "Language", "Denomination")
w_ed = window_first(
    "EntityNumber",
    when(col("Language") == "1", 0).when(col("Language") == "2", 1).otherwise(2)
)
estab_denom_primary = estab_denom \
    .withColumn("_rn", row_number().over(w_ed)) \
    .filter(col("_rn") == 1) \
    .select(
        col("EntityNumber").alias("_estab_denom_key"),
        col("Denomination").alias("NomEtablissement"),
    )

# Lecture enterprise_profile depuis HDFS (évite de le recalculer en mémoire)
ep_persisted = spark.read.parquet(f"{SILVER_PATH}/enterprise_profile")
parent_info = broadcast(
    ep_persisted.select(
        col("EnterpriseNumber").alias("_parent_key"),
        col("NomPrincipal").alias("NomEntrepriseParent"),
        col("FormeJuridique").alias("FormeJuridiqueParent"),
    )
)

establishment_profile = establishment.select("EstablishmentNumber", "EnterpriseNumber", "StartDate") \
    .join(estab_addr_unique,
          establishment.EstablishmentNumber == estab_addr_unique._estab_addr_key, "left") \
    .join(estab_denom_primary,
          establishment.EstablishmentNumber == estab_denom_primary._estab_denom_key, "left") \
    .join(parent_info,
          establishment.EnterpriseNumber == parent_info._parent_key, "left") \
    .select(
        establishment.EstablishmentNumber,
        establishment.EnterpriseNumber,
        col("NomEntrepriseParent"),
        col("FormeJuridiqueParent"),
        establishment.StartDate,
        col("NomEtablissement"),
        col("EstabCodePostal"),
        col("EstabCommune"),
        col("EstabRue"),
        col("EstabNumero"),
        col("EstabPays"),
        current_timestamp().alias("_silver_loaded_at"),
    )

establishment_profile.write.mode("overwrite").parquet(f"{SILVER_PATH}/establishment_profile")
ep2_count = establishment_profile.count()
print(f"  ✅ establishment_profile : {ep2_count:,} lignes")


# ── 8a. SILVER TABLE 3 : all_activities ──────────────────────────────────────
print("\n🔨 [8a/8] all_activities...")

all_activities = activity.select("EntityNumber", "ActivityGroup", "NaceVersion", "NaceCode", "Classification") \
    .join(nace_desc_unique, "NaceCode", "left") \
    .join(
        broadcast(ep_persisted.select("EnterpriseNumber", "NomPrincipal")),
        activity.EntityNumber == col("EnterpriseNumber"),
        "left"
    ) \
    .select(
        activity.EntityNumber,
        col("NomPrincipal"),
        col("ActivityGroup"),
        col("NaceVersion"),
        col("NaceCode"),
        col("DescriptionNace"),
        col("Classification"),
        current_timestamp().alias("_silver_loaded_at"),
    )

all_activities.write.mode("overwrite").parquet(f"{SILVER_PATH}/all_activities")
aa_count = all_activities.count()
print(f"  ✅ all_activities : {aa_count:,} lignes")


# ── 8b. SILVER TABLE 4 : branch_profile ──────────────────────────────────────
print("\n🔨 [8b/8] branch_profile...")

branch_profile = branch.select("Id", "EnterpriseNumber", "StartDate") \
    .join(
        broadcast(ep_persisted.select(
            col("EnterpriseNumber").alias("_bp_key"),
            col("NomPrincipal").alias("NomEntrepriseParent"),
            col("FormeJuridique").alias("FormeJuridiqueParent"),
        )),
        branch.EnterpriseNumber == col("_bp_key"),
        "left"
    ) \
    .select(
        branch.Id.alias("BranchId"),
        branch.EnterpriseNumber,
        col("NomEntrepriseParent"),
        col("FormeJuridiqueParent"),
        branch.StartDate,
        current_timestamp().alias("_silver_loaded_at"),
    )

branch_profile.write.mode("overwrite").parquet(f"{SILVER_PATH}/branch_profile")
bp_count = branch_profile.count()
print(f"  ✅ branch_profile : {bp_count:,} lignes")

code.unpersist()

# ── Résumé ────────────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("  SILVER LAYER — Résumé")
print("=" * 60)
print(f"  enterprise_profile   : {ep_count:>10,} lignes")
print(f"  establishment_profile: {ep2_count:>10,} lignes")
print(f"  all_activities       : {aa_count:>10,} lignes")
print(f"  branch_profile       : {bp_count:>10,} lignes")
print()
print("🎉 Silver transformation terminée.")
spark.stop()
