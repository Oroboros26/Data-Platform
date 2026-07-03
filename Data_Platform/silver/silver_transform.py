"""
BCE/KBO — Silver Transformation (MongoDB)
enterprise_finale (Bronze) → enterprise_silver

Transformations appliquées :
  1. Normalisation StartDate  DD-MM-YYYY → YYYY-MM-DD
  2. Déduplication activités  même NaceCode+Classification → 1 seul
  3. Adresse unique           TypeOfAddress = REGO uniquement
  4. Dénomination principale  TypeOfDenomination = 001 en premier
  5. Décodage codes → labels  JuridicalForm, Status, NaceCode (FR)

Usage :
  python3 silver/silver_transform.py
"""

import os
import re
import sys
import time
import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

# ── Config ─────────────────────────────────────────────────────────────────────
DATA_DIR       = os.getenv("DATA_DIR",   "/workspaces/Data-Platform/TP2/data")
MONGO_URI      = os.getenv("MONGO_URI",  "mongodb://admin:bce_password@localhost:27017/")
MONGO_DB_SRC   = os.getenv("MONGO_DB_SRC",  "bce_bronze")
MONGO_DB_DST   = os.getenv("MONGO_DB_DST",  "bce_silver")
COLL_SRC       = "enterprise_finale"
COLL_DST       = "enterprise_silver"
BATCH_SIZE     = 5_000


# ── Chargement code.csv en lookup ─────────────────────────────────────────────
def load_codes(data_dir):
    """Retourne deux dicts :
      code_lookup[(Category, Code)] = Description_FR
      status_lookup[Code]           = Description_FR  (Category='Status')
    """
    df = pd.read_csv(f"{data_dir}/code.csv", dtype=str, keep_default_na=False)
    df_fr = df[df["Language"] == "FR"]

    general = {}
    for _, row in df_fr.iterrows():
        general[(row["Category"], row["Code"])] = row["Description"]

    return general


def get_label(codes, category, code, default=""):
    if not code:
        return default
    return codes.get((category, str(code).zfill(3)), codes.get((category, code), default))


# ── Normalisation de date ─────────────────────────────────────────────────────
_DATE_RE = re.compile(r"^(\d{2})-(\d{2})-(\d{4})$")

def normalize_date(s):
    """DD-MM-YYYY → YYYY-MM-DD. Retourne '' si non parseable."""
    if not s:
        return ""
    m = _DATE_RE.match(s.strip())
    if m:
        d, mo, y = m.groups()
        return f"{y}-{mo}-{d}"
    # Déjà au bon format ?
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s.strip()):
        return s.strip()
    return s  # inchangé si format inconnu


# ── Déduplication activités ───────────────────────────────────────────────────
def dedup_activities(activities):
    """Garde un seul item par (NaceCode, Classification).
    Si doublon exact : prend le plus récent NaceVersion.
    """
    seen = {}
    for act in activities:
        key = (act.get("NaceCode", ""), act.get("Classification", ""))
        existing = seen.get(key)
        if existing is None:
            seen[key] = act
        else:
            # Préférer la version NACE la plus récente
            try:
                if int(act.get("NaceVersion", 0)) > int(existing.get("NaceVersion", 0)):
                    seen[key] = act
            except (ValueError, TypeError):
                pass
    return list(seen.values())


# ── Transformation d'un document ─────────────────────────────────────────────
def transform(doc, codes):
    eid = doc["_id"]

    # 1. Normalisation date
    start_date = normalize_date(doc.get("StartDate", ""))

    # 2. Décodage JuridicalForm + Status
    jf_code = doc.get("JuridicalForm", "")
    status  = doc.get("Status", "")

    jf_label     = get_label(codes, "JuridicalForm",     jf_code)
    status_label = get_label(codes, "Status", status)

    # 3. Adresse REGO uniquement
    addresses = [a for a in doc.get("addresses", []) if a.get("TypeOfAddress") == "REGO"]
    address   = addresses[0] if addresses else {}

    # 4. Dénominations : TypeOfDenomination=001 en premier, puis Language=1 (FR)
    denoms = doc.get("denominations", [])
    denoms_sorted = sorted(
        denoms,
        key=lambda d: (
            0 if d.get("TypeOfDenomination") == "001" else 1,
            0 if d.get("Language") == "1" else 1,
        )
    )
    primary_name = denoms_sorted[0]["Denomination"] if denoms_sorted else ""

    # 5. Déduplication + décodage activités
    activities_raw  = doc.get("activities", [])
    activities_dedup = dedup_activities(activities_raw)

    activities_enriched = []
    for act in activities_dedup:
        nace_code = act.get("NaceCode", "")
        nace_ver  = act.get("NaceVersion", "")
        # Essayer d'abord NaceVersion directe, sinon chercher dans Nace2008/Nace2025
        nace_label = (
            codes.get(("Nace" + nace_ver, nace_code), "")
            or codes.get(("NaceCode", nace_code), "")
        )
        activities_enriched.append({**act, "NaceLabel": nace_label})

    # 6. Contacts
    contacts = doc.get("contacts", [])

    return {
        "_id":               eid,
        "EnterpriseNumber":  eid,
        "Status":            status,
        "StatusLabel":       status_label,
        "JuridicalSituation":doc.get("JuridicalSituation", ""),
        "TypeOfEnterprise":  doc.get("TypeOfEnterprise", ""),
        "JuridicalForm":     jf_code,
        "JuridicalFormLabel":jf_label,
        "StartDate":         start_date,
        "PrimaryName":       primary_name,
        "denominations":     denoms_sorted,
        "address":           address,
        "activities":        activities_enriched,
        "contacts":          contacts,
    }


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    print("=" * 60)
    print("  BCE/KBO — SILVER Transformation (MongoDB)")
    print(f"  Source  : {MONGO_DB_SRC}.{COLL_SRC}")
    print(f"  Target  : {MONGO_DB_DST}.{COLL_DST}")
    print("=" * 60)

    client  = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5_000)
    src     = client[MONGO_DB_SRC][COLL_SRC]
    dst_db  = client[MONGO_DB_DST]
    dst     = dst_db[COLL_DST]

    total_src = src.estimated_document_count()
    if total_src == 0:
        print(f"\n❌  {COLL_SRC} est vide — lance d'abord ingest_mongo.py")
        sys.exit(1)
    print(f"\nSource : {total_src:,} documents")

    # Drop et recréer
    if dst.estimated_document_count() > 0:
        print(f"⚠️  {COLL_DST} existante — suppression...")
        dst.drop()

    # Chargement des codes
    print("Chargement code.csv...")
    codes = load_codes(DATA_DIR)
    print(f"  {len(codes):,} entrées codes chargées")

    # Traitement par batches
    print(f"\nTransformation par batches de {BATCH_SIZE:,}...")
    total_written = 0
    batch_n = 0
    batch = []

    cursor = src.find({}, batch_size=BATCH_SIZE)
    for doc in cursor:
        batch.append(transform(doc, codes))

        if len(batch) >= BATCH_SIZE:
            ops = [UpdateOne({"_id": d["_id"]}, {"$setOnInsert": d}, upsert=True) for d in batch]
            try:
                result = dst.bulk_write(ops, ordered=False)
                total_written += result.upserted_count
            except BulkWriteError as bwe:
                total_written += bwe.details.get("nUpserted", 0)
            batch_n += 1
            elapsed = time.time() - t0
            print(f"  Batch {batch_n:>3}  {total_written:>9,}/{total_src:,}  [{elapsed:.0f}s]")
            batch = []

    # Dernier batch
    if batch:
        ops = [UpdateOne({"_id": d["_id"]}, {"$setOnInsert": d}, upsert=True) for d in batch]
        try:
            result = dst.bulk_write(ops, ordered=False)
            total_written += result.upserted_count
        except BulkWriteError as bwe:
            total_written += bwe.details.get("nUpserted", 0)

    # Index
    print("\nCréation des index...")
    dst.create_index("Status")
    dst.create_index("JuridicalForm")
    dst.create_index("TypeOfEnterprise")
    dst.create_index([("activities.NaceCode", 1)])
    dst.create_index([("activities.Classification", 1)])
    dst.create_index("StartDate")
    dst.create_index("PrimaryName")
    print("  Index créés")

    elapsed = time.time() - t0
    final_count = dst.estimated_document_count()
    print(f"\n{'='*60}")
    print(f"  ✅  enterprise_silver  :  {final_count:,} documents")
    print(f"  Durée totale          :  {elapsed:.0f}s")
    print(f"{'='*60}")

    client.close()


if __name__ == "__main__":
    main()
