"""
BCE/KBO — Bronze MongoDB Ingestion (multi-passes, memory-bounded)
CSV bruts → collection enterprise_finale

Stratégie :
  Pass 0 : insère les documents enterprise de base (sans lookups)
  Pass 1 : denomination  → $set denominations
  Pass 2 : address       → $set addresses
  Pass 3 : activity      → $set activities
  Pass 4 : contact       → $set contacts

Un seul CSV en mémoire à la fois → RAM bornée ~300-400 MB.

Usage :
  python3 silver/ingest_mongo.py [--from-pass N]   # reprendre à la passe N
"""

import os
import sys
import time
import argparse
import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

DATA_DIR   = os.getenv("DATA_DIR",   "/workspaces/Data-Platform/TP2/data")
MONGO_URI  = os.getenv("MONGO_URI",  "mongodb://admin:bce_password@localhost:27017/")
MONGO_DB   = os.getenv("MONGO_DB",   "bce_bronze")
COLL_NAME  = "enterprise_finale"
BATCH_SIZE = 5_000


def norm_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.replace(".", "", regex=False)\
                         .str.replace(" ", "", regex=False)\
                         .str.strip()


def read_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False,
                       on_bad_lines="skip", engine="python")


# ── Pass 0 : documents enterprise de base ─────────────────────────────────────
def pass0_enterprise(coll, data_dir):
    print("\n[Pass 0] Ingestion enterprise.csv (documents de base)...")
    t0 = time.time()
    total = 0
    chunk_n = 0

    for chunk in pd.read_csv(
        f"{data_dir}/enterprise.csv",
        dtype=str,
        keep_default_na=False,
        on_bad_lines="skip",
        engine="python",
        chunksize=BATCH_SIZE,
    ):
        chunk_n += 1
        chunk["EnterpriseNumber"] = norm_series(chunk["EnterpriseNumber"])
        chunk = chunk[chunk["EnterpriseNumber"] != ""]

        ops = []
        for row in chunk.to_dict("records"):
            eid = row["EnterpriseNumber"]
            ops.append(UpdateOne(
                {"_id": eid},
                {"$setOnInsert": {
                    "_id":                eid,
                    "EnterpriseNumber":   eid,
                    "Status":             row.get("Status", ""),
                    "JuridicalSituation": row.get("JuridicalSituation", ""),
                    "TypeOfEnterprise":   row.get("TypeOfEnterprise", ""),
                    "JuridicalForm":      row.get("JuridicalForm", ""),
                    "JuridicalFormCAC":   row.get("JuridicalFormCAC", ""),
                    "StartDate":          row.get("StartDate", ""),
                    "denominations": [], "addresses": [],
                    "activities":    [], "contacts":  [],
                }},
                upsert=True,
            ))

        try:
            r = coll.bulk_write(ops, ordered=False)
            total += r.upserted_count
        except BulkWriteError as e:
            total += e.details.get("nUpserted", 0)

        print(f"  chunk {chunk_n:>3}  total={total:>9,}  [{time.time()-t0:.0f}s]")

    print(f"  ✅  {total:,} docs insérés  ({time.time()-t0:.1f}s)")
    return total


# ── Passes lookup : chunked $push — mémoire = O(chunk_size) ───────────────────
def pass_lookup(coll, csv_path, entity_col, field_name, reset_field=False):
    """
    Lit le CSV par chunks de BATCH_SIZE lignes.
    Pour chaque chunk, groupby local + $push/$each dans MongoDB.
    Pic mémoire : ~quelques MB (un seul chunk à la fois).
    """
    name = os.path.basename(csv_path)
    print(f"\n[Lookup] {name} → {field_name} ...")
    t0 = time.time()

    # Optionnel : réinitialiser le tableau si reprise partielle
    if reset_field:
        print(f"  Réinitialisation {field_name}...")
        coll.update_many({}, {"$set": {field_name: []}})

    total_rows = 0
    ops_sent   = 0
    chunk_n    = 0

    for chunk in pd.read_csv(
        csv_path,
        dtype=str,
        keep_default_na=False,
        on_bad_lines="skip",
        engine="python",
        chunksize=BATCH_SIZE,
    ):
        chunk_n += 1
        chunk[entity_col] = norm_series(chunk[entity_col])
        chunk = chunk[chunk[entity_col] != ""]
        if chunk.empty:
            continue

        other_cols = [c for c in chunk.columns if c != entity_col]
        total_rows += len(chunk)

        ops = []
        for eid, grp in chunk.groupby(entity_col, sort=False):
            records = grp[other_cols].to_dict("records")
            ops.append(UpdateOne(
                {"_id": eid},
                {"$push": {field_name: {"$each": records}}}
            ))

        try:
            coll.bulk_write(ops, ordered=False)
        except BulkWriteError:
            pass
        ops_sent += len(ops)

        elapsed = time.time() - t0
        print(f"  chunk {chunk_n:>4}  lignes={total_rows:>9,}  ops={ops_sent:>7,}  [{elapsed:.0f}s]")

    print(f"  ✅  {total_rows:,} lignes traitées  ({time.time()-t0:.1f}s)")
    return ops_sent


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-pass", type=int, default=0,
                        help="Reprendre à partir de la passe N (0-4)")
    args = parser.parse_args()
    start_pass = args.from_pass

    t_global = time.time()
    print("=" * 60)
    print("  BCE/KBO — BRONZE MongoDB Ingestion (multi-passes)")
    print(f"  Source  : {DATA_DIR}")
    print(f"  Target  : {MONGO_DB}.{COLL_NAME}")
    if start_pass > 0:
        print(f"  Reprise depuis la passe {start_pass}")
    print("=" * 60)

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5_000)
    coll   = client[MONGO_DB][COLL_NAME]

    if start_pass == 0:
        existing = coll.estimated_document_count()
        if existing > 0:
            print(f"\n⚠️  {existing:,} docs existants — drop et réingestion complète...")
            coll.drop()
        pass0_enterprise(coll, DATA_DIR)

    if start_pass <= 1:
        pass_lookup(coll, f"{DATA_DIR}/denomination.csv", "EntityNumber", "denominations")

    if start_pass <= 2:
        pass_lookup(coll, f"{DATA_DIR}/address.csv",      "EntityNumber", "addresses")

    if start_pass <= 3:
        pass_lookup(coll, f"{DATA_DIR}/activity.csv",     "EntityNumber", "activities")

    if start_pass <= 4:
        pass_lookup(coll, f"{DATA_DIR}/contact.csv",      "EntityNumber", "contacts")

    # Index
    print("\nCréation des index...")
    coll.create_index("Status")
    coll.create_index("JuridicalForm")
    coll.create_index("TypeOfEnterprise")
    coll.create_index([("activities.NaceCode", 1)])
    coll.create_index([("activities.Classification", 1)])

    elapsed = time.time() - t_global
    final   = coll.estimated_document_count()
    print(f"\n{'='*60}")
    print(f"  ✅  enterprise_finale  :  {final:,} documents")
    print(f"  Durée totale          :  {elapsed:.0f}s  ({elapsed/60:.1f} min)")
    print(f"{'='*60}")
    client.close()


if __name__ == "__main__":
    main()
