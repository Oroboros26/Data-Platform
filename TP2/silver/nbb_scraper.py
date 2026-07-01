"""
BCE/KBO — NBB Scraper (secteur hôtellerie)
enterprise_silver → StateDB → dépôts financiers NBB 2021-2025

Étapes :
  1. Extraction des entreprises hôtelières depuis enterprise_silver
  2. Chargement en StateDB (status=pending si nouvel enregistrement)
  3. Scraping NBB CBSO pour chaque entreprise (status=pending/failed)
  4. Stockage CSV local + upload WebHDFS

Usage :
  python3 silver/nbb_scraper.py [--load-only] [--scrape-only] [--limit N]

  --load-only   : uniquement étape 1+2 (chargement StateDB)
  --scrape-only : uniquement étape 3+4 (scraping, StateDB déjà prêt)
  --limit N     : limiter à N entreprises pour test
"""

import os
import sys
import time
import argparse
import requests
from pathlib import Path
from datetime import datetime

from pymongo import MongoClient, UpdateOne

# Importer les fonctions NBB depuis consult.py (même répertoire)
_here = Path(__file__).parent
sys.path.insert(0, str(_here))
from consult import make_session, get_deposits, download_csv

# ── Config ─────────────────────────────────────────────────────────────────────
MONGO_URI      = os.getenv("MONGO_URI",     "mongodb://admin:bce_password@localhost:27017/")
MONGO_DB_SRC   = os.getenv("MONGO_DB_SRC",  "bce_silver")
MONGO_DB_STATE = os.getenv("MONGO_DB_STATE","bce_bronze")
HDFS_WEBAPI    = os.getenv("HDFS_WEBAPI",   "http://localhost:9870/webhdfs/v1")
LOCAL_OUT_DIR  = os.getenv("LOCAL_OUT_DIR",  "/workspaces/Data-Platform/TP2/data/nbb_financials")

COLL_SILVER    = "enterprise_silver"
COLL_STATE     = "state_nbb_scraping"

MIN_YEAR       = 2021
REQUEST_DELAY  = 0.5   # secondes entre requêtes NBB
MAX_RETRIES    = 3

# Codes NACE hôtellerie retenus (voir day2.md)
NACE_HOTELLERIE = {
    "55100", "55201", "55202", "55203", "55204", "55209",
    "55300", "55400", "55900",
}

# Formes juridiques exclues (entités publiques)
EXCLUDED_JF = {
    "110","114","116","117",              # entités publiques
    "301","302","303",                    # services fédéraux
    "310","320","330","340","350",        # autorités régionales
    "400","411","412","413","414","415",  # communes, CPAS
    "416","417","418","419","420",        # intercommunales
}


# ── Étape 1 : extraction entreprises hôtelières ───────────────────────────────
def extract_hotel_enterprises(silver_coll):
    """Retourne la liste des EnterpriseNumber éligibles au scraping."""
    print("  Requête MongoDB enterprise_silver (hôtellerie, Status=AC)...")

    query = {
        "Status": "AC",
        "TypeOfEnterprise": "2",
        "activities": {
            "$elemMatch": {
                "NaceCode":       {"$in": list(NACE_HOTELLERIE)},
                "Classification": "MAIN",
            }
        },
        "JuridicalForm": {"$nin": list(EXCLUDED_JF)},
    }

    total = silver_coll.count_documents(query)
    print(f"  {total:,} entreprises hôtelières éligibles trouvées")

    results = []
    for doc in silver_coll.find(query, {"_id": 1, "PrimaryName": 1, "activities": 1}):
        nace_codes = [
            a["NaceCode"]
            for a in doc.get("activities", [])
            if a.get("NaceCode") in NACE_HOTELLERIE and a.get("Classification") == "MAIN"
        ]
        results.append({
            "enterprise_number": doc["_id"],
            "name":              doc.get("PrimaryName", ""),
            "nace_codes":        nace_codes,
        })
    return results


# ── Étape 2 : chargement StateDB ─────────────────────────────────────────────
def load_statedb(state_coll, enterprises):
    """Insère les nouvelles entreprises en status=pending. Ne touche pas les existantes."""
    ops = []
    for e in enterprises:
        ops.append(UpdateOne(
            {"_id": e["enterprise_number"]},
            {"$setOnInsert": {
                "_id":               e["enterprise_number"],
                "enterprise_number": e["enterprise_number"],
                "name":              e["name"],
                "nace_codes":        e["nace_codes"],
                "status":            "pending",
                "filings_count":     0,
                "error":             None,
                "updated_at":        datetime.utcnow(),
            }},
            upsert=True
        ))
        if len(ops) >= 1000:
            state_coll.bulk_write(ops, ordered=False)
            ops = []
    if ops:
        state_coll.bulk_write(ops, ordered=False)

    counts = {
        "total":       state_coll.count_documents({}),
        "pending":     state_coll.count_documents({"status": "pending"}),
        "done":        state_coll.count_documents({"status": "done"}),
        "failed":      state_coll.count_documents({"status": "failed"}),
        "in_progress": state_coll.count_documents({"status": "in_progress"}),
    }
    return counts


# ── WebHDFS upload ─────────────────────────────────────────────────────────────
def hdfs_upload(local_path: Path, hdfs_path: str, webapi: str = HDFS_WEBAPI):
    """Upload un fichier local vers HDFS via WebHDFS REST API (2 étapes)."""
    url = f"{webapi}{hdfs_path}?op=CREATE&overwrite=true&noredirect=false"
    try:
        r1 = requests.put(url, allow_redirects=False, timeout=10)
        if r1.status_code in (307, 200, 201):
            redirect = r1.headers.get("Location", url.replace("noredirect=false", ""))
            with open(local_path, "rb") as f:
                r2 = requests.put(redirect, data=f, timeout=30)
            return r2.status_code in (200, 201)
    except Exception:
        pass
    return False


# ── Étape 3 : scraping NBB CBSO ──────────────────────────────────────────────
def scrape_enterprise(enterprise_number: str, out_dir: Path, hdfs_base: str):
    """
    Scrape les dépôts >= MIN_YEAR pour une entreprise.
    Retourne (n_filings_saved, error_or_None).
    """
    saved = 0
    session = None

    try:
        session = make_session(enterprise_number)
        deposits = get_deposits(session, enterprise_number)
    except Exception as e:
        return 0, f"session/deposits: {e}"

    for dep in deposits:
        year = dep.get("periodEndDateYear") or dep.get("periodEndDate", "")[:4]
        try:
            year = int(year)
        except (ValueError, TypeError):
            continue

        if year < MIN_YEAR:
            continue

        dep_id    = dep.get("id", "")
        reference = dep.get("reference", dep_id)

        # Chemin local
        local_dir = out_dir / enterprise_number / str(year)
        local_dir.mkdir(parents=True, exist_ok=True)
        local_file = local_dir / f"{reference}.csv"

        # Déjà téléchargé ?
        if local_file.exists():
            saved += 1
            continue

        # Dépôt migré : pas de CSV disponible
        if dep.get("migration"):
            continue

        for attempt in range(MAX_RETRIES):
            try:
                csv_text = download_csv(session, dep_id)
                local_file.write_text(csv_text, encoding="utf-8")
                saved += 1

                # Upload WebHDFS
                hdfs_path = f"{hdfs_base}/{enterprise_number}/{year}/{reference}.csv"
                hdfs_upload(local_file, hdfs_path)
                break
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 429:
                    wait = 30 * (attempt + 1)
                    print(f"      ⚠️  429 Rate limit — attente {wait}s...")
                    time.sleep(wait)
                elif attempt == MAX_RETRIES - 1:
                    raise
                else:
                    time.sleep(2)
            except Exception:
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(2)

        time.sleep(REQUEST_DELAY)

    return saved, None


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="BCE NBB Hôtellerie Scraper")
    parser.add_argument("--load-only",   action="store_true", help="Charge StateDB uniquement")
    parser.add_argument("--scrape-only", action="store_true", help="Scraping uniquement (StateDB déjà prêt)")
    parser.add_argument("--limit",       type=int, default=0, help="Limiter à N entreprises (0=toutes)")
    args = parser.parse_args()

    t0 = time.time()
    print("=" * 60)
    print("  BCE/KBO — NBB Hôtellerie Scraper")
    print("=" * 60)

    client      = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5_000)
    silver_coll = client[MONGO_DB_SRC][COLL_SILVER]
    state_coll  = client[MONGO_DB_STATE][COLL_STATE]

    out_dir   = Path(LOCAL_OUT_DIR)
    hdfs_base = "/datalake/bronze/nbb_financials"

    # Index StateDB
    state_coll.create_index("status")
    state_coll.create_index("enterprise_number")

    # ── Étape 1+2 : chargement StateDB ────────────────────────────
    if not args.scrape_only:
        print("\n[1/2] Extraction entreprises hôtelières...")
        silver_count = silver_coll.estimated_document_count()
        if silver_count == 0:
            print("❌  enterprise_silver est vide — lance d'abord silver_transform.py")
            sys.exit(1)

        enterprises = extract_hotel_enterprises(silver_coll)

        if args.limit > 0:
            enterprises = enterprises[: args.limit]
            print(f"  ⚠️  Limité à {args.limit} entreprises")

        print(f"\n[2/2] Chargement StateDB ({len(enterprises):,} entreprises)...")
        counts = load_statedb(state_coll, enterprises)
        print(f"  StateDB : total={counts['total']:,}  pending={counts['pending']:,}  done={counts['done']:,}  failed={counts['failed']:,}")

        if args.load_only:
            print("\n✅  StateDB chargée. Lance --scrape-only pour démarrer le scraping.")
            client.close()
            return

    # ── Étape 3 : scraping ────────────────────────────────────────
    pending_query  = {"status": {"$in": ["pending", "failed"]}}
    pending_count  = state_coll.count_documents(pending_query)
    print(f"\n[Scraping] {pending_count:,} entreprises à traiter...")

    if pending_count == 0:
        print("  Rien à scraper — toutes les entreprises sont 'done'.")
        client.close()
        return

    done_count  = 0
    error_count = 0

    for rec in state_coll.find(pending_query, batch_size=100):
        eid  = rec["enterprise_number"]
        name = rec.get("name", "")

        # Marquer in_progress
        state_coll.update_one(
            {"_id": eid},
            {"$set": {"status": "in_progress", "updated_at": datetime.utcnow()}}
        )

        try:
            n_saved, err = scrape_enterprise(eid, out_dir, hdfs_base)
            if err:
                state_coll.update_one(
                    {"_id": eid},
                    {"$set": {"status": "failed", "error": err, "updated_at": datetime.utcnow()}}
                )
                error_count += 1
                print(f"  ❌  {eid}  {name[:40]}  →  {err}")
            else:
                state_coll.update_one(
                    {"_id": eid},
                    {"$set": {
                        "status":        "done",
                        "filings_count": n_saved,
                        "error":         None,
                        "updated_at":    datetime.utcnow(),
                    }}
                )
                done_count += 1
                elapsed = time.time() - t0
                print(f"  ✅  {eid}  {name[:35]:<35}  {n_saved} dépôts  [{elapsed:.0f}s]")

        except KeyboardInterrupt:
            print("\n⚠️  Interruption clavier — reprise possible via --scrape-only")
            # Remettre en_progress → failed pour permettre la reprise
            state_coll.update_one(
                {"_id": eid},
                {"$set": {"status": "failed", "error": "interrupted", "updated_at": datetime.utcnow()}}
            )
            break
        except Exception as e:
            state_coll.update_one(
                {"_id": eid},
                {"$set": {"status": "failed", "error": str(e)[:200], "updated_at": datetime.utcnow()}}
            )
            error_count += 1
            print(f"  ❌  {eid}  {name[:40]}  →  {e}")

    elapsed = time.time() - t0
    final_done   = state_coll.count_documents({"status": "done"})
    final_failed = state_coll.count_documents({"status": "failed"})

    print(f"\n{'='*60}")
    print(f"  Session  :  +{done_count} done  /  +{error_count} erreurs")
    print(f"  Total    :  done={final_done:,}  failed={final_failed:,}")
    print(f"  CSV      :  {LOCAL_OUT_DIR}")
    print(f"  Durée    :  {elapsed:.0f}s")
    print(f"{'='*60}")

    client.close()


if __name__ == "__main__":
    main()
