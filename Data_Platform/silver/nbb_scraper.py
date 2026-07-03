"""
BCE/KBO — NBB Scraper (secteur hôtellerie)
enterprise_silver → StateDB → dépôts financiers NBB 2021-2025

Étapes :
  1. Extraction des entreprises hôtelières depuis enterprise_silver
  2. Chargement en StateDB (status=pending si nouvel enregistrement)
  3. Scraping NBB CBSO pour chaque entreprise (status=pending/failed)
  4. Stockage CSV local + upload WebHDFS

Usage :
  python3 silver/nbb_scraper.py [--load-only] [--scrape-only] [--limit N] [--workers N]

  --load-only   : uniquement étape 1+2 (chargement StateDB)
  --scrape-only : uniquement étape 3+4 (scraping, StateDB déjà prêt)
  --limit N     : limiter à N entreprises pour test
  --workers N   : nombre de threads parallèles (défaut: 5)
"""

import os
import sys
import time
import argparse
import requests
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
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
REQUEST_DELAY  = 0.15  # secondes entre requêtes NBB
MAX_RETRIES    = 3

# Codes NACE hébergement — liste complète (toutes versions NACE)
NACE_HOTELLERIE = {
    "55100", "55101", "55102",                           # hôtels
    "55201", "55202", "55203", "55204", "55209", "55210", # hébergement courte durée
    "55220", "55231", "55232", "55233",                  # camping / vacances
    "55300",                                             # terrains camping
    "55400",                                             # intermédiation hébergement
    "55900",                                             # autres hébergements
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
        "activities": {
            "$elemMatch": {
                "NaceCode": {"$in": list(NACE_HOTELLERIE)},
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


# ── Pool de sessions réutilisables ────────────────────────────────────────────
class SessionPool:
    """Pool thread-safe de sessions HTTP NBB réutilisables."""
    def __init__(self, size: int):
        self._pool = []
        self._lock = threading.Lock()
        # Pré-créer les sessions en batches avec stagger pour éviter le thundering herd
        for i in range(size):
            try:
                s = make_session("0878065378")  # Google Belgium — juste pour les cookies
                self._pool.append(s)
            except Exception:
                self._pool.append(requests.Session())
            if (i + 1) % 5 == 0:
                time.sleep(0.3)  # stagger par batch de 5

    def get(self):
        with self._lock:
            if self._pool:
                return self._pool.pop()
        return make_session("0878065378")

    def put(self, session):
        with self._lock:
            self._pool.append(session)


_session_pool: SessionPool | None = None


# ── Étape 3 : scraping NBB CBSO ──────────────────────────────────────────────
def scrape_enterprise(enterprise_number: str, out_dir: Path, hdfs_base: str):
    """
    Scrape les dépôts >= MIN_YEAR pour une entreprise.
    Retourne (n_filings_saved, error_or_None).
    """
    saved = 0
    session = _session_pool.get() if _session_pool else None

    try:
        if session is None:
            session = make_session(enterprise_number)
        else:
            page_url = f"https://consult.cbso.nbb.be/consult-enterprise/{enterprise_number}"
            session.headers.update({"Referer": page_url})

        # Retry get_deposits on 429 with exponential backoff
        deposits = None
        for attempt in range(6):
            try:
                deposits = get_deposits(session, enterprise_number)
                break
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 429:
                    wait = 5 * (2 ** attempt)  # 5, 10, 20, 40, 80, 160s
                    time.sleep(wait)
                elif attempt == 5:
                    raise
                else:
                    time.sleep(2)
        if deposits is None:
            raise Exception("max retries exceeded on get_deposits")
    except Exception as e:
        if _session_pool and session:
            _session_pool.put(session)
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

    if _session_pool and session:
        _session_pool.put(session)
    return saved, None


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="BCE NBB Hôtellerie Scraper")
    parser.add_argument("--load-only",   action="store_true", help="Charge StateDB uniquement")
    parser.add_argument("--scrape-only", action="store_true", help="Scraping uniquement (StateDB déjà prêt)")
    parser.add_argument("--limit",       type=int, default=0, help="Limiter à N entreprises (0=toutes)")
    parser.add_argument("--workers",     type=int, default=5, help="Nombre de threads parallèles (défaut: 5)")
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

    # ── Étape 3 : scraping parallèle ─────────────────────────────
    pending_query = {"status": {"$in": ["pending", "failed"]}}
    pending_count = state_coll.count_documents(pending_query)
    print(f"\n[Scraping] {pending_count:,} entreprises à traiter ({args.workers} workers)...")

    if pending_count == 0:
        print("  Rien à scraper — toutes les entreprises sont 'done'.")
        client.close()
        return

    # Initialiser le pool de sessions (une par worker)
    global _session_pool
    print(f"  Initialisation du pool de {args.workers} sessions HTTP...")
    _session_pool = SessionPool(args.workers)
    print("  Pool prêt.")

    # Charger tous les records pending en mémoire pour éviter les curseurs expirés
    records = list(state_coll.find(pending_query, {"_id": 1, "enterprise_number": 1, "name": 1}))

    counters    = {"done": 0, "error": 0}
    stop_event  = threading.Event()
    print_lock  = threading.Lock()
    counter_lock = threading.Lock()

    def process(rec):
        if stop_event.is_set():
            return
        eid  = rec["enterprise_number"]
        name = rec.get("name", "")

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
                with counter_lock:
                    counters["error"] += 1
                with print_lock:
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
                with counter_lock:
                    counters["done"] += 1
                elapsed = time.time() - t0
                total_done = state_coll.count_documents({"status": "done"})
                with print_lock:
                    print(f"  ✅  {eid}  {name[:35]:<35}  {n_saved} dépôts  [{elapsed:.0f}s]  ({total_done}/{len(records)})")
        except Exception as e:
            state_coll.update_one(
                {"_id": eid},
                {"$set": {"status": "failed", "error": str(e)[:200], "updated_at": datetime.utcnow()}}
            )
            with counter_lock:
                counters["error"] += 1
            with print_lock:
                print(f"  ❌  {eid}  {name[:40]}  →  {e}")

    try:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(process, rec): rec for rec in records}
            for future in as_completed(futures):
                if stop_event.is_set():
                    break
                try:
                    future.result()
                except Exception:
                    pass
    except KeyboardInterrupt:
        print("\n⚠️  Interruption — les workers terminent leur tâche en cours...")
        stop_event.set()
        # Remettre les in_progress → failed pour reprise propre
        state_coll.update_many(
            {"status": "in_progress"},
            {"$set": {"status": "failed", "error": "interrupted", "updated_at": datetime.utcnow()}}
        )

    elapsed = time.time() - t0
    final_done   = state_coll.count_documents({"status": "done"})
    final_failed = state_coll.count_documents({"status": "failed"})

    print(f"\n{'='*60}")
    print(f"  Session  :  +{counters['done']} done  /  +{counters['error']} erreurs")
    print(f"  Total    :  done={final_done:,}  failed={final_failed:,}")
    print(f"  CSV      :  {LOCAL_OUT_DIR}")
    print(f"  Durée    :  {elapsed:.0f}s")
    print(f"{'='*60}")

    client.close()


if __name__ == "__main__":
    main()
