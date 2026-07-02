"""
DAG : BCE Hotel Gold — Recalcul annuel incrémental
======================================================
S'exécute chaque 1er janvier. Retraite uniquement les entreprises
dont le nombre de dépôts NBB a changé depuis le dernier passage.

Étapes :
  1. list_done_enterprises     — toutes les status=done dans StateDB
  2. check_new_deposits        — compare filings_count StateDB vs NBB live
  3. download_new_deposits     — scrape uniquement les exercices manquants
  4. recalculate_gold          — recalcule les KPIs pour les entreprises mises à jour
  5. report                    — log du résumé (nouvelles entreprises, KPIs mis à jour)
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Config ─────────────────────────────────────────────────────────────────────
MONGO_URI     = "mongodb://admin:bce_password@localhost:27017/"
LOCAL_CSV_DIR = Path("/workspaces/Data-Platform/TP2/data/nbb_financials")
SILVER_PATH   = str(Path(__file__).parent.parent.parent / "silver")

DEFAULT_ARGS = {
    "owner":            "bce_team",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

# ── Task 1 : lister les entreprises terminées ──────────────────────────────────
def list_done_enterprises(**ctx):
    from pymongo import MongoClient
    client = MongoClient(MONGO_URI)
    coll = client["bce_bronze"]["state_nbb_scraping"]

    enterprises = [
        {
            "enterprise_number": doc["_id"],
            "name":              doc.get("name", ""),
            "filings_count":     doc.get("filings_count", 0),
        }
        for doc in coll.find({"status": "done"}, {"_id": 1, "name": 1, "filings_count": 1})
    ]
    client.close()

    print(f"[list_done] {len(enterprises)} entreprises status=done dans StateDB")
    ctx["ti"].xcom_push(key="enterprises", value=enterprises)
    return len(enterprises)


# ── Task 2 : détecter les nouveaux dépôts ──────────────────────────────────────
def check_new_deposits(**ctx):
    sys.path.insert(0, SILVER_PATH)
    from consult import make_session, get_deposits

    enterprises = ctx["ti"].xcom_pull(key="enterprises", task_ids="list_done_enterprises")
    if not enterprises:
        print("[check] Aucune entreprise à vérifier.")
        ctx["ti"].xcom_push(key="to_update", value=[])
        return 0

    to_update = []
    errors    = 0

    for ent in enterprises:
        num   = ent["enterprise_number"]
        known = ent["filings_count"]
        try:
            session  = make_session(num)
            deposits = get_deposits(session, num)
            # Compter les dépôts >= 2021 disponibles sur NBB
            live_count = sum(
                1 for d in deposits
                if int((d.get("periodEndDateYear") or d.get("periodEndDate", "0000")[:4]) or 0) >= 2021
                and not d.get("migration")
            )
            if live_count > known:
                to_update.append({
                    **ent,
                    "live_count": live_count,
                    "new_count":  live_count - known,
                    "deposits":   deposits,
                })
                print(f"  [NEW] {num} {ent['name'][:40]} : {known} → {live_count} dépôts")
            time.sleep(0.2)
        except Exception as e:
            print(f"  [ERR] {num} : {e}")
            errors += 1

    print(f"[check] {len(to_update)} entreprises avec nouveaux dépôts / {errors} erreurs")
    ctx["ti"].xcom_push(key="to_update", value=to_update)
    return len(to_update)


# ── Task 3 : télécharger les nouveaux dépôts ───────────────────────────────────
def download_new_deposits(**ctx):
    sys.path.insert(0, SILVER_PATH)
    from consult import make_session, download_csv
    from pymongo import MongoClient

    to_update = ctx["ti"].xcom_pull(key="to_update", task_ids="check_new_deposits")
    if not to_update:
        print("[download] Rien à télécharger.")
        ctx["ti"].xcom_push(key="updated_enterprises", value=[])
        return 0

    client = MongoClient(MONGO_URI)
    state  = client["bce_bronze"]["state_nbb_scraping"]

    updated    = []
    MIN_YEAR   = 2021
    MAX_RETRY  = 3

    for ent in to_update:
        num      = ent["enterprise_number"]
        deposits = ent["deposits"]
        session  = make_session(num)
        saved    = 0

        for dep in deposits:
            year = dep.get("periodEndDateYear") or dep.get("periodEndDate", "")[:4]
            try:
                year = int(year)
            except (ValueError, TypeError):
                continue
            if year < MIN_YEAR or dep.get("migration"):
                continue

            dep_id    = dep.get("id", "")
            reference = dep.get("reference", dep_id)
            local_dir  = LOCAL_CSV_DIR / num / str(year)
            local_file = local_dir / f"{reference}.csv"

            if local_file.exists():
                saved += 1
                continue

            local_dir.mkdir(parents=True, exist_ok=True)
            for attempt in range(MAX_RETRY):
                try:
                    csv_text = download_csv(session, dep_id)
                    local_file.write_text(csv_text, encoding="utf-8")
                    saved += 1
                    break
                except Exception as e:
                    if attempt == MAX_RETRY - 1:
                        print(f"    [SKIP] {dep_id} après {MAX_RETRY} tentatives : {e}")
                    else:
                        time.sleep(2)

            time.sleep(0.2)

        # Mettre à jour StateDB
        state.update_one(
            {"_id": num},
            {"$set": {"filings_count": saved, "updated_at": datetime.utcnow()}}
        )
        updated.append(num)
        print(f"  [OK] {num} {ent['name'][:40]} — {saved} dépôts sur disque")

    client.close()
    print(f"[download] {len(updated)} entreprises téléchargées")
    ctx["ti"].xcom_push(key="updated_enterprises", value=updated)
    return len(updated)


# ── Task 4 : recalculer la couche Gold ────────────────────────────────────────
def recalculate_gold(**ctx):
    from io import StringIO
    import pandas as pd
    from pymongo import MongoClient, UpdateOne

    updated = ctx["ti"].xcom_pull(key="updated_enterprises", task_ids="download_new_deposits")
    if not updated:
        print("[gold] Aucune entreprise à recalculer.")
        return 0

    client = MongoClient(MONGO_URI)
    coll   = client["bce_gold"]["hotel_gold"]

    def _get(codes, keys, default=0.0):
        if isinstance(keys, str):
            keys = [keys]
        for k in keys:
            v = codes.get(k)
            if v is not None and isinstance(v, float):
                return v
        return default

    def _pct(num, den):
        if num and den and den != 0:
            return round(num / den * 100, 2)
        return None

    def _ratio(num, den):
        if num and den and den != 0:
            return round(num / den, 4)
        return None

    def parse_csv(path: Path) -> dict:
        text = path.read_text(encoding="utf-8", errors="ignore")
        df   = pd.read_csv(StringIO(text), header=None, skiprows=1, dtype=str)
        codes = {}
        for _, row in df.iterrows():
            key = str(row.iloc[0]).strip().strip('"')
            raw = str(row.iloc[1]).strip().strip('"') if len(row) > 1 else ""
            try:
                codes[key] = float(raw.replace(",", "."))
            except Exception:
                codes[key] = raw
        return codes

    def compute_kpis(codes, year, reference) -> dict:
        ca             = _get(codes, "70") or None
        achats         = _get(codes, "60") or None
        var_stocks     = _get(codes, "71") or None
        ebit           = _get(codes, "9901") or None
        resultat_net   = _get(codes, "9904") or None
        tresorerie     = _get(codes, ["54/58", "54", "5"]) or None
        dettes_fin     = (_get(codes, "17") + _get(codes, "43")) or None
        fonds_propres  = _get(codes, ["10/15", "10"]) or None
        capital        = _get(codes, "100") or None
        depreciation   = _get(codes, "630") or None
        ca_net         = _get(codes, "9906") or None

        marge_brute = (ca - (achats or 0) + (var_stocks or 0)) if ca else None
        ebitda      = (ebit + (depreciation or 0)) if ebit else None

        return {
            "year": year, "reference": reference,
            "period_start":   codes.get("Accounting period start date", ""),
            "period_end":     codes.get("Accounting period end date", ""),
            "entity_name":    codes.get("Entity name", ""),
            "schema_type":    codes.get("Model code", ""),
            "ca": ca, "achats": achats, "marge_brute": marge_brute,
            "ebit": ebit, "ebitda": ebitda, "resultat_net": resultat_net,
            "tresorerie": tresorerie, "dettes_financieres": dettes_fin,
            "fonds_propres": fonds_propres, "capital_souscrit": capital,
            "chiffre_affaires_net": ca_net,
            "ratios": {
                "marge_nette_pct":    _pct(resultat_net, ca),
                "roe_pct":            _pct(resultat_net, fonds_propres),
                "liquidite":          _ratio(tresorerie, dettes_fin),
                "taux_endettement_pct": _pct(dettes_fin, fonds_propres),
                "taux_ebitda_pct":    _pct(ebitda, ca) if ebitda and ca else None,
                "marge_brute_pct":    _pct(marge_brute, ca) if marge_brute and ca else None,
            },
        }

    ops     = []
    success = 0

    for num in updated:
        ent_dir = LOCAL_CSV_DIR / num
        if not ent_dir.is_dir():
            continue

        years = []
        for year_dir in sorted(ent_dir.iterdir()):
            if not year_dir.is_dir():
                continue
            for csv_file in sorted(year_dir.glob("*.csv")):
                try:
                    codes = parse_csv(csv_file)
                    if codes:
                        years.append(compute_kpis(codes, int(year_dir.name), csv_file.stem))
                except Exception as e:
                    print(f"    [CSV ERR] {csv_file} : {e}")

        if not years:
            continue

        ops.append(UpdateOne(
            {"_id": num},
            {"$set": {
                "_id":               num,
                "enterprise_number": num,
                "years":             sorted(years, key=lambda y: y["year"], reverse=True),
                "last_updated":      datetime.utcnow(),
            }},
            upsert=True,
        ))
        success += 1

        if len(ops) >= 200:
            coll.bulk_write(ops, ordered=False)
            ops = []

    if ops:
        coll.bulk_write(ops, ordered=False)

    client.close()
    print(f"[gold] {success} entreprises recalculées dans bce_gold.hotel_gold")
    ctx["ti"].xcom_push(key="gold_updated", value=success)
    return success


# ── Task 5 : rapport final ─────────────────────────────────────────────────────
def report(**ctx):
    n_done     = ctx["ti"].xcom_pull(key=None,               task_ids="list_done_enterprises")
    n_new      = ctx["ti"].xcom_pull(key="to_update",        task_ids="check_new_deposits")
    n_dl       = ctx["ti"].xcom_pull(key="updated_enterprises", task_ids="download_new_deposits")
    n_gold     = ctx["ti"].xcom_pull(key="gold_updated",     task_ids="recalculate_gold")

    n_new  = len(n_new)  if isinstance(n_new, list)  else (n_new  or 0)
    n_dl   = len(n_dl)   if isinstance(n_dl,  list)  else (n_dl   or 0)

    print("=" * 55)
    print("  BCE Hotel Gold — Rapport de recalcul")
    print("=" * 55)
    print(f"  Entreprises status=done         : {n_done}")
    print(f"  Avec nouveaux dépôts détectés   : {n_new}")
    print(f"  Téléchargements effectués       : {n_dl}")
    print(f"  Documents Gold mis à jour       : {n_gold}")
    print("=" * 55)


# ── DAG definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="bce_hotel_gold_annual_refresh",
    description="Recalcul annuel incrémental de la couche Gold hôtellerie BCE",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 6 1 1 *",   # 1er janvier à 6h00
    catchup=False,
    tags=["bce", "gold", "hotel", "annual"],
) as dag:

    t1 = PythonOperator(
        task_id="list_done_enterprises",
        python_callable=list_done_enterprises,
    )

    t2 = PythonOperator(
        task_id="check_new_deposits",
        python_callable=check_new_deposits,
    )

    t3 = PythonOperator(
        task_id="download_new_deposits",
        python_callable=download_new_deposits,
    )

    t4 = PythonOperator(
        task_id="recalculate_gold",
        python_callable=recalculate_gold,
    )

    t5 = PythonOperator(
        task_id="report",
        python_callable=report,
    )

    t1 >> t2 >> t3 >> t4 >> t5
