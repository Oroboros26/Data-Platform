"""
BCE/KBO — Gold Layer : KPIs financiers hôtellerie
Lit les CSVs PCMN locaux → calcule les ratios → upsert MongoDB bce_gold.hotel_gold

Un document par entreprise :
  { enterprise_number, years: [{year, ca, ebit, resultat_net, ...ratios}], last_updated }

Usage :
  python3 gold/hotel_gold.py [--workers N]
"""

import os
import sys
import argparse
import threading
from pathlib import Path
from datetime import datetime
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from pymongo import MongoClient, UpdateOne

# ── Config ─────────────────────────────────────────────────────────────────────
MONGO_URI     = os.getenv("MONGO_URI",    "mongodb://admin:bce_password@localhost:27017/")
LOCAL_CSV_DIR = Path(os.getenv("LOCAL_OUT_DIR", "/workspaces/Data-Platform/TP2/data/nbb_financials"))
GOLD_DB       = "bce_gold"
GOLD_COLL     = "hotel_gold"

# ── Parsing CSV PCMN ───────────────────────────────────────────────────────────
def parse_csv_file(path: Path) -> dict:
    """Parse un CSV NBB PCMN → dict {code: valeur}."""
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
        df = pd.read_csv(StringIO(text), header=None, skiprows=1, dtype=str)
        codes = {}
        for _, row in df.iterrows():
            key = str(row.iloc[0]).strip().strip('"')
            raw = str(row.iloc[1]).strip().strip('"') if len(row) > 1 else ""
            try:
                codes[key] = float(raw.replace(",", "."))
            except (ValueError, TypeError):
                codes[key] = raw
        return codes
    except Exception:
        return {}


def _get(codes: dict, keys, default=0.0) -> float:
    """Retourne la première valeur numérique trouvée parmi les clés."""
    if isinstance(keys, str):
        keys = [keys]
    for k in keys:
        v = codes.get(k)
        if isinstance(v, (int, float)):
            return float(v)
    return default


def _pct(num, denom):
    return round(num / denom * 100, 2) if denom else None


def _ratio(num, denom, decimals=3):
    return round(num / denom, decimals) if denom else None


def compute_year_kpis(codes: dict, year: int, reference: str) -> dict:
    """Calcule les KPIs pour un exercice à partir des codes PCMN."""
    ca           = _get(codes, "70")
    achats       = _get(codes, "60")
    var_stocks   = _get(codes, "71")
    ebit         = _get(codes, "9901")
    resultat_net = _get(codes, "9904")
    tresorerie   = _get(codes, ["54/58", "54", "5"])
    dettes_fin   = _get(codes, "17") + _get(codes, "43")
    fonds_propres = _get(codes, ["10/15", "10"])
    capital      = _get(codes, "100")
    depreciation = _get(codes, "630")
    chiffre_affaires_net = _get(codes, "9906")   # présent dans modèles abrégés

    marge_brute  = ca - achats + var_stocks if ca else None
    ebitda       = ebit + depreciation if ebit else None

    return {
        "year":         year,
        "reference":    reference,
        "period_start": codes.get("Accounting period start date", ""),
        "period_end":   codes.get("Accounting period end date", ""),
        "entity_name":  codes.get("Entity name", ""),
        "schema_type":  codes.get("Model code", ""),
        "ca":           ca or None,
        "achats":       achats or None,
        "var_stocks":   var_stocks or None,
        "marge_brute":  marge_brute,
        "ebit":         ebit or None,
        "ebitda":       ebitda,
        "resultat_net": resultat_net or None,
        "tresorerie":   tresorerie or None,
        "dettes_financieres": dettes_fin or None,
        "fonds_propres":      fonds_propres or None,
        "capital_souscrit":   capital or None,
        "chiffre_affaires_net": chiffre_affaires_net or None,
        "ratios": {
            "marge_nette_pct":      _pct(resultat_net, ca),
            "roe_pct":              _pct(resultat_net, fonds_propres),
            "liquidite":            _ratio(tresorerie, dettes_fin),
            "taux_endettement_pct": _pct(dettes_fin, fonds_propres),
            "taux_ebitda_pct":      _pct(ebitda, ca) if ebitda and ca else None,
            "marge_brute_pct":      _pct(marge_brute, ca) if marge_brute is not None and ca else None,
        },
    }


# ── Traitement par entreprise ──────────────────────────────────────────────────
def process_enterprise(ent_dir: Path) -> dict | None:
    """Lit tous les CSVs d'une entreprise et retourne le doc Gold."""
    enterprise_number = ent_dir.name
    years = []

    for year_dir in sorted(ent_dir.iterdir()):
        if not year_dir.is_dir():
            continue
        try:
            year = int(year_dir.name)
        except ValueError:
            continue

        for csv_file in sorted(year_dir.glob("*.csv")):
            codes = parse_csv_file(csv_file)
            if not codes:
                continue
            kpis = compute_year_kpis(codes, year, csv_file.stem)
            years.append(kpis)

    if not years:
        return None

    return {
        "_id":               enterprise_number,
        "enterprise_number": enterprise_number,
        "years":             sorted(years, key=lambda y: y["year"], reverse=True),
        "last_updated":      datetime.utcnow(),
    }


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Gold Layer — KPIs hôtellerie")
    parser.add_argument("--workers", type=int, default=8, help="Threads parallèles (défaut: 8)")
    args = parser.parse_args()

    print("=" * 60)
    print("  BCE/KBO — Gold Layer : KPIs Financiers Hôtellerie")
    print("=" * 60)

    if not LOCAL_CSV_DIR.exists():
        print(f"❌  Répertoire CSV introuvable : {LOCAL_CSV_DIR}")
        sys.exit(1)

    ent_dirs = [d for d in LOCAL_CSV_DIR.iterdir() if d.is_dir()]
    total    = len(ent_dirs)
    print(f"  {total:,} entreprises à traiter ({args.workers} workers)...")

    client    = MongoClient(MONGO_URI)
    gold_coll = client[GOLD_DB][GOLD_COLL]
    gold_coll.create_index("enterprise_number")
    gold_coll.create_index([("years.year", 1)])

    ops         = []
    ops_lock    = threading.Lock()
    counter     = {"done": 0, "skip": 0}
    print_lock  = threading.Lock()

    def flush(force=False):
        with ops_lock:
            if ops and (force or len(ops) >= 500):
                gold_coll.bulk_write(ops.copy(), ordered=False)
                ops.clear()

    def process(ent_dir):
        doc = process_enterprise(ent_dir)
        if doc is None:
            with ops_lock:
                counter["skip"] += 1
            return
        op = UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True)
        with ops_lock:
            ops.append(op)
            counter["done"] += 1
            done = counter["done"]
        if done % 500 == 0:
            flush()
            with print_lock:
                print(f"  {done}/{total} traités ({counter['skip']} sans CSV)...")

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process, d): d for d in ent_dirs}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                with print_lock:
                    print(f"  ⚠️  {futures[future].name}: {e}")

    flush(force=True)

    final = gold_coll.count_documents({})
    print(f"\n✅  hotel_gold : {final:,} documents dans bce_gold")
    print(f"  Traités  : {counter['done']:,}  |  Sans CSV : {counter['skip']:,}")
    print(f"  Accès UI : http://localhost:8082  (Mongo Express → bce_gold → hotel_gold)")
    client.close()


if __name__ == "__main__":
    main()
