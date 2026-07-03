"""
BCE/KBO — API FastAPI
Expose les données Silver + Gold au frontend React

Endpoints :
  GET /api/search?q=...              recherche par nom ou numéro BCE
  GET /api/enterprise/{num}          fiche complète (silver + gold)
  GET /api/enterprise/{num}/dirigeants  SSE — scraping kbopub (TODO)
  GET /api/stats                     stats globales (scraping progress)
"""

import os
import asyncio
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pymongo import MongoClient

# ── Config ─────────────────────────────────────────────────────────────────────
MONGO_URI  = os.getenv("MONGO_URI", "mongodb://admin:bce_password@localhost:27017/")
SILVER_DB  = "bce_silver"
GOLD_DB    = "bce_gold"
STATE_DB   = "bce_bronze"

client = MongoClient(MONGO_URI)
silver = client[SILVER_DB]["enterprise_silver"]
gold   = client[GOLD_DB]["hotel_gold"]
state  = client[STATE_DB]["state_nbb_scraping"]

app = FastAPI(title="BCE/KBO Hotel API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Helpers ────────────────────────────────────────────────────────────────────
def _fmt_enterprise(doc: dict) -> dict:
    """Formate un doc enterprise_silver pour la réponse API."""
    doc.pop("_id", None)
    # Convertir les dates en str si besoin
    for k, v in doc.items():
        if isinstance(v, datetime):
            doc[k] = v.isoformat()
    return doc


def _fmt_gold(doc: dict) -> dict:
    """Formate un doc hotel_gold pour la réponse API."""
    doc.pop("_id", None)
    if isinstance(doc.get("last_updated"), datetime):
        doc["last_updated"] = doc["last_updated"].isoformat()
    return doc


# ── GET /api/search ────────────────────────────────────────────────────────────
@app.get("/api/search")
def search(q: str = Query(..., min_length=2)):
    """
    Recherche par nom (PrimaryName) ou numéro BCE (_id).
    Retourne jusqu'à 20 résultats avec infos de base.
    """
    q = q.strip()

    # Recherche exacte par numéro BCE (format 0XXXXXXXXX)
    if q.replace(".", "").replace("-", "").isdigit():
        num = q.replace(".", "").replace("-", "")
        query = {"_id": {"$regex": f"^{num}"}}
    else:
        # Recherche texte sur le nom
        query = {"PrimaryName": {"$regex": q, "$options": "i"}}

    results = []
    for doc in silver.find(query, {
        "_id": 1, "PrimaryName": 1, "Status": 1, "StatusLabel": 1,
        "JuridicalFormLabel": 1, "activities": 1,
        "addresses": 1,
    }).limit(20):
        addr = next((a for a in doc.get("addresses", [])), {})
        nace = [a["NaceLabel"] for a in doc.get("activities", []) if a.get("NaceLabel")][:2]
        results.append({
            "enterprise_number": doc["_id"],
            "name":    doc.get("PrimaryName", ""),
            "status":  doc.get("StatusLabel", doc.get("Status", "")),
            "form":    doc.get("JuridicalFormLabel", ""),
            "city":    addr.get("MunicipalityFR", addr.get("Municipality", "")),
            "zip":     addr.get("Zipcode", ""),
            "nace":    nace,
        })

    return {"count": len(results), "results": results}


# ── GET /api/enterprise/{num} ──────────────────────────────────────────────────
@app.get("/api/enterprise/{num}")
def get_enterprise(num: str):
    """Fiche complète : données Silver + ratios Gold."""
    num = num.strip()

    silver_doc = silver.find_one({"_id": num})
    if not silver_doc:
        return JSONResponse(status_code=404, content={"error": f"Entreprise {num} introuvable"})

    silver_doc = _fmt_enterprise(silver_doc)

    gold_doc = gold.find_one({"enterprise_number": num})
    financials = _fmt_gold(gold_doc) if gold_doc else None

    state_doc = state.find_one({"_id": num})
    scrape_status = {
        "status":        state_doc.get("status") if state_doc else "not_scraped",
        "filings_count": state_doc.get("filings_count", 0) if state_doc else 0,
    }

    return {
        "enterprise":  silver_doc,
        "financials":  financials,
        "scrape_status": scrape_status,
    }


# ── GET /api/enterprise/{num}/financials ───────────────────────────────────────
@app.get("/api/enterprise/{num}/financials")
def get_financials(num: str):
    """Retourne uniquement les ratios financiers Gold pour une entreprise."""
    doc = gold.find_one({"enterprise_number": num})
    if not doc:
        return JSONResponse(status_code=404, content={"error": "Pas de données financières"})
    return _fmt_gold(doc)


# ── GET /api/stats ─────────────────────────────────────────────────────────────
@app.get("/api/stats")
def get_stats():
    """Stats globales : progression scraping + gold layer."""
    done    = state.count_documents({"status": "done"})
    failed  = state.count_documents({"status": "failed"})
    pending = state.count_documents({"status": "pending"})
    inprog  = state.count_documents({"status": "in_progress"})
    total   = state.count_documents({})
    gold_total = gold.count_documents({})

    agg = list(state.aggregate([{"$group": {"_id": None, "f": {"$sum": "$filings_count"}}}]))
    filings = agg[0]["f"] if agg else 0

    return {
        "scraping": {
            "total": total, "done": done, "failed": failed,
            "pending": pending, "in_progress": inprog,
            "pct": round(done / total * 100, 1) if total else 0,
            "filings": filings,
        },
        "gold": {
            "enterprises": gold_total,
        },
    }


# ── GET /api/enterprise/{num}/dirigeants (SSE) ────────────────────────────────
@app.get("/api/enterprise/{num}/dirigeants")
async def get_dirigeants(num: str):
    """
    SSE : scraping kbopub.economie.fgov.be pour les dirigeants.
    Chaque événement SSE envoie un dirigeant en JSON.
    """
    from pathlib import Path
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent / "silver"))

    async def event_stream():
        try:
            yield f"data: {{'status': 'starting', 'enterprise': '{num}'}}\n\n"
            await asyncio.sleep(0.1)

            # Essaie de charger depuis MongoDB d'abord
            existing = client[GOLD_DB]["dirigeants"].find_one({"enterprise_number": num})
            if existing:
                import json
                for d in existing.get("dirigeants", []):
                    yield f"data: {json.dumps(d, default=str)}\n\n"
                    await asyncio.sleep(0.05)
                yield "data: {\"status\": \"done\", \"source\": \"cache\"}\n\n"
                return

            # Scraping kbopub
            import requests, json
            url = f"https://kbopub.economie.fgov.be/kbopub/tabellendetail.html?lang=fr&ondernemingsnummer={num.replace('.','')}"
            try:
                from bs4 import BeautifulSoup
                r = requests.get(url, timeout=10)
                soup = BeautifulSoup(r.text, "html.parser")
                rows = soup.select("table.table tr")
                dirigeants = []
                for row in rows[1:]:
                    cols = [td.get_text(strip=True) for td in row.find_all("td")]
                    if len(cols) >= 3:
                        d = {"nom": cols[0], "fonction": cols[1], "depuis": cols[2]}
                        dirigeants.append(d)
                        yield f"data: {json.dumps(d, ensure_ascii=False)}\n\n"
                        await asyncio.sleep(0.1)

                # Persister en base
                if dirigeants:
                    client[GOLD_DB]["dirigeants"].update_one(
                        {"enterprise_number": num},
                        {"$set": {"enterprise_number": num, "dirigeants": dirigeants, "scraped_at": datetime.utcnow()}},
                        upsert=True
                    )
            except Exception as e:
                yield f"data: {{\"status\": \"error\", \"message\": \"{str(e)[:100]}\"}}\n\n"

            yield "data: {\"status\": \"done\"}\n\n"

        except asyncio.CancelledError:
            return

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
