"""
NBB Scraping — Live Progress Dashboard
Accessible sur http://localhost:5050
"""
from http.server import BaseHTTPRequestHandler, HTTPServer
from pymongo import MongoClient
from pathlib import Path
import json, time

MONGO_URI = "mongodb://admin:bce_password@localhost:27017/"
LOCAL_OUT  = Path("/workspaces/Data-Platform/TP2/data/nbb_financials")

def get_stats():
    try:
        c   = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        col = c["bce_bronze"]["state_nbb_scraping"]
        done    = col.count_documents({"status": "done"})
        failed  = col.count_documents({"status": "failed"})
        pending = col.count_documents({"status": "pending"})
        inprog  = col.count_documents({"status": "in_progress"})
        total   = col.count_documents({})
        agg = list(col.aggregate([{"$group": {"_id": None, "f": {"$sum": "$filings_count"}}}]))
        filings = agg[0]["f"] if agg else 0
        recent  = list(col.find({"status": "done"}, {"_id":1,"name":1,"filings_count":1,"updated_at":1})
                          .sort("updated_at", -1).limit(8))
        c.close()
        csv_count = len(list(LOCAL_OUT.rglob("*.csv"))) if LOCAL_OUT.exists() else 0
        pct = round(done / total * 100, 1) if total else 0
        return {
            "total": total, "done": done, "failed": failed,
            "pending": pending, "inprog": inprog,
            "filings": filings, "csv": csv_count, "pct": pct,
            "recent": [{"id": r["_id"], "name": r.get("name","")[:45],
                        "filings": r.get("filings_count",0)} for r in recent],
        }
    except Exception as e:
        return {"error": str(e)}

HTML = """<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="8">
<title>NBB Scraping — Progress</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; padding: 32px; }}
  h1 {{ font-size: 1.4rem; font-weight: 600; color: #7dd3fc; margin-bottom: 24px; }}
  .grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 28px; }}
  .card {{ background: #1e293b; border-radius: 12px; padding: 20px; }}
  .card .label {{ font-size: 0.75rem; color: #94a3b8; text-transform: uppercase; letter-spacing: .05em; margin-bottom: 6px; }}
  .card .value {{ font-size: 2rem; font-weight: 700; }}
  .done   .value {{ color: #4ade80; }}
  .failed .value {{ color: #f87171; }}
  .pending .value {{ color: #fbbf24; }}
  .csv    .value {{ color: #7dd3fc; }}
  .bar-wrap {{ background: #1e293b; border-radius: 12px; padding: 20px; margin-bottom: 28px; }}
  .bar-label {{ font-size: 0.85rem; color: #94a3b8; margin-bottom: 10px; }}
  .bar-track {{ background: #334155; border-radius: 999px; height: 24px; overflow: hidden; }}
  .bar-fill  {{ height: 100%; border-radius: 999px; background: linear-gradient(90deg, #22c55e, #4ade80);
                transition: width .5s ease; display: flex; align-items: center; justify-content: flex-end; padding-right: 10px;
                font-size: 0.8rem; font-weight: 700; color: #052e16; }}
  .table-wrap {{ background: #1e293b; border-radius: 12px; padding: 20px; }}
  .table-wrap h2 {{ font-size: 0.9rem; color: #94a3b8; margin-bottom: 14px; text-transform: uppercase; letter-spacing: .05em; }}
  table {{ width: 100%; border-collapse: collapse; }}
  th {{ text-align: left; font-size: 0.72rem; color: #64748b; padding-bottom: 8px; border-bottom: 1px solid #334155; }}
  td {{ padding: 8px 0; font-size: 0.85rem; border-bottom: 1px solid #1e293b; }}
  td:last-child {{ color: #4ade80; text-align: right; }}
  .badge {{ display: inline-block; background: #166534; color: #4ade80; border-radius: 4px; padding: 1px 6px; font-size: 0.7rem; }}
  .ts {{ font-size: 0.72rem; color: #475569; margin-top: 20px; text-align: right; }}
  .workers {{ color: #a78bfa; }}
</style>
</head>
<body>
<h1>&#x1F4CA; NBB Scraping — Secteur Hôtellerie Belgique</h1>
<div class="grid">
  <div class="card done">   <div class="label">Terminées</div><div class="value">{done}</div></div>
  <div class="card failed"> <div class="label">Erreurs NBB</div><div class="value">{failed}</div></div>
  <div class="card pending"><div class="label">En attente</div><div class="value">{pending}</div></div>
  <div class="card csv">    <div class="label">CSV téléchargés</div><div class="value">{csv}</div></div>
</div>
<div class="bar-wrap">
  <div class="bar-label">Progression globale — <span class="workers">{inprog} workers actifs</span> — {filings} filings scrappés</div>
  <div class="bar-track">
    <div class="bar-fill" style="width:{pct}%">{pct}%</div>
  </div>
  <div style="margin-top:8px;font-size:0.75rem;color:#64748b">{done} / {total} entreprises</div>
</div>
<div class="table-wrap">
  <h2>Dernières entreprises scrappées</h2>
  <table>
    <tr><th>N° BCE</th><th>Nom</th><th style="text-align:right">Dépôts</th></tr>
    {rows}
  </table>
</div>
<p class="ts">Rafraîchissement automatique toutes les 8 secondes</p>
</body></html>"""

def render(stats):
    if "error" in stats:
        return f"<h1>Erreur MongoDB : {stats['error']}</h1>".encode()
    rows = "".join(
        f"<tr><td><code>{r['id']}</code></td><td>{r['name']}</td><td>{r['filings']} <span class='badge'>CSV</span></td></tr>"
        for r in stats["recent"]
    )
    html = HTML.format(rows=rows, **{k: stats[k] for k in
           ["done","failed","pending","csv","pct","inprog","filings","total"]})
    return html.encode()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/stats":
            body = json.dumps(get_stats()).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
        else:
            body = render(get_stats())
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass  # silence access logs


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 5050), Handler)
    print("Dashboard: http://localhost:5050  (Ctrl+C pour arrêter)")
    server.serve_forever()
