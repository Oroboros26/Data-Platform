# BCE/KBO — Secteur Hôtellerie Belgique

Pipeline de données complet sur les **4 112 entreprises hôtelières belges** (codes NACE 55xxx, statut actif).  
Architecture Medallion Bronze → Silver → Gold, exposée via une API FastAPI et un frontend React.

---

## Architecture

```
BCE Open Data (CSV)
        │
        ▼
┌───────────────────────────────────────────────┐
│  BRONZE  —  MongoDB bce_bronze                │
│  enterprise_silver  ← silver_transform.py     │
│  state_nbb_scraping ← StateDB scraping        │
└──────────────────────────┬────────────────────┘
                           │
                           ▼
┌───────────────────────────────────────────────┐
│  SILVER  —  MongoDB bce_silver                │
│  enterprise_silver : 4 112 hôtels enrichis    │
│  (nom, adresse, NACE, forme juridique...)     │
└──────────────────────────┬────────────────────┘
                           │  nbb_scraper.py
                           │  → consult.cbso.nbb.be
                           │  → CSV PCMN par exercice
                           ▼
┌───────────────────────────────────────────────┐
│  GOLD  —  MongoDB bce_gold                    │
│  hotel_gold   : 1 doc/entreprise + KPIs       │
│  dirigeants   : cache kbopub (SSE)            │
└──────────────────────────┬────────────────────┘
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
    FastAPI (port 8000)        Airflow DAG
    React + Vite (port 5173)   recalcul annuel
```

---

## Stack technique

| Composant | Technologie | Rôle |
|-----------|-------------|------|
| Base de données | MongoDB 7.0 | Bronze + Silver + Gold |
| Scraping NBB | Python + requests + BeautifulSoup | Dépôts financiers CBSO |
| Gold layer | Python + pandas + ThreadPoolExecutor | Calcul KPIs et ratios |
| API | FastAPI + uvicorn | REST + SSE dirigeants |
| Frontend | React 18 + Vite + axios | UI recherche + fiches |
| Orchestration | Apache Airflow 2.9 | DAG recalcul annuel |
| Dashboard | Python http.server | Monitoring scraping |

> Spark non utilisé : OOM en environnement Codespaces. Remplacé par pandas + ThreadPoolExecutor (8 workers).

---

## Structure du projet

```
TP2/
├── silver/
│   ├── silver_transform.py     ← BCE CSV → MongoDB enterprise_silver
│   ├── nbb_scraper.py          ← Scraping CBSO NBB → CSV PCMN locaux
│   ├── consult.py              ← Client HTTP consult.cbso.nbb.be
│   └── dashboard.py            ← Dashboard progression scraping (port 5050)
│
├── gold/
│   ├── hotel_gold.py           ← CSV PCMN → KPIs → bce_gold.hotel_gold
│   └── day3.md                 ← Spécification jour 3
│
├── api/
│   └── main.py                 ← FastAPI : search, fiche, financials, SSE
│
├── frontend/
│   ├── src/
│   │   ├── App.jsx             ← SearchView + EnterpriseView
│   │   ├── api.js              ← Axios + EventSource wrappers
│   │   └── components/
│   │       ├── FinancialPanel.jsx   ← Onglets années + KPIs + ratios
│   │       ├── SankeySvg.jsx        ← Sankey SVG CA→Marge→Résultat
│   │       └── DirigeantsPanel.jsx  ← SSE kbopub
│   └── vite.config.js          ← Proxy /api → localhost:8000
│
├── airflow/
│   └── dags/
│       └── bce_hotel_gold_refresh.py  ← DAG recalcul annuel incrémental
│
├── data/
│   └── nbb_financials/         ← CSV PCMN téléchargés (un répertoire par BCE)
│       └── {enterprise_number}/
│           └── {year}/
│               └── {reference}.csv
│
└── docker-compose.yml          ← Stack Hadoop + MongoDB (MongoDB seul utilisé)
```

---

## Services et ports

| Service | Port | URL Codespaces |
|---------|------|----------------|
| **Frontend React** | 5173 | `...-5173.app.github.dev` |
| **FastAPI** | 8000 | `...-8000.app.github.dev` |
| **Dashboard scraping** | 5050 | `...-5050.app.github.dev` |
| **Airflow UI** | 8088 | `...-8088.app.github.dev` |
| **MongoDB** | 27017 | localhost uniquement |

> Tous les ports doivent être en **Public** dans l'onglet Ports de Codespaces.

---

## Lancer les services

```bash
cd /workspaces/Data-Platform/TP2

# API FastAPI
nohup uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload > /tmp/api.log 2>&1 &

# Frontend React
cd frontend && nohup npm run dev > /tmp/vite.log 2>&1 &

# Dashboard scraping
nohup python3 silver/dashboard.py > /tmp/dashboard.log 2>&1 &

# Airflow
export AIRFLOW_HOME=/workspaces/Data-Platform/TP2/airflow
nohup airflow webserver --port 8088 > /tmp/airflow_web.log 2>&1 &
nohup airflow scheduler > /tmp/airflow_sched.log 2>&1 &
```

---

## Pipeline de données

### 1. Silver — Chargement BCE

```bash
cd /workspaces/Data-Platform/TP2
python3 silver/silver_transform.py
```

Lit les CSV BCE Open Data et construit `bce_silver.enterprise_silver` :  
1 document par entreprise avec nom, adresse, NACE, forme juridique, statut.

### 2. Scraping NBB CBSO

```bash
python3 silver/nbb_scraper.py --scrape-only --workers 8
```

Pour chaque entreprise hôtelière (status=pending dans StateDB) :
- Interroge `consult.cbso.nbb.be` pour lister les dépôts financiers ≥ 2021
- Télécharge chaque dépôt en CSV PCMN
- Stocke dans `data/nbb_financials/{bce}/{année}/{référence}.csv`
- Met à jour `bce_bronze.state_nbb_scraping`

### 3. Gold — Calcul des KPIs

```bash
python3 gold/hotel_gold.py
```

Pour chaque répertoire d'entreprise dans `data/nbb_financials/` :
- Parse les codes PCMN
- Calcule les ratios financiers
- Upsert dans `bce_gold.hotel_gold`

Le script est **idempotent** (upsert) — peut être relancé après un scraping partiel.

---

## Codes PCMN → Champs Gold

| Code PCMN | Champ | Description |
|-----------|-------|-------------|
| 70 | `ca` | Chiffre d'affaires (modèles full uniquement) |
| 60 | `achats` | Achats de marchandises |
| 71 | `var_stocks` | Variation de stocks |
| 9901 | `ebit` | Résultat d'exploitation |
| 9904 | `resultat_net` | Résultat de l'exercice |
| 54/58 | `tresorerie` | Valeurs disponibles |
| 17 + 43 | `dettes_financieres` | Dettes financières totales |
| 10/15 | `fonds_propres` | Capitaux propres |
| 100 | `capital_souscrit` | Capital souscrit |
| 630 | `depreciation` | Amortissements |
| 9906 | `chiffre_affaires_net` | CA net (modèles abrégés) |

### Ratios calculés

| Ratio | Formule |
|-------|---------|
| Marge nette % | Résultat net / CA × 100 |
| Marge brute % | (CA − Achats + Var. stocks) / CA × 100 |
| ROE % | Résultat net / Fonds propres × 100 |
| EBITDA % | (EBIT + Amortissements) / CA × 100 |
| Ratio de liquidité | Trésorerie / Dettes financières |
| Taux d'endettement % | Dettes financières / Fonds propres × 100 |

> Les ratios impliquant le CA retournent `null` pour les modèles abrégés (m87-f, m07-f) qui ne publient pas le code 70.

---

## Schéma MongoDB — bce_gold.hotel_gold

```json
{
  "_id": "0402873860",
  "enterprise_number": "0402873860",
  "last_updated": "2026-07-02T16:17:46.257Z",
  "years": [
    {
      "year": 2024,
      "reference": "2025-00232173",
      "schema_type": "m01-f",
      "period_start": "2024-01-01",
      "period_end": "2024-12-31",
      "ca": null,
      "ebit": -2600417.4,
      "resultat_net": -2731522.96,
      "tresorerie": 3097140.01,
      "dettes_financieres": 11087065.19,
      "fonds_propres": 3792890.12,
      "ratios": {
        "roe_pct": -72.02,
        "liquidite": 0.279,
        "taux_endettement_pct": 292.31
      }
    }
  ]
}
```

---

## API FastAPI — Endpoints

| Méthode | Endpoint | Description |
|---------|----------|-------------|
| GET | `/api/search?q=...` | Recherche par nom ou numéro BCE (max 20) |
| GET | `/api/enterprise/{num}` | Fiche complète (silver + gold + scrape_status) |
| GET | `/api/enterprise/{num}/financials` | KPIs Gold uniquement |
| GET | `/api/enterprise/{num}/dirigeants` | SSE — kbopub, mis en cache après 1er scrape |
| GET | `/api/stats` | Progression scraping + compteurs Gold |

---

## DAG Airflow — Recalcul annuel

**Schedule** : `0 6 1 1 *` (1er janvier à 6h00)

```
list_done_enterprises
        ↓
check_new_deposits      ← compare StateDB vs NBB live
        ↓
download_new_deposits   ← scrape uniquement les exercices manquants
        ↓
recalculate_gold        ← upsert bce_gold.hotel_gold
        ↓
report
```

Logique incrémentale : seules les entreprises dont `filings_count` a augmenté depuis le dernier run sont retraitées.

**Login Airflow UI** : `admin` / `admin`

---

## Accès MongoDB

```bash
mongosh "mongodb://admin:bce_password@localhost:27017/"

# Entreprises avec données Gold
use bce_gold
db.hotel_gold.countDocuments()

# Recherche par nom
use bce_silver
db.enterprise_silver.findOne({ PrimaryName: /metropole/i })

# Progression scraping
use bce_bronze
db.state_nbb_scraping.aggregate([
  { $group: { _id: "$status", count: { $sum: 1 } } }
])
```

---

## État du projet (2026-07-02)

| Couche | État |
|--------|------|
| Silver | Complet — 4 112 entreprises |
| Scraping NBB | ~52% — 2 141 / 4 112 done, 6 898 CSV |
| Gold | 1 849 documents avec KPIs |
| API | Opérationnel |
| Frontend | Opérationnel |
| Airflow DAG | Configuré, schedule annuel |
