# Journal de Suivi — Projet BCE/KBO

> Fichier mis à jour automatiquement lors de chaque modification du projet.

---

## 2026-07-01 — Initialisation du projet

| Tâche | Statut | Notes |
|---|---|---|
| Exploration des données BCE | ✅ | 7 CSVs, ~7,5 M lignes, snapshot du 27-06-2026 |
| Analyse du notebook de référence `BCE_final.ipynb` | ✅ | Sections 1-3 identifiées |
| Création du notebook `BCE_answers.ipynb` | ✅ | ~40 cellules, toutes sections implémentées |
| Création du `README.md` | ✅ | En français, structure complète |
| Création du `SUIVI.md` | ✅ | Ce fichier |

---

## État des Sections

### Section 0 — Configuration
- ✅ Imports (pandas, numpy, requests, bs4, matplotlib, plotly)
- ✅ Chargement des 7 CSVs avec `dtype=str`
- ✅ Normalisation des numéros BCE (suppression des points)
- ✅ Fonctions utilitaires : `normalize_number`, `format_bce`, `get_code_description`, `get_denomination`, `get_address_str`

### Section 1 — Entités de Base
- ✅ `get_entity(numero)` — consolidation des 7 CSVs
- ✅ `afficher_entity(entity)` — affichage avec codes traduits
- ✅ Affichage des 3 entreprises (Google, Apple, SNCB)

### Section 2 — Informations Enrichies
- ✅ **2.1** Informations générales (nom FR/NL, adresse, NACE principal, date création)
- ✅ **2.2** Informations juridiques (forme, situation, type, TVA)
- ✅ **2.3** Activités NACE (toutes versions, avec libellés traduits)
- ✅ **2.4** Dirigeants — scraping KBO public (`kbopub.economie.fgov.be`)
- ✅ **2.5** Liens entre entités — scraping KBO public
- ✅ **2.6** Statuts notariaux — API `statuts.notaire.be`
- ✅ **2.7** Comptes annuels — API + scraping `consult.cbso.nbb.be`
  - Exclusion des comptes consolidés (mc-*)
  - Déduplication par année (préférence FR)
  - Téléchargement optionnel PDFs/CSVs
- ✅ **2.8** Établissements (depuis CSVs + adresses)
- ✅ **2.9** Publications eJustice — scraping `ejustice.just.fgov.be`
- ✅ **2.10** Coordonnées de contact (depuis CSVs)

### Section 3 — Analyse Financière
- ✅ **3.1** Tableaux d'indicateurs (performance, marges, solvabilité)
  - Google Belgium : données réelles NBB 2021–2025
  - Apple Retail Belgium : estimations indicatives ⚠️
  - SNCB : estimations indicatives ⚠️
- ✅ **3.2** Diagrammes de Sankey (Plotly) — 3 entreprises
- ✅ **3.3** Graphiques d'évolution CA & Résultat Net (Matplotlib)
- ✅ **3.3** Graphique comparatif des marges nettes

### Glossaire
- ✅ Performance (CA, Marge brute, EBIT, EBITDA, Résultat net)
- ✅ Croissance & Marges
- ✅ Autonomie financière
- ✅ Solvabilité
- ✅ Ressources humaines

---

## Points d'Attention

| Point | Action requise |
|---|---|
| Apple & SNCB — données financières estimatives | Exécuter Section 2.7 pour récupérer les vraies données NBB et mettre à jour Section 3.1 |
| Scraping KBO/eJustice | Connexion internet requise lors de l'exécution des sections 2.4–2.7, 2.9 |
| Données directors (2.4) | Le site KBO peut avoir une structure HTML variable — vérifier le résultat |
| Fichiers graphiques | `evolution_financiere.png` et `marges_comparatives.png` générés dans `/data/` |

---

## Données du Snapshot BCE

| Champ | Valeur |
|---|---|
| Date du snapshot | 27-06-2026 |
| Horodatage extraction | 28-06-2026 09:13:47 |
| Type d'extraction | Full |
| Numéro d'extraction | 404 |
| Version | 1.0.0 |
