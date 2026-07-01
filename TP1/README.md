# Projet BCE/KBO — Analyse des Entreprises Belges

## Introduction

Ce projet analyse les données open data du **Carrefour des Entreprises Belges (BCE)** /
**Kruispuntbank van Ondernemingen (KBO)** pour trois grandes entreprises opérant en Belgique :

| Entreprise | Numéro BCE | Secteur |
|---|---|---|
| **Google Belgium** | 0878.065.378 | Services informatiques |
| **Apple Retail Belgium** | 0836.157.420 | Commerce de détail high-tech |
| **SNCB** | 0203.430.576 | Transport ferroviaire |

L'objectif est de constituer un profil complet de chaque entreprise en combinant :
1. Les **données CSV de la BCE** (registre officiel, adresses, activités NACE)
2. Des **sources web externes** (site KBO public, NBB/CBSO, Notaire.be, eJustice)
3. Une **analyse financière** sur la période 2021–2025

---

## Structure des données

Les 7 fichiers CSV forment une base de données relationnelle :

```
enterprise.csv          ← table principale (1,2 M entrées)
    ↓ EnterpriseNumber
denomination.csv        ← noms officiels FR/NL
address.csv             ← adresses (siège, établissements)
activity.csv            ← codes NACE d'activité
contact.csv             ← téléphone, email, site web
establishment.csv       ← unités d'exploitation
    ↑ Code → Description
code.csv                ← table de traduction des codes
```

**Métadonnées du snapshot** : Date 27-06-2026, Extract n°404, Version 1.0.0.

---

## Organisation du Notebook (`BCE_answers.ipynb`)

| Section | Contenu | Source |
|---|---|---|
| **Section 0** | Configuration, imports, chargement des CSVs | — |
| **Section 1** | Entités brutes avec codes traduits | CSV KBO |
| **Section 2.1** | Informations générales | CSV KBO |
| **Section 2.2** | Informations juridiques | CSV KBO |
| **Section 2.3** | Activités NACE | CSV KBO |
| **Section 2.4** | Dirigeants et représentants | kbopub.economie.fgov.be |
| **Section 2.5** | Liens entre entités | kbopub.economie.fgov.be |
| **Section 2.6** | Statuts notariaux | statuts.notaire.be |
| **Section 2.7** | Comptes annuels NBB/CBSO | consult.cbso.nbb.be |
| **Section 2.8** | Établissements | CSV KBO |
| **Section 2.9** | Publications Moniteur belge | ejustice.just.fgov.be |
| **Section 2.10** | Coordonnées de contact | CSV KBO |
| **Section 3.1** | Tableaux d'indicateurs financiers | NBB/CBSO |
| **Section 3.2** | Diagrammes de Sankey (Plotly) | Calculé |
| **Section 3.3** | Graphiques d'évolution (Matplotlib) | Calculé |
| **Glossaire** | Définitions PCMN de tous les indicateurs | — |

---

## Instructions d'Exécution

### Prérequis

```bash
pip install pandas numpy requests beautifulsoup4 matplotlib plotly
```

### Lancement

```bash
cd /workspaces/Data-Platform/TP1/data
jupyter notebook BCE_answers.ipynb
```

### Notes importantes

- Les données CSV doivent être dans `/workspaces/Data-Platform/TP1/data/`
- Les sections **2.4 à 2.7** et **2.9** nécessitent une connexion internet
- Certaines sources web peuvent être temporairement indisponibles (gestion d'erreurs intégrée)
- Les données financières d'**Apple Retail Belgium** et de la **SNCB** sont des **estimations indicatives** à remplacer par les vraies données NBB après exécution de la Section 2.7

---

## Sources des Données

| Source | URL | Données |
|---|---|---|
| **BCE Open Data** | [data.economie.fgov.be](https://data.economie.fgov.be) | CSVs KBO |
| **KBO Public** | [kbopub.economie.fgov.be](https://kbopub.economie.fgov.be) | Dirigeants, liens |
| **NBB/CBSO** | [consult.cbso.nbb.be](https://consult.cbso.nbb.be) | Comptes annuels |
| **Notaire.be** | [statuts.notaire.be](https://statuts.notaire.be) | Statuts |
| **eJustice** | [ejustice.just.fgov.be](https://www.ejustice.just.fgov.be) | Publications légales |

---

## Structure des Fichiers

```
TP1/
├── README.md               ← Ce fichier
├── SUIVI.md                ← Journal de suivi du projet
└── data/
    ├── enterprise.csv
    ├── denomination.csv
    ├── address.csv
    ├── activity.csv
    ├── contact.csv
    ├── establishment.csv
    ├── code.csv
    ├── meta.csv
    ├── BCE_final.ipynb     ← Notebook de référence (template)
    ├── BCE_answers.ipynb   ← Notebook complet avec réponses ✅
    ├── evolution_financiere.png
    └── marges_comparatives.png
```
