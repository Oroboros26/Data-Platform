# Stratégie de Jointures BCE/KBO — Architecture Medallion

## Schéma relationnel des CSV source

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      TABLES SOURCE (Bronze)                              │
└─────────────────────────────────────────────────────────────────────────┘

  enterprise.csv                      code.csv
  ──────────────────────              ─────────────────────────────────
  EnterpriseNumber  PK  ◄─────────   Category + Code  PK  (référentiel)
  Status                │            Language
  JuridicalSituation    │            Description
  TypeOfEnterprise      │
  JuridicalForm         │        denomination.csv
  StartDate             │        ─────────────────────
                        ├────────► EntityNumber  FK
                        │          Language
                        │          TypeOfDenomination  (001=officiel, 002=commercial)
                        │          Denomination
                        │
                        │        address.csv
                        │        ──────────────────────
                        ├────────► EntityNumber  FK  (enterprise OU establishment)
                        │          TypeOfAddress  (REGO=siège, COOR=correspondance)
                        │          Zipcode, MunicipalityFR/NL, StreetFR/NL, HouseNumber
                        │
                        │        activity.csv
                        │        ──────────────────────
                        ├────────► EntityNumber  FK
                        │          ActivityGroup  (code numérique)
                        │          NaceVersion  (2003/2008/2025)
                        │          NaceCode
                        │          Classification  (MAIN=principale)
                        │
                        │        contact.csv
                        │        ──────────────────────
                        ├────────► EntityNumber  FK
                        │          EntityContact  (ENT=entreprise, EST=établissement)
                        │          ContactType  (TEL, EMAIL, WEB, FAX)
                        │          Value
                        │
                        │        establishment.csv
                        │        ──────────────────────
                        ├────────► EnterpriseNumber  FK  (entreprise parente)
                        │          EstablishmentNumber  PK
                        │          StartDate
                        │          ↕
                        │    [EstablishmentNumber agit comme EntityNumber
                        │     dans denomination.csv et address.csv]
                        │
                        │        branch.csv
                        │        ──────────────────────
                        └────────► EnterpriseNumber  FK
                                   Id  PK
                                   StartDate
```

---

## Clés de jointure par table

| Table source        | Clé primaire (PK)        | Clé étrangère (FK)                        |
|---------------------|--------------------------|-------------------------------------------|
| `enterprise`        | `EnterpriseNumber`       | —                                         |
| `denomination`      | `EntityNumber + Language + TypeOfDenomination` | `EntityNumber → enterprise.EnterpriseNumber` |
| `address`           | `EntityNumber + TypeOfAddress` | `EntityNumber → enterprise.EnterpriseNumber` **ou** `establishment.EstablishmentNumber` |
| `activity`          | `EntityNumber + NaceCode + NaceVersion` | `EntityNumber → enterprise.EnterpriseNumber` |
| `contact`           | `EntityNumber + ContactType` | `EntityNumber → enterprise.EnterpriseNumber` |
| `establishment`     | `EstablishmentNumber`    | `EnterpriseNumber → enterprise.EnterpriseNumber` |
| `branch`            | `Id`                     | `EnterpriseNumber → enterprise.EnterpriseNumber` |
| `code`              | `Category + Code + Language` | lookup depuis enterprise, activity, etc. |

> **Note sur les numéros BCE** : Dans les CSV, les numéros sont au format `0878.065.378` (avec points).
> La normalisation Bronze supprime les points → `0878065378` (10 chiffres, zéro initial inclus).
> Sur le site KBO public, le zéro initial est omis : `878065378`.

---

## Règles de déduplication

### Dénomination principale
```
denomination.TypeOfDenomination = '001'   → nom officiel
+ Window.partitionBy(EntityNumber).orderBy(
    CASE Language WHEN '1' THEN 0  -- Français prioritaire
                  WHEN '2' THEN 1  -- Néerlandais en fallback
                  ELSE 2
  END
)
→ ROW_NUMBER() = 1  (une seule ligne par entreprise)
```

### Adresse REGO
```
address.TypeOfAddress = 'REGO'   → siège social enregistré
+ Window.partitionBy(EntityNumber).orderBy(1)
→ ROW_NUMBER() = 1  (une seule par entité)
```

### Activité NACE principale
```
activity.Classification = 'MAIN'   → activité principale
+ Window.partitionBy(EntityNumber).orderBy(NaceVersion DESC)
→ ROW_NUMBER() = 1  (version la plus récente : 2025 > 2008 > 2003)
```

### Contact (pivot)
```
contact.EntityContact = 'ENT'   → contacts de l'entreprise (pas des établissements)
pivot(ContactType, ['TEL', 'EMAIL', 'WEB', 'FAX'])
  .agg(first(Value))
→ une ligne par entreprise avec colonnes TEL / EMAIL / WEB / FAX
```

---

## Silver Table 1 : `enterprise_profile`

```
enterprise
  LEFT JOIN denomination  ON EnterpriseNumber = EntityNumber
                             TypeOfDenomination = '001', langue FR préférée
                             → NomPrincipal

  LEFT JOIN denomination  ON EnterpriseNumber = EntityNumber
                             TypeOfDenomination = '002', langue FR préférée
                             → NomCommercial

  LEFT JOIN address       ON EnterpriseNumber = EntityNumber
                             TypeOfAddress = 'REGO'
                             → CodePostal, Commune, Rue, Numero, Boite, Pays

  LEFT JOIN activity      ON EnterpriseNumber = EntityNumber
                             Classification = 'MAIN', NaceVersion la plus récente
                             → NaceCode, NaceVersion

  LEFT JOIN code          ON NaceCode = Code
                             Category ∈ {Nace2003, Nace2008, Nace2025}, Language = '1'
                             → DescriptionNace

  LEFT JOIN contact_pivot ON EnterpriseNumber = EntityNumber
                             EntityContact = 'ENT'
                             → TEL, EMAIL, WEB, FAX

  LEFT JOIN code          ON JuridicalForm = Code
                             Category = 'JuridicalForm', Language = '1'
                             → FormeJuridique

  LEFT JOIN code          ON Status = Code
                             Category = 'Status', Language = '1'
                             → StatutLibelle

  LEFT JOIN code          ON TypeOfEnterprise = Code
                             Category = 'TypeOfEnterprise', Language = '1'
                             → TypeEntreprise

  LEFT JOIN code          ON JuridicalSituation = Code
                             Category = 'JuridicalSituation', Language = '1'
                             → SituationJuridique
```

**Résultat** : une ligne plate par entreprise avec tous ses attributs enrichis.

---

## Silver Table 2 : `establishment_profile`

```
establishment
  LEFT JOIN address       ON EstablishmentNumber = EntityNumber
                             TypeOfAddress = 'REGO'
                             → EstabCodePostal, EstabCommune, EstabRue, EstabNumero, EstabPays

  LEFT JOIN denomination  ON EstablishmentNumber = EntityNumber
                             TypeOfDenomination = '001', langue FR préférée
                             → NomEtablissement

  LEFT JOIN enterprise_profile ON EnterpriseNumber = EnterpriseNumber
                             → NomEntrepriseParent, FormeJuridiqueParent
```

> **Astuce clé** : `EstablishmentNumber` est utilisé comme `EntityNumber` dans `address.csv`
> et `denomination.csv`. C'est la même colonne — les établissements ont leurs propres
> entrées d'adresse et de dénomination dans ces tables.

---

## Silver Table 3 : `all_activities`

```
activity  (TOUTES les activités, pas seulement MAIN)
  LEFT JOIN code          ON NaceCode = Code
                             Category ∈ {Nace2003, Nace2008, Nace2025}, Language = '1'
                             → DescriptionNace

  LEFT JOIN enterprise_profile ON EntityNumber = EnterpriseNumber
                             → NomPrincipal
```

---

## Silver Table 4 : `branch_profile`

```
branch
  LEFT JOIN enterprise_profile ON EnterpriseNumber = EnterpriseNumber
                             → NomEntrepriseParent, FormeJuridiqueParent
```

---

## Gold Tables (agrégations finales → MongoDB)

| Collection Gold          | Source Silver                        | Agrégation principale                          |
|--------------------------|--------------------------------------|------------------------------------------------|
| `company_directory`      | enterprise_profile + establishment_profile + all_activities | NbEtablissements, NbActivites par entreprise   |
| `activity_stats`         | all_activities                       | countDistinct(EnterpriseNumber) par NaceCode   |
| `establishment_stats`    | establishment_profile                | count(EstablishmentNumber) par EnterpriseNumber |
| `geo_stats`              | enterprise_profile                   | countDistinct(EnterpriseNumber) par CodePostal  |

---

## Cardinalités observées (snapshot 27-06-2026)

| Table             | Lignes estimées  | Notes                                      |
|-------------------|------------------|--------------------------------------------|
| enterprise        | ~1 200 000       | ~1 200 000 entreprises actives + radiées   |
| denomination      | ~2 800 000       | Plusieurs noms par entreprise (FR + NL + commercial) |
| address           | ~2 400 000       | REGO + COOR + adresses d'établissements    |
| activity          | ~1 960 000       | Plusieurs NACE par entreprise              |
| contact           | ~706 000         | Toutes entreprises ne publient pas leurs contacts |
| establishment     | ~1 600 000       | Unités d'exploitation                      |
| branch            | ~350 000         | Unités légales de type branche             |
| code              | ~21 000          | Référentiel complet toutes catégories      |

---

## Volumes après transformation

| Layer  | Table                  | Lignes         | Format  | Stockage HDFS |
|--------|------------------------|----------------|---------|---------------|
| Bronze | enterprise             | ~1 200 000     | Parquet | ~50 MB        |
| Bronze | denomination           | ~2 800 000     | Parquet | ~80 MB        |
| Bronze | address                | ~2 400 000     | Parquet | ~120 MB       |
| Bronze | activity               | ~1 960 000     | Parquet | ~50 MB        |
| Bronze | contact                | ~706 000       | Parquet | ~20 MB        |
| Bronze | establishment          | ~1 600 000     | Parquet | ~30 MB        |
| Bronze | branch                 | ~350 000       | Parquet | ~10 MB        |
| Silver | enterprise_profile     | ~1 200 000     | Parquet | ~200 MB       |
| Silver | establishment_profile  | ~1 600 000     | Parquet | ~100 MB       |
| Silver | all_activities         | ~1 960 000     | Parquet | ~60 MB        |
| Gold   | company_directory      | ~1 200 000     | Parquet | ~250 MB       |
| Gold   | activity_stats         | ~8 000         | Parquet | ~1 MB         |
| Gold   | establishment_stats    | ~600 000       | Parquet | ~80 MB        |
| Gold   | geo_stats              | ~2 500         | Parquet | ~1 MB         |
