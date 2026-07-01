# Erreur Silver Layer — À résoudre

**Date** : 2026-07-01  
**Statut** : ⚠️ En attente de correction

---

## Symptôme

La commande `./scripts/run_pipeline.sh silver` échoue sur l'étape **[6/8] enterprise_profile**
avec l'erreur suivante :

```
java.util.concurrent.RejectedExecutionException: Task ... rejected from ThreadPoolExecutor [Shutting down]
java.lang.IllegalStateException: Cannot call methods on a stopped SparkContext.
org.apache.spark.SparkException: Job aborted.
Caused by: org.apache.spark.SparkException: Job 16 cancelled because SparkContext was shut down
    at ... AdaptiveSparkPlanExec ...
/tmp/blockmgr-.../temp_shuffle_... (No such file or directory)
```

---

## Cause racine

**OOM (Out of Memory) dans le conteneur Spark** lors de la jointure `enterprise_profile`.

La jointure centrale implique simultanément :
- `enterprise` : 1 231 876 lignes × 7 colonnes
- `denomination` dédupliqué : ~1 M lignes
- `address` dédupliqué : ~1 M lignes
- `activity` dédupliqué : ~1 M lignes
- `contact` pivoté : ~400 K lignes
- 4 broadcast lookups depuis `code.csv`

Le total des données manipulées en mémoire dépasse la capacité du conteneur (~4-5 Go RAM alloués).

**Mode cluster** (spark://spark-master:7077) → `MetadataFetchFailedException` (worker crash + shuffle perdu)  
**Mode local** (`local[*]`) → SparkContext killed (OOM driver JVM)

---

## Ce qui fonctionne ✅

- Bronze layer : **7 565 679 lignes** ingérées en HDFS (9 tables Parquet)
- Structure HDFS `/datalake/bronze/silver/gold` créée

---

## Solutions à tester

### Option A — Augmenter la RAM Docker (recommandé)
Allouer 12 Go+ à Docker Desktop → Settings > Resources > Memory.  
Puis relancer : `./scripts/run_pipeline.sh silver`

### Option B — Découper enterprise_profile en plusieurs passes
Traiter les jointures séquentiellement en écrivant chaque résultat intermédiaire sur HDFS :

```python
# Passe 1 : enterprise + denomination + address → HDFS intermédiaire
# Passe 2 : résultat passe 1 + activity + contact → enterprise_profile final
```

### Option C — Réduire le dataset à tester
Filtrer sur les entreprises actives uniquement (Status = 'AC') pour ~700K lignes
au lieu de 1.2M, vérifier que ça passe, puis retirer le filtre.

```python
enterprise_clean = enterprise.filter(col("Status") == "AC")  # temporaire
```

### Option D — Augmenter mémoire Spark dans docker-compose.yml
```yaml
spark-master:
  environment:
    - SPARK_DRIVER_MEMORY=6g
spark-worker:
  environment:
    - SPARK_WORKER_MEMORY=6g
    - SPARK_EXECUTOR_MEMORY=5g
```

---

## Commande de reprise

```bash
# Reprendre depuis Silver (Bronze déjà OK)
./scripts/run_pipeline.sh silver

# Puis Gold
./scripts/run_pipeline.sh gold
```
