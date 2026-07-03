#!/bin/bash
# ============================================================
#  BCE/KBO — Pipeline complet Medallion Architecture
#  Bronze → Silver → Gold (HDFS + MongoDB)
# ============================================================
#
#  Usage:
#    ./scripts/run_pipeline.sh            # pipeline complet
#    ./scripts/run_pipeline.sh bronze     # bronze uniquement
#    ./scripts/run_pipeline.sh silver     # silver uniquement
#    ./scripts/run_pipeline.sh gold       # gold uniquement
#
# ============================================================

set -e

STEP=${1:-all}
HDFS_URL="hdfs://namenode:9000"
SPARK_BIN="/spark/bin/spark-submit"
# local[*] : tous les cores du conteneur, pas de réseau driver-worker
# → évite les MetadataFetchFailedException sur shuffles volumineux
SPARK_OPTS="--master local[*] \
  --conf spark.hadoop.fs.defaultFS=${HDFS_URL} \
  --conf spark.driver.memory=4g \
  --conf spark.sql.shuffle.partitions=20 \
  --conf spark.sql.autoBroadcastJoinThreshold=52428800"

LOGO="
╔══════════════════════════════════════════════════════════╗
║    BCE / KBO  —  Medallion Data Platform                 ║
║    Bronze  ▶  Silver  ▶  Gold (HDFS + MongoDB)           ║
╚══════════════════════════════════════════════════════════╝
"
echo "$LOGO"

run_bronze() {
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  🟤  BRONZE — Ingestion CSV → HDFS Parquet"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    docker exec bce_namenode bash /init_hdfs.sh
    docker exec bce_spark_master ${SPARK_BIN} \
        $SPARK_OPTS \
        /app/bronze/ingest_bronze.py
    echo ""
}

run_silver() {
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  ⚪  SILVER — Jointures & Enrichissement"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    docker exec bce_spark_master ${SPARK_BIN} \
        $SPARK_OPTS \
        /app/silver/transform_silver.py
    echo ""
}

run_gold() {
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  🟡  GOLD — Agrégations & MongoDB"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    docker exec bce_spark_master ${SPARK_BIN} \
        $SPARK_OPTS \
        --conf spark.mongodb.output.uri="mongodb://admin:bce_password@mongodb:27017/bce_gold" \
        /app/gold/load_gold.py
    echo ""
}

case "$STEP" in
    bronze)
        run_bronze
        ;;
    silver)
        run_silver
        ;;
    gold)
        run_gold
        ;;
    all)
        run_bronze
        run_silver
        run_gold
        ;;
    *)
        echo "Usage: $0 [bronze|silver|gold|all]"
        exit 1
        ;;
esac

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  🎉  Pipeline terminé avec succès !"
echo ""
echo "  URLs d'accès :"
echo "    📊  Spark Web UI    : http://localhost:8080"
echo "    🗄️   HDFS Web UI    : http://localhost:9870"
echo "    🍃  MongoDB         : localhost:27017"
echo "    🌐  Mongo Express   : http://localhost:8082"
echo "    📓  Jupyter Lab     : http://localhost:8888  (token: bce2026)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
