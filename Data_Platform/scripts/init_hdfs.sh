#!/bin/bash
# ============================================================
#  BCE/KBO — Initialisation de la structure HDFS
#  À exécuter depuis le conteneur namenode
# ============================================================

set -e

echo "⏳ Attente du NameNode HDFS..."
MAX_RETRIES=30
RETRIES=0

until hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; do
    RETRIES=$((RETRIES + 1))
    if [ $RETRIES -ge $MAX_RETRIES ]; then
        echo "❌ NameNode non disponible après ${MAX_RETRIES} tentatives."
        exit 1
    fi
    echo "   Tentative $RETRIES/$MAX_RETRIES..."
    sleep 3
done

echo "✅ NameNode prêt (safemode OFF)"
echo ""

# ── Création de la structure de répertoires Medallion ────────────────────────
echo "📁 Création des répertoires HDFS..."

hdfs dfs -mkdir -p /datalake/bronze
hdfs dfs -mkdir -p /datalake/silver
hdfs dfs -mkdir -p /datalake/gold

# Permissions ouvertes pour le dev
hdfs dfs -chmod -R 777 /datalake

echo ""
echo "✅ Structure HDFS créée :"
hdfs dfs -ls /datalake

echo ""
echo "📊 Espace disponible sur HDFS :"
hdfs dfs -df -h /

echo ""
echo "🎉 Initialisation HDFS terminée."
