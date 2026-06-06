#!/bin/bash
# ============================================================
#  submit_streaming.sh
#  Chạy Spark Structured Streaming job với đúng Kafka package
#  PATCH 2: spark-sql-kafka-0-10_2.13:4.1.1 (Scala 2.13)
#
#  Cách dùng:
#    chmod +x submit_streaming.sh
#    ./submit_streaming.sh
#
#  Hoặc chạy từ host qua docker exec:
#    docker exec spark-master bash /opt/spark/work-dir/04_streaming/submit_streaming.sh
# ============================================================

set -e

# ── Versions (PATCH 1 + PATCH 2) ─────────────────────────────
SPARK_VERSION="4.1.1"
SCALA_VERSION="2.13"           # CRITICAL: Spark 4.x dùng Scala 2.13
KAFKA_VERSION="4.1.1"          # spark-sql-kafka phiên bản khớp Spark

# ── Paths ─────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STREAMING_SCRIPT="${SCRIPT_DIR}/streaming_job.py"
SPARK_HOME="${SPARK_HOME:-/opt/spark}"

# ── Kafka JAR (PATCH 2: _2.13 không phải _2.12) ──────────────
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_${SCALA_VERSION}:${KAFKA_VERSION}"
POSTGRES_PACKAGE="org.postgresql:postgresql:42.7.3"

echo "========================================================"
echo "  Instacart Spark Streaming Job"
echo "  Spark     : ${SPARK_VERSION}"
echo "  Scala     : ${SCALA_VERSION}"
echo "  Packages  : ${KAFKA_PACKAGE}"
echo "              ${POSTGRES_PACKAGE}"
echo "========================================================"

# ── Environment (có thể override bằng .env) ───────────────────
export HDFS_NAMENODE="${HDFS_NAMENODE:-hdfs://namenode:9000}"
export SPARK_MASTER="${SPARK_MASTER:-spark://spark-master:7077}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
export KAFKA_TOPIC="${KAFKA_TOPIC:-instacart-orders}"
export SUPABASE_JDBC_URL="${SUPABASE_JDBC_URL:-}"
export SUPABASE_DB_USER="${SUPABASE_DB_USER:-postgres}"
export SUPABASE_DB_PASSWORD="${SUPABASE_DB_PASSWORD:-}"

echo ""
echo "Config:"
echo "  HDFS       : ${HDFS_NAMENODE}"
echo "  Spark      : ${SPARK_MASTER}"
echo "  Kafka      : ${KAFKA_BOOTSTRAP_SERVERS} → ${KAFKA_TOPIC}"
echo "  Supabase   : ${SUPABASE_JDBC_URL:-NOT CONFIGURED (console mode)}"
echo ""

# ── Tạo HDFS thư mục cần thiết (nếu chưa có) ─────────────────
echo "Ensuring HDFS directories exist..."
hdfs dfs -mkdir -p /instacart/streaming   2>/dev/null || true
hdfs dfs -mkdir -p /instacart/checkpoints 2>/dev/null || true
hdfs dfs -mkdir -p /spark-logs            2>/dev/null || true
echo "  ✓ HDFS directories ready"
echo ""

# ── spark-submit ──────────────────────────────────────────────
echo "Submitting streaming job..."
${SPARK_HOME}/bin/spark-submit \
    --master "${SPARK_MASTER}" \
    --packages "${KAFKA_PACKAGE},${POSTGRES_PACKAGE}" \
    --conf "spark.hadoop.fs.defaultFS=${HDFS_NAMENODE}" \
    --conf "spark.executor.memory=1g" \
    --conf "spark.driver.memory=1g" \
    --conf "spark.executor.cores=2" \
    --conf "spark.sql.shuffle.partitions=20" \
    --conf "spark.streaming.stopGracefullyOnShutdown=true" \
    --conf "spark.network.timeout=300s" \
    --conf "spark.executor.heartbeatInterval=60s" \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    "${STREAMING_SCRIPT}"

echo ""
echo "Streaming job finished."
