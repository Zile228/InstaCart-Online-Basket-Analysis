#!/bin/bash
# ============================================================
#  Spark Entrypoint — detects SPARK_MODE and starts service
#  SPARK_MODE=master → start Spark Master
#  SPARK_MODE=worker → start Spark Worker (kết nối đến master)
#
#  Multi-machine update:
#    - Worker có thể chạy trên máy khác qua Tailscale
#    - SPARK_MASTER_URL chứa Tailscale IP (truyền qua env var)
#    - wait_for_service dùng MASTER_HOST đã resolve qua extra_hosts
#      (không hard-code "spark-master" hostname cho worker)
#
#  GIỮ NGUYÊN từ config cũ (đã debug kỹ):
#    - nc -z thay /dev/tcp (reliable hơn trên một số Docker kernel)
# ============================================================

set -e

SPARK_MODE="${SPARK_MODE:-master}"

echo "======================================================"
echo "  Spark ${SPARK_VERSION:-4.1.1} — starting as: ${SPARK_MODE}"
echo "======================================================"

# ── Wait function (dùng nc -z, không dùng /dev/tcp) ──────────
# nc -z reliable hơn /dev/tcp trên các Docker image minimal
wait_for_service() {
    local HOST=$1
    local PORT=$2
    local MAX_TRIES=${3:-40}
    local WAIT_SECS=${4:-3}
    local count=0

    echo "Waiting for ${HOST}:${PORT} ..."
    while ! nc -z "${HOST}" "${PORT}" 2>/dev/null; do
        count=$((count + 1))
        if [ "${count}" -ge "${MAX_TRIES}" ]; then
            echo "  WARNING: Timeout waiting for ${HOST}:${PORT} after $((MAX_TRIES * WAIT_SECS))s, starting anyway..."
            break
        fi
        echo "  [${count}/${MAX_TRIES}] retrying in ${WAIT_SECS}s..."
        sleep "${WAIT_SECS}"
    done
    echo "  ${HOST}:${PORT} is up (or timeout — proceeding anyway)!"
}

# ── Spark Master ─────────────────────────────────────────────
if [ "${SPARK_MODE}" = "master" ]; then

    echo "Starting Spark Master on ${SPARK_MASTER_HOST:-spark-master}:${SPARK_MASTER_PORT:-7077}"
    exec ${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.master.Master \
        --host "${SPARK_MASTER_HOST:-spark-master}" \
        --port "${SPARK_MASTER_PORT:-7077}" \
        --webui-port "${SPARK_MASTER_WEBUI_PORT:-8080}"

# ── Spark Worker ─────────────────────────────────────────────
elif [ "${SPARK_MODE}" = "worker" ]; then

    MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"

    # Trích xuất host từ MASTER_URL để wait_for_service
    # MASTER_URL format: spark://HOST:PORT
    MASTER_HOST=$(echo "$MASTER_URL" | sed 's|spark://||' | cut -d: -f1)
    MASTER_PORT=$(echo "$MASTER_URL" | sed 's|spark://||' | cut -d: -f2)

    echo "Spark Master URL: ${MASTER_URL}"
    echo "Waiting for Master at ${MASTER_HOST}:${MASTER_PORT}..."

    wait_for_service "${MASTER_HOST}" "${MASTER_PORT}" 40 5

    echo "Starting Spark Worker → ${MASTER_URL}"
    exec ${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.worker.Worker \
        --webui-port "${SPARK_WORKER_WEBUI_PORT:-8081}" \
        --memory "${SPARK_WORKER_MEMORY:-2G}" \
        --cores "${SPARK_WORKER_CORES:-2}" \
        "${MASTER_URL}"

# ── Passthrough (spark-submit, spark-shell, v.v.) ─────────────
else
    exec "$@"
fi