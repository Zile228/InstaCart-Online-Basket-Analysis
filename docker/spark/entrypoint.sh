#!/bin/bash
# ============================================================
#  Spark Entrypoint — detects SPARK_MODE and starts service
#  SPARK_MODE=master → start Spark Master
#  SPARK_MODE=worker → start Spark Worker (connects to master)
# ============================================================

set -e

SPARK_MODE="${SPARK_MODE:-master}"

echo "======================================================"
echo "  Spark ${SPARK_VERSION:-4.1.1} — starting as: ${SPARK_MODE}"
echo "======================================================"

# ── Wait function ──────────────────────────────────────────
wait_for_service() {
    local HOST=$1
    local PORT=$2
    local MAX_TRIES=${3:-30}
    local WAIT_SECS=${4:-3}
    local count=0

    echo "Waiting for ${HOST}:${PORT} ..."
    while ! (echo >/dev/tcp/${HOST}/${PORT}) 2>/dev/null; do
        count=$((count + 1))
        if [ "${count}" -ge "${MAX_TRIES}" ]; then
            echo "  WARNING: Timeout waiting for ${HOST}:${PORT}, starting anyway..."
            break
        fi
        echo "  [${count}/${MAX_TRIES}] retrying in ${WAIT_SECS}s..."
        sleep "${WAIT_SECS}"
    done
}

# ── Spark Master ───────────────────────────────────────────
if [ "${SPARK_MODE}" = "master" ]; then

    echo "Starting Spark Master on ${SPARK_MASTER_HOST:-spark-master}:${SPARK_MASTER_PORT:-7077}"
    exec ${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.master.Master \
        --host "${SPARK_MASTER_HOST:-spark-master}" \
        --port "${SPARK_MASTER_PORT:-7077}" \
        --webui-port "${SPARK_MASTER_WEBUI_PORT:-8080}"

# ── Spark Worker ───────────────────────────────────────────
elif [ "${SPARK_MODE}" = "worker" ]; then

    MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"

    # Brief wait to give master time to start
    wait_for_service spark-master 7077 20 3

    echo "Starting Spark Worker → ${MASTER_URL}"
    exec ${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.worker.Worker \
        --webui-port "${SPARK_WORKER_WEBUI_PORT:-8081}" \
        --memory "${SPARK_WORKER_MEMORY:-2G}" \
        --cores "${SPARK_WORKER_CORES:-2}" \
        "${MASTER_URL}"

# ── Passthrough (e.g. spark-submit from outside) ──────────
else
    exec "$@"
fi
