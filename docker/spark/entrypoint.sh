#!/bin/bash
# ============================================================
#  Spark Entrypoint - Nhận diện SPARK_MODE để khởi chạy dịch vụ
#  SPARK_MODE=master -> Khởi động Spark Master
#  SPARK_MODE=worker -> Khởi động Spark Worker và kết nối đến master
# ============================================================

set -e

SPARK_MODE="${SPARK_MODE:-master}"

echo "======================================================"
echo "  Spark ${SPARK_VERSION:-4.1.1} - starting as: ${SPARK_MODE}"
echo "======================================================"

# --- Hàm chờ dịch vụ sẵn sàng ---
# Sử dụng 'nc -z' vì hoạt động ổn định hơn cơ chế '/dev/tcp' trên môi trường Docker
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

# --- Khởi động Spark Master ---
if [ "${SPARK_MODE}" = "master" ]; then

    echo "Starting Spark Master on ${SPARK_MASTER_HOST:-spark-master}:${SPARK_MASTER_PORT:-7077}"
    exec ${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.master.Master \
        --host "${SPARK_MASTER_HOST:-spark-master}" \
        --port "${SPARK_MASTER_PORT:-7077}" \
        --webui-port "${SPARK_MASTER_WEBUI_PORT:-8080}"

# --- Khởi động Spark Worker ---
elif [ "${SPARK_MODE}" = "worker" ]; then

    MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"

    # Trích xuất thông tin host và port từ MASTER_URL để kiểm tra kết nối
    # Định dạng của MASTER_URL: spark://HOST:PORT
    MASTER_HOST=$(echo "$MASTER_URL" | sed 's|spark://||' | cut -d: -f1)
    MASTER_PORT=$(echo "$MASTER_URL" | sed 's|spark://||' | cut -d: -f2)

    echo "Spark Master URL: ${MASTER_URL}"
    echo "Waiting for Master at ${MASTER_HOST}:${MASTER_PORT}..."

    wait_for_service "${MASTER_HOST}" "${MASTER_PORT}" 40 5

    echo "Starting Spark Worker -> ${MASTER_URL}"
    exec ${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.worker.Worker \
        --webui-port "${SPARK_WORKER_WEBUI_PORT:-8081}" \
        --memory "${SPARK_WORKER_MEMORY:-2G}" \
        --cores "${SPARK_WORKER_CORES:-2}" \
        "${MASTER_URL}"

# --- Trường hợp chạy các lệnh khác (spark-submit, spark-shell, v.v.) ---
else
    exec "$@"
fi