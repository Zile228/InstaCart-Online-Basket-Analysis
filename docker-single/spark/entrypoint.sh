#!/bin/bash
# Entrypoint khoi chay Spark - tu dong phat hien SPARK_MODE de bat Master hoac Worker
# - SPARK_MODE=master: Khoi dong Spark Master
# - SPARK_MODE=worker: Khoi dong Spark Worker va ket noi den Master

set -e

SPARK_MODE="${SPARK_MODE:-master}"

echo "------------------------------------------------"
echo "  Spark ${SPARK_VERSION:-4.1.1} - bat dau chay voi vai tro: ${SPARK_MODE}"
echo "------------------------------------------------"

# Ham kiem tra va cho dung port dich vu hoat dong truoc khi ket noi
wait_for_service() {
    local HOST=$1
    local PORT=$2
    local MAX_TRIES=${3:-40}
    local WAIT_SECS=${4:-3}
    local count=0

    echo "Dang cho ket noi den ${HOST}:${PORT} ..."
    while ! nc -z "${HOST}" "${PORT}" 2>/dev/null; do
        count=$((count + 1))
        if [ "${count}" -ge "${MAX_TRIES}" ]; then
            echo "  CANH BAO: Da qua thoi gian cho ket noi ${HOST}:${PORT} sau $((MAX_TRIES * WAIT_SECS))s, van tiep tuc khoi chay..."
            break
        fi
        echo "  [${count}/${MAX_TRIES}] Chua san sang, se thu lai sau ${WAIT_SECS}s..."
        sleep "${WAIT_SECS}"
    done
    echo "  ${HOST}:${PORT} da san sang (hoac ket thuc thoi gian cho - tiep tuc xu ly)!"
}

# Chay o che do Master
if [ "${SPARK_MODE}" = "master" ]; then

    echo "Dang khoi dong Spark Master tren ${SPARK_MASTER_HOST:-spark-master}:${SPARK_MASTER_PORT:-7077}"
    exec ${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.master.Master \
        --host "${SPARK_MASTER_HOST:-spark-master}" \
        --port "${SPARK_MASTER_PORT:-7077}" \
        --webui-port "${SPARK_MASTER_WEBUI_PORT:-8080}"

# Chay o che do Worker
elif [ "${SPARK_MODE}" = "worker" ]; then

    MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"

    # Cat lay chuoi Host va Port tu MASTER_URL de dung lam tham so cho ham wait_for_service
    # Format mac dinh cua MASTER_URL co dang: spark://HOST:PORT
    MASTER_HOST=$(echo "$MASTER_URL" | sed 's|spark://||' | cut -d: -f1)
    MASTER_PORT=$(echo "$MASTER_URL" | sed 's|spark://||' | cut -d: -f2)

    echo "Spark Master URL: ${MASTER_URL}"
    echo "Dang cho ket noi den Master tai ${MASTER_HOST}:${MASTER_PORT}..."

    wait_for_service "${MASTER_HOST}" "${MASTER_PORT}" 40 5

    echo "Dang khoi dong Spark Worker va ket noi den: ${MASTER_URL}"
    exec ${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.worker.Worker \
        --webui-port "${SPARK_WORKER_WEBUI_PORT:-8081}" \
        --memory "${SPARK_WORKER_MEMORY:-2G}" \
        --cores "${SPARK_WORKER_CORES:-2}" \
        "${MASTER_URL}"

# Truong hop truyen vao cac lenh dac biet khac (nhu spark-submit, spark-shell, v.v.)
else
    exec "$@"
fi