#!/bin/bash
# ============================================================
#  Hadoop Entrypoint — detects HADOOP_ROLE and starts service
#  HADOOP_ROLE=namenode → format (once) + start NameNode + YARN RM
#  HADOOP_ROLE=datanode → wait for NameNode + start DataNode
# ============================================================

# KHÔNG dùng set -e: script quản lý nhiều process nền,
# một lệnh phụ fail không nên kéo chết toàn bộ container.
set -u

# Fallback cho HADOOP_VERSION: biến được set bởi Dockerfile ENV,
# nhưng với set -u, nếu không được export đúng cách sẽ gây lỗi unbound.
HADOOP_VERSION="${HADOOP_VERSION:-3.4.3}"

HADOOP_ROLE="${HADOOP_ROLE:-namenode}"
NAMENODE_DIR="/hadoop/dfs/name"
DATANODE_DIR="/hadoop/dfs/data"
FORMAT_MARKER="${NAMENODE_DIR}/.formatted"

echo "======================================================"
echo "  Hadoop ${HADOOP_VERSION} — starting as: ${HADOOP_ROLE}"
echo "======================================================"

# ── Wait function ──────────────────────────────────────────
wait_for_service() {
    local HOST=$1
    local PORT=$2
    local MAX_TRIES=${3:-30}
    local WAIT_SECS=${4:-5}
    local count=0
    echo "Waiting for ${HOST}:${PORT} ..."
    while ! nc -z "${HOST}" "${PORT}" 2>/dev/null; do
        count=$((count + 1))
        if [ "${count}" -ge "${MAX_TRIES}" ]; then
            echo "ERROR: Timeout waiting for ${HOST}:${PORT}"
            exit 1
        fi
        echo "  [${count}/${MAX_TRIES}] not ready, retrying in ${WAIT_SECS}s..."
        sleep "${WAIT_SECS}"
    done
    echo "  ${HOST}:${PORT} is up!"
}

# ── NameNode ───────────────────────────────────────────────
if [ "${HADOOP_ROLE}" = "namenode" ]; then

    mkdir -p "${NAMENODE_DIR}" /hadoop/tmp "${HADOOP_HOME}/logs"

    # Format only once
    if [ ! -f "${FORMAT_MARKER}" ]; then
        echo "Formatting HDFS NameNode for the first time..."
        hdfs namenode -format -force -nonInteractive
        touch "${FORMAT_MARKER}"
        echo "NameNode formatted successfully."
    else
        echo "NameNode already formatted (skipping)."
    fi

    echo "Starting NameNode..."
    hdfs namenode &
    NAMENODE_PID=$!

    # Bước 1: Chờ NameNode Web UI (port 9870)
    # FIX: dùng $(hostname) thay vì localhost vì NameNode bind vào
    #      hostname "namenode" (172.18.0.x), KHÔNG phải 127.0.0.1.
    #      nc -z localhost 9870 luôn fail → script exit sau 3 phút
    #      → kill toàn bộ background process kể cả YARN RM.
    echo "[1/3] Waiting for NameNode Web UI on port 9870..."
    count=0
    while ! nc -z "$(hostname)" 9870 2>/dev/null; do
        count=$((count + 1))
        if [ "$count" -ge 60 ]; then
            echo "ERROR: NameNode did not start after 3 minutes."
            exit 1
        fi
        echo "  [${count}/60] not ready yet, retrying in 3s..."
        sleep 3
    done
    echo "  NameNode is up!"

    # Bước 2: Chờ HDFS thoát Safe Mode
    # YARN crash ngay nếu HDFS còn Safe Mode (không ghi được /tmp/hadoop-yarn)
    echo "[2/3] Waiting for HDFS to exit Safe Mode..."
    count=0
    while true; do
        SAFEMODE=$(hdfs dfsadmin -safemode get 2>/dev/null || echo "error")
        if echo "$SAFEMODE" | grep -q "Safe mode is OFF"; then
            echo "  HDFS is out of Safe Mode!"
            break
        fi
        count=$((count + 1))
        if [ "$count" -ge 30 ]; then
            echo "  Safe Mode timeout — forcing exit..."
            hdfs dfsadmin -safemode forceExit 2>/dev/null || true
            sleep 3
            break
        fi
        echo "  [${count}/30] Still in Safe Mode, waiting 5s..."
        sleep 5
    done

    # Bước 3: Chuẩn bị thư mục HDFS cho YARN
    echo "[3/3] Preparing YARN directories on HDFS..."
    hdfs dfs -mkdir -p /tmp/hadoop-yarn/staging/history/done_intermediate 2>/dev/null || true
    hdfs dfs -mkdir -p /user/root 2>/dev/null || true
    hdfs dfs -chmod -R 777 /tmp 2>/dev/null || true
    echo "  HDFS directories ready."

    # Start YARN ResourceManager
    echo "Starting YARN ResourceManager..."
    yarn resourcemanager &
    YARN_PID=$!
    echo "  YARN RM started (PID=$YARN_PID)"

    echo "======================================================"
    echo "  NameNode PID=$NAMENODE_PID | YARN RM PID=$YARN_PID"
    echo "  NameNode UI : http://localhost:9870"
    echo "  YARN RM UI  : http://localhost:8088"
    echo "======================================================"

    # Giám sát: container sống chừng nào NameNode còn sống
    # YARN được phép restart mà không kéo chết container
    wait $NAMENODE_PID
    echo "NameNode exited. Shutting down container."

# ── DataNode + NodeManager ─────────────────────────────────
# FIX: thêm NodeManager để YARN ResourceManager có node đăng ký.
#      Không có NodeManager → YARN RM khởi động nhưng không có
#      worker nào → RM tự crash hoặc treo → port 8088 chết.
elif [ "${HADOOP_ROLE}" = "datanode" ]; then

    mkdir -p "${DATANODE_DIR}" /hadoop/tmp "${HADOOP_HOME}/logs"

    # ── FIX: Inject Tailscale IP vào dfs.datanode.address ─────────
    # DataNode không tự detect được IP đúng trên máy worker: routing phức tạp
    # khiến nó detect nhầm IP public (vd: 151.101.64.223 của Fastly CDN).
    # Giải pháp: set tường minh qua DATANODE_HOST (= Tailscale IP của worker).
    # entrypoint sed placeholder DATANODE_HOST_PLACEHOLDER trong hdfs-site.xml
    # thành IP thực trước khi DataNode start.
    # Luôn làm trước YARN override để HADOOP_CONF_DIR được set đúng thứ tự.
    if [ -z "${DATANODE_HOST:-}" ]; then
        echo "ERROR: Biến DATANODE_HOST chưa được set."
        echo "  Thêm DATANODE_HOST=<WORKER_TAILSCALE_IP> vào environment"
        echo "  của service datanode trong docker-compose.worker.yml."
        echo "  Lấy IP: tailscale ip -4  (trên máy worker)"
        exit 1
    fi

    echo "Injecting DATANODE_HOST=${DATANODE_HOST} vào hdfs-site.xml..."
    mkdir -p /hadoop/conf-override
    cp -r "${HADOOP_HOME}/etc/hadoop/." /hadoop/conf-override/
    sed -i "s|DATANODE_HOST_PLACEHOLDER|${DATANODE_HOST}|g" /hadoop/conf-override/hdfs-site.xml
    export HADOOP_CONF_DIR=/hadoop/conf-override
    echo "  dfs.datanode.address      → ${DATANODE_HOST}:9866"
    echo "  dfs.datanode.http.address → ${DATANODE_HOST}:9864"
    echo "  dfs.datanode.ipc.address  → ${DATANODE_HOST}:9867"
    echo "  HADOOP_CONF_DIR=${HADOOP_CONF_DIR}"

    # ── Override YARN NodeManager memory từ env var ────────────────
    # HADOOP_CONF_DIR đã được set ở trên → sed trực tiếp vào conf-override.
    # Không cần copy lại lần nữa.
    if [ -n "${YARN_NODEMANAGER_RESOURCE_MEMORY_MB:-}" ]; then
        echo "Overriding YARN NM memory → ${YARN_NODEMANAGER_RESOURCE_MEMORY_MB}MB"
        sed -i "/<name>yarn.nodemanager.resource.memory-mb<\/name>/{n; s|<value>[0-9]*</value>|<value>${YARN_NODEMANAGER_RESOURCE_MEMORY_MB}</value>|}" \
            /hadoop/conf-override/yarn-site.xml
        sed -i "/<name>yarn.scheduler.maximum-allocation-mb<\/name>/{n; s|<value>[0-9]*</value>|<value>${YARN_NODEMANAGER_RESOURCE_MEMORY_MB}</value>|}" \
            /hadoop/conf-override/yarn-site.xml
        echo "  YARN NM memory overridden → ${YARN_NODEMANAGER_RESOURCE_MEMORY_MB}MB"
    fi

    # Ưu tiên dùng SERVICE_PRECONDITION (format: "HOST:PORT") để wait
    # Khi chạy trên worker machine, SERVICE_PRECONDITION="${MASTER_TS_IP}:9870"
    # → tránh wait bằng hostname "namenode" có thể chưa resolve kịp
    if [ -n "${SERVICE_PRECONDITION:-}" ]; then
        WAIT_HOST=$(echo "$SERVICE_PRECONDITION" | cut -d: -f1)
        WAIT_PORT=$(echo "$SERVICE_PRECONDITION" | cut -d: -f2)
        echo "Waiting for NameNode via SERVICE_PRECONDITION: ${WAIT_HOST}:${WAIT_PORT}"
        wait_for_service "${WAIT_HOST}" "${WAIT_PORT}" 40 5
    else
        wait_for_service namenode 9870 40 5
    fi

    echo "Starting DataNode..."
    hdfs datanode &
    DATANODE_PID=$!

    echo "Starting NodeManager..."
    yarn nodemanager &
    NODEMANAGER_PID=$!
    echo "  NodeManager started (PID=$NODEMANAGER_PID)"

    echo "======================================================"
    echo "  DataNode PID=$DATANODE_PID | NodeManager PID=$NODEMANAGER_PID"
    echo "  NodeManager UI : http://$(hostname):8042"
    echo "======================================================"

    # Container sống chừng nào DataNode còn sống
    wait $DATANODE_PID
    echo "DataNode exited. Shutting down container."

# ── Unknown role ───────────────────────────────────────────
else
    echo "ERROR: Unknown HADOOP_ROLE='${HADOOP_ROLE}'"
    echo "  Valid values: namenode, datanode"
    exit 1
fi