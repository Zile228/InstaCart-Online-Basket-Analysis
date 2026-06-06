#!/bin/bash
# ============================================================
#  Hadoop Entrypoint — detects HADOOP_ROLE and starts service
#  HADOOP_ROLE=namenode → format (once) + start NameNode + YARN RM
#  HADOOP_ROLE=datanode → wait for NameNode + start DataNode
# ============================================================

# KHÔNG dùng set -e: script quản lý nhiều process nền,
# một lệnh phụ fail không nên kéo chết toàn bộ container.
set -u

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
    wait_for_service namenode 9870 40 5

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
