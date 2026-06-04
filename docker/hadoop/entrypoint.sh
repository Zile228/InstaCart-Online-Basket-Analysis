#!/bin/bash
# ============================================================
#  Hadoop Entrypoint — detects HADOOP_ROLE and starts service
#  HADOOP_ROLE=namenode → format (once) + start NameNode
#  HADOOP_ROLE=datanode → wait for NameNode + start DataNode
# ============================================================

set -e

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
        echo "  [${count}/${MAX_TRIES}] ${HOST}:${PORT} not ready, retrying in ${WAIT_SECS}s..."
        sleep "${WAIT_SECS}"
    done
    echo "  ${HOST}:${PORT} is up!"
}

# ── NameNode ───────────────────────────────────────────────
# ── NameNode ───────────────────────────────────────────────
if [ "${HADOOP_ROLE}" = "namenode" ]; then

    mkdir -p "${NAMENODE_DIR}" /hadoop/tmp ${HADOOP_HOME}/logs

    if [ ! -f "${FORMAT_MARKER}" ]; then
        echo "Formatting HDFS NameNode for the first time..."
        hdfs namenode -format -force -nonInteractive
        touch "${FORMAT_MARKER}"
    else
        echo "NameNode already formatted (skipping)."
    fi

    echo "Starting NameNode..."
    hdfs namenode &               # ← chạy background

    echo "Starting YARN ResourceManager..."
    exec yarn resourcemanager     # ← chạy foreground (giữ container sống)

# ── DataNode ───────────────────────────────────────────────
elif [ "${HADOOP_ROLE}" = "datanode" ]; then

    mkdir -p "${DATANODE_DIR}" /hadoop/tmp ${HADOOP_HOME}/logs
    wait_for_service namenode 9870 40 5

    echo "Starting DataNode..."
    hdfs datanode &               # ← background

    echo "Starting YARN NodeManager..."
    exec yarn nodemanager         # ← foreground



# ── Unknown role ───────────────────────────────────────────
else
    echo "ERROR: Unknown HADOOP_ROLE='${HADOOP_ROLE}'"
    echo "  Valid values: namenode, datanode"
    exit 1
fi
