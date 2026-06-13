#!/bin/bash
# ============================================================
#  Hadoop Entrypoint (single-node)
#  HADOOP_ROLE=namenode → format (once) + start NameNode + YARN RM
#  HADOOP_ROLE=datanode → wait for NameNode + start DataNode + NodeManager
# ============================================================

set -u

HADOOP_VERSION="${HADOOP_VERSION:-3.4.3}"
HADOOP_ROLE="${HADOOP_ROLE:-namenode}"
NAMENODE_DIR="/hadoop/dfs/name"
DATANODE_DIR="/hadoop/dfs/data"
FORMAT_MARKER="${NAMENODE_DIR}/.formatted"

echo "======================================================"
echo "  Hadoop ${HADOOP_VERSION} — starting as: ${HADOOP_ROLE}"
echo "======================================================"

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

# ── NameNode + YARN ResourceManager ───────────────────────────
if [ "${HADOOP_ROLE}" = "namenode" ]; then

    mkdir -p "${NAMENODE_DIR}" /hadoop/tmp "${HADOOP_HOME}/logs"

    if [ ! -f "${FORMAT_MARKER}" ]; then
        echo "Formatting HDFS NameNode..."
        hdfs namenode -format -force -nonInteractive
        touch "${FORMAT_MARKER}"
    else
        echo "NameNode already formatted (skipping)."
    fi

    echo "Starting NameNode..."
    hdfs namenode &
    NAMENODE_PID=$!

    echo "Waiting for NameNode Web UI on port 9870..."
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

    echo "Waiting for HDFS to exit Safe Mode..."
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

    echo "Preparing YARN directories on HDFS..."
    hdfs dfs -mkdir -p /tmp/hadoop-yarn/staging/history/done_intermediate 2>/dev/null || true
    hdfs dfs -mkdir -p /user/root 2>/dev/null || true
    hdfs dfs -chmod -R 777 /tmp 2>/dev/null || true

    echo "Starting YARN ResourceManager..."
    yarn resourcemanager &
    YARN_PID=$!

    echo "======================================================"
    echo "  NameNode UI : http://localhost:9870"
    echo "  YARN RM UI  : http://localhost:8088"
    echo "======================================================"

    wait $NAMENODE_PID
    echo "NameNode exited. Shutting down container."

# ── DataNode + YARN NodeManager ───────────────────────────────
elif [ "${HADOOP_ROLE}" = "datanode" ]; then

    mkdir -p "${DATANODE_DIR}" /hadoop/tmp "${HADOOP_HOME}/logs"

    # Override YARN NodeManager memory nếu có env var
    if [ -n "${YARN_NODEMANAGER_RESOURCE_MEMORY_MB:-}" ]; then
        echo "Overriding YARN NM memory → ${YARN_NODEMANAGER_RESOURCE_MEMORY_MB}MB"
        mkdir -p /hadoop/conf-override
        cp -r "${HADOOP_HOME}/etc/hadoop/." /hadoop/conf-override/
        sed -i "/<name>yarn.nodemanager.resource.memory-mb<\/name>/{n; s|<value>[0-9]*</value>|<value>${YARN_NODEMANAGER_RESOURCE_MEMORY_MB}</value>|}" \
            /hadoop/conf-override/yarn-site.xml
        sed -i "/<name>yarn.scheduler.maximum-allocation-mb<\/name>/{n; s|<value>[0-9]*</value>|<value>${YARN_NODEMANAGER_RESOURCE_MEMORY_MB}</value>|}" \
            /hadoop/conf-override/yarn-site.xml
        export HADOOP_CONF_DIR=/hadoop/conf-override
    fi

    # Chờ NameNode sẵn sàng
    if [ -n "${SERVICE_PRECONDITION:-}" ]; then
        WAIT_HOST=$(echo "$SERVICE_PRECONDITION" | cut -d: -f1)
        WAIT_PORT=$(echo "$SERVICE_PRECONDITION" | cut -d: -f2)
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

    echo "======================================================"
    echo "  DataNode UI    : http://localhost:9864"
    echo "  NodeManager UI : http://localhost:8042"
    echo "======================================================"

    wait $DATANODE_PID
    echo "DataNode exited. Shutting down container."

else
    echo "ERROR: Unknown HADOOP_ROLE='${HADOOP_ROLE}'. Valid: namenode, datanode"
    exit 1
fi
