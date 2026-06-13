#!/bin/bash
# Entrypoint khoi chay Hadoop (phien ban single-node)
# - HADOOP_ROLE=namenode: format o lan dau chay, sau do bat NameNode va YARN RM
# - HADOOP_ROLE=datanode: cho NameNode san sang roi moi bat DataNode va NodeManager

set -u

HADOOP_VERSION="${HADOOP_VERSION:-3.4.3}"
HADOOP_ROLE="${HADOOP_ROLE:-namenode}"
NAMENODE_DIR="/hadoop/dfs/name"
DATANODE_DIR="/hadoop/dfs/data"
FORMAT_MARKER="${NAMENODE_DIR}/.formatted"

echo "------------------------------------------------"
echo "  Hadoop ${HADOOP_VERSION} - bat dau chay voi vai tro: ${HADOOP_ROLE}"
echo "------------------------------------------------"

# Ham kiem tra ket noi port de cho dich vu san sang
wait_for_service() {
    local HOST=$1
    local PORT=$2
    local MAX_TRIES=${3:-30}
    local WAIT_SECS=${4:-5}
    local count=0
    echo "Dang cho ket noi den ${HOST}:${PORT} ..."
    while ! nc -z "${HOST}" "${PORT}" 2>/dev/null; do
        count=$((count + 1))
        if [ "${count}" -ge "${MAX_TRIES}" ]; then
            echo "LOI: Da qua thoi gian cho phep de ket noi ${HOST}:${PORT}"
            exit 1
        fi
        echo "  [${count}/${MAX_TRIES}] Chua san sang, se thu lai sau ${WAIT_SECS}s..."
        sleep "${WAIT_SECS}"
    done
    echo "  ${HOST}:${PORT} da hoat dong!"
}

# --- Khoi chay NameNode va YARN ResourceManager ---
if [ "${HADOOP_ROLE}" = "namenode" ]; then

    mkdir -p "${NAMENODE_DIR}" /hadoop/tmp "${HADOOP_HOME}/logs"

    # Neu day la lan dau tien chay va chua co file marker, ta se tien hanh format NameNode
    if [ ! -f "${FORMAT_MARKER}" ]; then
        echo "Dang format HDFS NameNode..."
        hdfs namenode -format -force -nonInteractive
        touch "${FORMAT_MARKER}"
    else
        echo "NameNode da duoc format truoc do (bo qua buoc nay)."
    fi

    echo "Dang khoi dong NameNode..."
    hdfs namenode &
    NAMENODE_PID=$!

    echo "Cho NameNode Web UI len o port 9870..."
    count=0
    while ! nc -z "$(hostname)" 9870 2>/dev/null; do
        count=$((count + 1))
        if [ "$count" -ge 60 ]; then
            echo "LOI: NameNode khong khoi dong duoc sau 3 phut."
            exit 1
        fi
        echo "  [${count}/60] Chua san sang, thu lai sau 3s..."
        sleep 3
    done
    echo "  NameNode da len!"

    # Cho den khi HDFS thoat khoi trang thai Safe Mode
    echo "Cho HDFS thoat khoi Safe Mode..."
    count=0
    while true; do
        SAFEMODE=$(hdfs dfsadmin -safemode get 2>/dev/null || echo "error")
        if echo "$SAFEMODE" | grep -q "Safe mode is OFF"; then
            echo "  HDFS da an toan (Safe Mode is OFF)!"
            break
        fi
        count=$((count + 1))
        if [ "$count" -ge 30 ]; then
            echo "  Qua thoi gian cho Safe Mode - bat buoc phai thoat..."
            hdfs dfsadmin -safemode forceExit 2>/dev/null || true
            sleep 3
            break
        fi
        echo "  [${count}/30] Van dang trong Safe Mode, cho 5s..."
        sleep 5
    done

    # Tao san cac thu muc can thiet cho YARN tren HDFS
    echo "Tao cac thu muc cho YARN tren HDFS..."
    hdfs dfs -mkdir -p /tmp/hadoop-yarn/staging/history/done_intermediate 2>/dev/null || true
    hdfs dfs -mkdir -p /user/root 2>/dev/null || true
    hdfs dfs -chmod -R 777 /tmp 2>/dev/null || true

    echo "Dang khoi dong YARN ResourceManager..."
    yarn resourcemanager &
    YARN_PID=$!

    echo "------------------------------------------------"
    echo "  NameNode UI : http://localhost:9870"
    echo "  YARN RM UI  : http://localhost:8088"
    echo "------------------------------------------------"

    wait $NAMENODE_PID
    echo "NameNode da dung hoat dong. Tat container."

# --- Khoi chay DataNode va YARN NodeManager ---
elif [ "${HADOOP_ROLE}" = "datanode" ]; then

    mkdir -p "${DATANODE_DIR}" /hadoop/tmp "${HADOOP_HOME}/logs"

    # Neu co khai bao RAM rieng cho NodeManager, ta ghi de vao file config luc run container
    if [ -n "${YARN_NODEMANAGER_RESOURCE_MEMORY_MB:-}" ]; then
        echo "Ghi de RAM cua YARN NM -> ${YARN_NODEMANAGER_RESOURCE_MEMORY_MB}MB"
        mkdir -p /hadoop/conf-override
        cp -r "${HADOOP_HOME}/etc/hadoop/." /hadoop/conf-override/
        sed -i "/<name>yarn.nodemanager.resource.memory-mb<\/name>/{n; s|<value>[0-9]*</value>|<value>${YARN_NODEMANAGER_RESOURCE_MEMORY_MB}</value>|}" \
            /hadoop/conf-override/yarn-site.xml
        sed -i "/<name>yarn.scheduler.maximum-allocation-mb<\/name>/{n; s|<value>[0-9]*</value>|<value>${YARN_NODEMANAGER_RESOURCE_MEMORY_MB}</value>|}" \
            /hadoop/conf-override/yarn-site.xml
        export HADOOP_CONF_DIR=/hadoop/conf-override
    fi

    # Cho cho NameNode chay hoan tat thi DataNode moi bat dau de tranh loi
    if [ -n "${SERVICE_PRECONDITION:-}" ]; then
        WAIT_HOST=$(echo "$SERVICE_PRECONDITION" | cut -d: -f1)
        WAIT_PORT=$(echo "$SERVICE_PRECONDITION" | cut -d: -f2)
        wait_for_service "${WAIT_HOST}" "${WAIT_PORT}" 40 5
    else
        wait_for_service namenode 9870 40 5
    fi

    echo "Dang khoi dong DataNode..."
    hdfs datanode &
    DATANODE_PID=$!

    echo "Dang khoi dong NodeManager..."
    yarn nodemanager &
    NODEMANAGER_PID=$!

    echo "------------------------------------------------"
    echo "  DataNode UI    : http://localhost:9864"
    echo "  NodeManager UI : http://localhost:8042"
    echo "------------------------------------------------"

    wait $DATANODE_PID
    echo "DataNode da dung hoat dong. Tat container."

else
    echo "LOI: Khong tim thay HADOOP_ROLE='${HADOOP_ROLE}'. Bien hop le: namenode, datanode"
    exit 1
fi