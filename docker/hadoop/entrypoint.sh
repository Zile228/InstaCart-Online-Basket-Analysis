#!/bin/bash
# ============================================================
#  Hadoop Entrypoint - Nhận diện HADOOP_ROLE để khởi chạy dịch vụ phù hợp
#  HADOOP_ROLE=namenode -> Khởi tạo + chạy NameNode + YARN RM
#  HADOOP_ROLE=datanode -> Chờ NameNode + chạy DataNode
# ============================================================

# Không dùng set -e vì script quản lý nhiều tiến trình chạy ngầm,
# tránh việc một lệnh phụ bị lỗi làm tắt toàn bộ container.
set -u

# Dự phòng cho HADOOP_VERSION nếu không được export đúng cách.
HADOOP_VERSION="${HADOOP_VERSION:-3.4.3}"

HADOOP_ROLE="${HADOOP_ROLE:-namenode}"
NAMENODE_DIR="/hadoop/dfs/name"
DATANODE_DIR="/hadoop/dfs/data"
FORMAT_MARKER="${NAMENODE_DIR}/.formatted"

echo "======================================================"
echo "  Hadoop ${HADOOP_VERSION} - starting as: ${HADOOP_ROLE}"
echo "======================================================"

# --- Hàm chờ dịch vụ sẵn sàng (Wait function) ---
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

# --- Cấu hình cho NameNode ---
if [ "${HADOOP_ROLE}" = "namenode" ]; then

    mkdir -p "${NAMENODE_DIR}" /hadoop/tmp "${HADOOP_HOME}/logs"

    # Chỉ định dạng (format) ổ đĩa đúng một lần đầu tiên khi khởi tạo
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

    # Bước 1: Chờ NameNode Web UI hoạt động trên cổng 9870
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

    # Bước 2: Chờ HDFS thoát khỏi chế độ an toàn (Safe Mode)
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
            echo "  Safe Mode timeout - forcing exit..."
            hdfs dfsadmin -safemode forceExit 2>/dev/null || true
            sleep 3
            break
        fi
        echo "  [${count}/30] Still in Safe Mode, waiting 5s..."
        sleep 5
    done

    # Bước 3: Tạo các thư mục cần thiết cho YARN trên hệ thống HDFS
    echo "[3/3] Preparing YARN directories on HDFS..."
    hdfs dfs -mkdir -p /tmp/hadoop-yarn/staging/history/done_intermediate 2>/dev/null || true
    hdfs dfs -mkdir -p /user/root 2>/dev/null || true
    hdfs dfs -chmod -R 777 /tmp 2>/dev/null || true
    echo "  HDFS directories ready."

    # Khởi động YARN ResourceManager
    echo "Starting YARN ResourceManager..."
    yarn resourcemanager &
    YARN_PID=$!
    echo "  YARN RM started (PID=$YARN_PID)"

    echo "======================================================"
    echo "  NameNode PID=$NAMENODE_PID | YARN RM PID=$YARN_PID"
    echo "  NameNode UI : http://localhost:9870"
    echo "  YARN RM UI  : http://localhost:8088"
    echo "======================================================"

    # Giữ container hoạt động liên tục chừng nào tiến trình NameNode còn chạy
    wait $NAMENODE_PID
    echo "NameNode exited. Shutting down container."

# --- Cấu hình cho DataNode và NodeManager ---
elif [ "${HADOOP_ROLE}" = "datanode" ]; then

    mkdir -p "${DATANODE_DIR}" /hadoop/tmp "${HADOOP_HOME}/logs"

    # Tự động thay đổi cổng kết nối của DataNode dựa trên WORKER_ID để tránh xung đột
    if [ -z "${DATANODE_HOST:-}" ]; then
        echo "ERROR: Biến DATANODE_HOST chưa được set."
        echo "  Thêm DATANODE_HOST=<WORKER_TAILSCALE_IP> vào environment"
        echo "  của service datanode trong docker-compose.worker.yml."
        echo "  Lấy IP: tailscale ip -4  (trên máy worker)"
        exit 1
    fi

    WID="${WORKER_ID:-1}"
    PORT_OFFSET=$(( (WID - 1) * 10 ))
    DN_XFER_PORT="${DN_XFER_PORT:-$((9866 + PORT_OFFSET))}"
    DN_HTTP_PORT="${DN_HTTP_PORT:-$((9864 + PORT_OFFSET))}"
    DN_IPC_PORT="${DN_IPC_PORT:-$((9867 + PORT_OFFSET))}"

    mkdir -p /hadoop/conf-override
    cp -r "${HADOOP_HOME}/etc/hadoop/." /hadoop/conf-override/

    DN_HOSTNAME="$(hostname)"

    # Cập nhật địa chỉ và cổng mới vào file cấu hình hdfs-site.xml tạm thời
    for ENTRY in \
        "dfs.datanode.hostname|${DN_HOSTNAME}" \
        "dfs.datanode.address|0.0.0.0:${DN_XFER_PORT}" \
        "dfs.datanode.http.address|0.0.0.0:${DN_HTTP_PORT}" \
        "dfs.datanode.ipc.address|0.0.0.0:${DN_IPC_PORT}"
    do
        NAME="${ENTRY%%|*}"
        VALUE="${ENTRY##*|}"
        if grep -q "<name>${NAME}</name>" /hadoop/conf-override/hdfs-site.xml; then
            # Cập nhật giá trị nếu thuộc tính đã tồn tại
            sed -i "/<name>${NAME}<\/name>/{n; s|<value>.*</value>|<value>${VALUE}</value>|}" \
                /hadoop/conf-override/hdfs-site.xml
        else
            # Chèn thuộc tính mới vào ngay trước thẻ đóng cấu hình nếu chưa tồn tại
            sed -i "s|</configuration>|  <property>\n    <name>${NAME}</name>\n    <value>${VALUE}</value>\n  </property>\n</configuration>|" \
                /hadoop/conf-override/hdfs-site.xml
        fi
    done

    export HADOOP_CONF_DIR=/hadoop/conf-override
    echo "DataNode (WORKER_ID=${WID}) sẽ advertise:"
    echo "  dfs.datanode.hostname     -> ${DN_HOSTNAME}"
    echo "  dfs.datanode.address      -> 0.0.0.0:${DN_XFER_PORT}  (bind all; advertise via hostname)"
    echo "  dfs.datanode.http.address -> 0.0.0.0:${DN_HTTP_PORT}"
    echo "  dfs.datanode.ipc.address  -> 0.0.0.0:${DN_IPC_PORT}"
    echo "  (Thay đổi cổng +${PORT_OFFSET} theo WORKER_ID để tránh trùng lặp)"
    echo "  HADOOP_CONF_DIR=${HADOOP_CONF_DIR}"

    # Ghi đè cấu hình dung lượng RAM của YARN NodeManager từ biến môi trường
    if [ -n "${YARN_NODEMANAGER_RESOURCE_MEMORY_MB:-}" ]; then
        echo "Overriding YARN NM memory -> ${YARN_NODEMANAGER_RESOURCE_MEMORY_MB}MB"
        sed -i "/<name>yarn.nodemanager.resource.memory-mb<\/name>/{n; s|<value>[0-9]*<\/value>|<value>${YARN_NODEMANAGER_RESOURCE_MEMORY_MB}<\/value>|}" \
            /hadoop/conf-override/yarn-site.xml
        sed -i "/<name>yarn.scheduler.maximum-allocation-mb<\/name>/{n; s|<value>[0-9]*<\/value>|<value>${YARN_NODEMANAGER_RESOURCE_MEMORY_MB}<\/value>|}" \
            /hadoop/conf-override/yarn-site.xml
    fi

    # Chờ NameNode sẵn sàng dựa trên biến SERVICE_PRECONDITION nếu được thiết lập
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

    # Giữ container hoạt động liên tục chừng nào tiến trình DataNode còn chạy
    wait $DATANODE_PID
    echo "DataNode exited. Shutting down container."

# --- Trường hợp vai trò (role) không hợp lệ ---
else
    echo "ERROR: Unknown HADOOP_ROLE='${HADOOP_ROLE}'"
    echo "  Valid values: namenode, datanode"
    exit 1
fi