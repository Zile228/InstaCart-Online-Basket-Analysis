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

    # ── Override datanode address/hostname/port qua HADOOP_CONF_DIR (XML) ──
    # PHÁT HIỆN MỚI (sau khi test dfs.datanode.hostname): KHÔNG hiệu quả.
    # DatanodeID.ipAddr mà NameNode lưu được set từ ĐỊA CHỈ IP CỦA RPC
    # CONNECTION THỰC TẾ (Server.getRemoteIp() phía NameNode khi nhận
    # registerDatanode()), KHÔNG đọc từ config phía DataNode. Trên Docker
    # Desktop (Windows/macOS), traffic từ MỌI container worker machine đều
    # bị NAT về cùng 1 IP nội bộ "192.168.65.1" — không có cách nào từ phía
    # DataNode khai báo lại IP này.
    #
    # FIX THỰC SỰ: NameNode định danh datanode theo key (ipAddr:xferPort).
    # Nếu IP buộc phải trùng (192.168.65.1), thì PORT phải khác nhau giữa
    # các worker để tránh đụng key. Dùng WORKER_ID để offset port:
    #   WORKER_ID=1 → 9866/9864/9867 (giữ nguyên default)
    #   WORKER_ID=2 → 9876/9874/9877 (+10)
    #   WORKER_ID=N → port mặc định + (N-1)*10
    # Container vẫn LISTEN trên các port này, và docker-compose.worker.yml
    # publish đúng port tương ứng ra host.
    if [ -z "${DATANODE_HOST:-}" ]; then
        echo "ERROR: Biến DATANODE_HOST chưa được set."
        echo "  Thêm DATANODE_HOST=<WORKER_TAILSCALE_IP> vào environment"
        echo "  của service datanode trong docker-compose.worker.yml."
        echo "  Lấy IP: tailscale ip -4  (trên máy worker)"
        exit 1
    fi

<<<<<<< HEAD
    # ── Override datanode address/hostname qua HADOOP_CONF_DIR (XML) ──
    # SỬA: -Ddfs.datanode.address qua HADOOP_DATANODE_OPTS có hiệu lực
    # (Hadoop có cơ chế riêng đọc property này từ system property trong code
    # DataNode.java), NHƯNG -Ddfs.datanode.hostname KHÔNG có cơ chế tương tự
    # → bị bỏ qua hoàn toàn → DataNode tự lấy IP từ socket connection (NAT'd
    # 192.168.65.1) làm registration hostname → 2 worker trùng nhau → loop
    # DNA_REGISTER vô hạn (mỗi bên ghi đè bên kia mỗi ~3s).
    #
    # Fix: thêm dfs.datanode.address VÀ dfs.datanode.hostname vào file XML
    # thật (qua HADOOP_CONF_DIR override), để Configuration object có giá trị
    # đúng ngay từ đầu — không phụ thuộc system property.
=======
    WID="${WORKER_ID:-1}"
    PORT_OFFSET=$(( (WID - 1) * 10 ))
    DN_XFER_PORT="${DN_XFER_PORT:-$((9866 + PORT_OFFSET))}"
    DN_HTTP_PORT="${DN_HTTP_PORT:-$((9864 + PORT_OFFSET))}"
    DN_IPC_PORT="${DN_IPC_PORT:-$((9867 + PORT_OFFSET))}"

>>>>>>> 56c15afbd835e5fd3a6caf8167dc4bf894023c5b
    mkdir -p /hadoop/conf-override
    cp -r "${HADOOP_HOME}/etc/hadoop/." /hadoop/conf-override/

    DN_HOSTNAME="$(hostname)"

    # Thêm/ghi đè dfs.datanode.hostname + dfs.datanode.address/http/ipc vào hdfs-site.xml override
    for ENTRY in \
        "dfs.datanode.hostname|${DN_HOSTNAME}" \
<<<<<<< HEAD
        "dfs.datanode.address|${DATANODE_HOST}:9866" \
        "dfs.datanode.http.address|${DATANODE_HOST}:9864" \
        "dfs.datanode.ipc.address|${DATANODE_HOST}:9867"
=======
        "dfs.datanode.address|${DATANODE_HOST}:${DN_XFER_PORT}" \
        "dfs.datanode.http.address|${DATANODE_HOST}:${DN_HTTP_PORT}" \
        "dfs.datanode.ipc.address|${DATANODE_HOST}:${DN_IPC_PORT}"
>>>>>>> 56c15afbd835e5fd3a6caf8167dc4bf894023c5b
    do
        NAME="${ENTRY%%|*}"
        VALUE="${ENTRY##*|}"
        if grep -q "<name>${NAME}</name>" /hadoop/conf-override/hdfs-site.xml; then
            # Property đã tồn tại → sed giá trị
            sed -i "/<name>${NAME}<\/name>/{n; s|<value>.*</value>|<value>${VALUE}</value>|}" \
                /hadoop/conf-override/hdfs-site.xml
        else
            # Chưa tồn tại → chèn property mới trước </configuration>
            sed -i "s|</configuration>|  <property>\n    <name>${NAME}</name>\n    <value>${VALUE}</value>\n  </property>\n</configuration>|" \
                /hadoop/conf-override/hdfs-site.xml
        fi
    done

    export HADOOP_CONF_DIR=/hadoop/conf-override
<<<<<<< HEAD
    echo "DataNode sẽ advertise:"
    echo "  dfs.datanode.hostname     → ${DN_HOSTNAME}   (SỬA: NameNode dùng hostname"
    echo "                                này để định danh node, tránh trùng IP NAT"
    echo "                                192.168.65.1 giữa các máy worker khác nhau)"
    echo "  dfs.datanode.address      → ${DATANODE_HOST}:9866"
    echo "  dfs.datanode.http.address → ${DATANODE_HOST}:9864"
    echo "  dfs.datanode.ipc.address  → ${DATANODE_HOST}:9867"
=======
    echo "DataNode (WORKER_ID=${WID}) sẽ advertise:"
    echo "  dfs.datanode.hostname     → ${DN_HOSTNAME}"
    echo "  dfs.datanode.address      → ${DATANODE_HOST}:${DN_XFER_PORT}"
    echo "  dfs.datanode.http.address → ${DATANODE_HOST}:${DN_HTTP_PORT}"
    echo "  dfs.datanode.ipc.address  → ${DATANODE_HOST}:${DN_IPC_PORT}"
    echo "  (SỬA: port lệch +${PORT_OFFSET} theo WORKER_ID — NameNode định danh"
    echo "   datanode theo IP:PORT, port khác nhau giúp phân biệt 2 worker dù"
    echo "   IP đều bị Docker Desktop NAT thành 192.168.65.1)"
>>>>>>> 56c15afbd835e5fd3a6caf8167dc4bf894023c5b
    echo "  HADOOP_CONF_DIR=${HADOOP_CONF_DIR}"

    # ── Override YARN NodeManager memory từ env var ────────────────
    # /hadoop/conf-override đã được tạo ở bước trên (datanode hostname fix),
    # chỉ cần sed thêm vào yarn-site.xml trong cùng thư mục — KHÔNG cp -r lại
    # (sẽ xóa mất các sửa đổi hdfs-site.xml ở trên).
    if [ -n "${YARN_NODEMANAGER_RESOURCE_MEMORY_MB:-}" ]; then
        echo "Overriding YARN NM memory → ${YARN_NODEMANAGER_RESOURCE_MEMORY_MB}MB"
        sed -i "/<name>yarn.nodemanager.resource.memory-mb<\/name>/{n; s|<value>[0-9]*<\/value>|<value>${YARN_NODEMANAGER_RESOURCE_MEMORY_MB}<\/value>|}" \
            /hadoop/conf-override/yarn-site.xml
        sed -i "/<name>yarn.scheduler.maximum-allocation-mb<\/name>/{n; s|<value>[0-9]*<\/value>|<value>${YARN_NODEMANAGER_RESOURCE_MEMORY_MB}<\/value>|}" \
            /hadoop/conf-override/yarn-site.xml
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