#!/usr/bin/env bash
# ============================================================
#  status.sh - Kiểm tra trạng thái toàn bộ cluster
#
#  Cách dùng: ./scripts/status.sh
#
#  Hỗ trợ trên: macOS, WSL2, Linux
#  Yêu cầu: nc (netcat), curl, python3
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"

# Tải cấu hình từ file .env (nếu có)
if [ -f "$DOCKER_DIR/.env" ]; then
    set -a
    # shellcheck disable=SC1090
    source "$DOCKER_DIR/.env"
    set +a
fi

MASTER="${MASTER_TS_IP:-localhost}"

# --- Kiểm tra sự tồn tại của công cụ nc (netcat) ---
# Nếu hệ thống không có sẵn nc -> chuyển sang dùng cơ chế bash /dev/tcp
CHECK_NC=true
if ! command -v nc > /dev/null 2>&1; then
    echo "  Hệ thống không có nc - chuyển sang chế độ dự phòng bash /dev/tcp"
    CHECK_NC=false
fi

# --- Hàm kiểm tra cổng kết nối TCP (sử dụng nc hoặc bash dự phòng) ---
tcp_check() {
    local host=$1 port=$2
    if [ "$CHECK_NC" = "true" ]; then
        nc -z "$host" "$port" 2>/dev/null
    else
        (bash -c "echo >/dev/tcp/$host/$port" 2>/dev/null)
    fi
}

# --- Hàm kiểm tra trạng thái hoạt động của một dịch vụ ---
check_service() {
    local name=$1
    local host=$2
    local port=$3
    local url=$4

    if tcp_check "$host" "$port"; then
        printf "  %-28s \033[32m[UP]\033[0m    %s\n" "$name" "$url"
    else
        printf "  %-28s \033[31m[DOWN]\033[0m  cổng $port không phản hồi\n" "$name"
    fi
}

echo ""
echo "============================================================"
echo "  Trạng thái Cluster - Master: $MASTER"
echo "============================================================"

echo ""
echo "--- Các dịch vụ trên máy Master ---"
check_service "NameNode Web UI"       "$MASTER" 9870 "http://$MASTER:9870"
check_service "NameNode RPC (HDFS)"   "$MASTER" 9000 "hdfs://$MASTER:9000"
check_service "YARN ResourceManager"  "$MASTER" 8088 "http://$MASTER:8088"
check_service "YARN RM Scheduler"     "$MASTER" 8032 "$MASTER:8032"
check_service "Spark Master UI"       "$MASTER" 8080 "http://$MASTER:8080"
check_service "Spark Master RPC"      "$MASTER" 7077 "spark://$MASTER:7077"
check_service "Jupyter"               "$MASTER" 8888 "http://$MASTER:8888"

# --- Danh sách HDFS DataNode đang đăng ký ---
echo ""
echo "--- HDFS DataNodes ---"
DN_JSON=$(curl -s --max-time 5 \
    "http://$MASTER:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState" 2>/dev/null)

if [ -n "$DN_JSON" ]; then
    LIVE=$(echo "$DN_JSON" | python3 -c \
        "import sys,json; d=json.load(sys.stdin)['beans'][0]; print(d.get('NumLiveDataNodes','?'))" 2>/dev/null)
    DEAD=$(echo "$DN_JSON" | python3 -c \
        "import sys,json; d=json.load(sys.stdin)['beans'][0]; print(d.get('NumDeadDataNodes','?'))" 2>/dev/null)
    STALE=$(echo "$DN_JSON" | python3 -c \
        "import sys,json; d=json.load(sys.stdin)['beans'][0]; print(d.get('NumStaleDataNodes','?'))" 2>/dev/null)
    printf "  Đang hoạt động: \033[32m%s\033[0m  |  Mất kết nối: \033[31m%s\033[0m  |  Chờ phản hồi: %s\n" \
        "${LIVE:-?}" "${DEAD:-?}" "${STALE:-?}"
    echo "  Chi tiết tại: http://$MASTER:9870/dfshealth.html#tab-datanode"
else
    echo "  Không lấy được thông tin (NameNode chưa sẵn sàng hoặc đã tắt)"
fi

# --- Danh sách các Spark Worker đang đăng ký ---
echo ""
echo "--- Spark Workers ---"
SPARK_JSON=$(curl -s --max-time 5 "http://$MASTER:8080/json/" 2>/dev/null)

if [ -n "$SPARK_JSON" ]; then
    python3 -c "
import sys, json
try:
    d = json.loads('''$SPARK_JSON''')
    alive  = d.get('aliveworkers', '?')
    cores  = d.get('cores', '?')
    mem    = d.get('memory', '?')
    status = d.get('status', '?')
    print(f'  Trạng thái: {status}  |  Số worker hoạt động: {alive}  |  Số Cores: {cores}  |  Bộ nhớ: {mem} MB')
    for w in d.get('workers', []):
        wstate = w.get('state','?')
        whost  = w.get('host','?')
        wcores = w.get('cores','?')
        wmem   = w.get('memory','?')
        print(f'    - {whost}  trạng thái={wstate}  cores={wcores}  bộ nhớ={wmem}MB')
except Exception as e:
    print(f'  Lỗi xử lý dữ liệu: {e}')
" 2>/dev/null || echo "  Không lấy được thông tin (Spark Master chưa sẵn sàng)"
else
    echo "  Không lấy được thông tin (Spark Master đã tắt hoặc không thể truy cập)"
fi

# --- Trạng thái các Docker Container nội bộ ---
echo ""
echo "--- Trạng thái các Docker Container nội bộ ---"
docker ps --format "  {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null \
    | grep -E "(namenode|spark|jupyter|datanode|virtual)" \
    | sed 's/\t/  /g' \
    || echo "  (Không có container nào đang chạy)"

echo ""
echo "============================================================"