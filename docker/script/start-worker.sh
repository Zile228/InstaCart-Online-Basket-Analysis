#!/usr/bin/env bash
# ============================================================
#  start-worker.sh - Khởi động Worker node
#  Tương thích: Git Bash (Windows), WSL2, macOS, Linux
# ============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"

# --- Kiểm tra sự tồn tại của file .env ---
if [ ! -f "$DOCKER_DIR/.env" ]; then
    echo ""
    echo "  ERROR: Thiếu file .env"
    echo "  Chạy: cp $DOCKER_DIR/.env.example $DOCKER_DIR/.env"
    echo "  Rồi điền MASTER_TS_IP và WORKER_ID vào .env"
    echo ""
    exit 1
fi

set -a
# Tự động tính SPARK_WORKER_PORT từ WORKER_ID nếu chưa cấu hình trong .env
# worker-1 -> 8081, worker-2 -> 8082, ...
# shellcheck disable=SC1090
source "$DOCKER_DIR/.env"
set +a

# Tự tính SPARK_WORKER_PORT nếu chưa có trong .env
if [ -z "${SPARK_WORKER_PORT:-}" ]; then
    SPARK_WORKER_PORT="808${WORKER_ID:-1}"
fi

# --- Kiểm tra cổng (port) có bị trùng lặp trên máy host không ---
# Nếu cổng bị chiếm (ví dụ 8082) -> chuyển sang cổng dự phòng khác.
is_port_in_use() {
    local port=$1
    (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null && { exec 3>&- 2>/dev/null; exec 3<&- 2>/dev/null; return 0; }
    return 1
}

if is_port_in_use "$SPARK_WORKER_PORT"; then
    FALLBACK_PORT=83
    echo ""
    echo "  WARNING: Port $SPARK_WORKER_PORT đang bị chiếm trên máy này."
    echo "  Fallback sang port $FALLBACK_PORT cho Spark Worker Web UI."
    SPARK_WORKER_PORT="$FALLBACK_PORT"
fi

export SPARK_WORKER_PORT

# --- Kiểm tra các biến cấu hình bắt buộc ---
if [ -z "${MASTER_TS_IP:-}" ] || [ "$MASTER_TS_IP" = "100.x.x.x" ]; then
    echo ""
    echo "  ERROR: Chưa điền MASTER_TS_IP trong .env"
    echo "  Lấy Tailscale IP của máy master từ bạn trong nhóm:"
    echo "    (bạn master chạy: tailscale ip -4)"
    echo ""
    exit 1
fi

if [ -z "${WORKER_ID:-}" ]; then
    echo ""
    echo "  ERROR: Chưa điền WORKER_ID trong .env"
    echo "  Đặt WORKER_ID=1 cho worker thứ nhất, WORKER_ID=2 cho thứ hai"
    echo ""
    exit 1
fi

echo ""
echo "============================================================"
echo "  Khởi động Worker $WORKER_ID"
echo "  Kết nối đến Master: $MASTER_TS_IP"
echo "============================================================"

# --- Lựa chọn công cụ kiểm tra kết nối mạng (Ưu tiên curl) ---
CHECK_TOOL=""
if command -v curl > /dev/null 2>&1; then
    CHECK_TOOL="curl"
elif command -v nc > /dev/null 2>&1; then
    CHECK_TOOL="nc"
fi

# Hàm kiểm tra kết nối host:port
check_port() {
    local host=$1
    local port=$2
    local timeout=${3:-5}
    if [ "$CHECK_TOOL" = "curl" ]; then
        curl -s --connect-timeout "$timeout" --max-time "$timeout" \
            "http://$host:$port" -o /dev/null 2>/dev/null
        return $?
    elif [ "$CHECK_TOOL" = "nc" ]; then
        nc -z -w "$timeout" "$host" "$port" 2>/dev/null
        return $?
    else
        return 0   # không có tool -> bỏ qua kiểm tra cổng
    fi
}

# --- [1/4] Kiểm tra kết nối Tailscale ---
echo ""
echo "  [1/4] Kiểm tra kết nối Tailscale đến $MASTER_TS_IP..."

if [ -z "$CHECK_TOOL" ]; then
    echo "  WARNING: Không tìm thấy curl hay nc - bỏ qua kiểm tra kết nối."
    echo "  Đảm bảo Tailscale đang chạy và master đã start trước khi tiếp tục."
elif check_port "$MASTER_TS_IP" 9870 5; then
    echo "  Kết nối OK [OK] (NameNode phản hồi trên cổng 9870)"
elif check_port "$MASTER_TS_IP" 8080 5; then
    echo "  Kết nối OK [OK] (Spark phản hồi trên cổng 8080)"
else
    if ! check_port "$MASTER_TS_IP" 9092 5 && ! check_port "$MASTER_TS_IP" 8888 5; then
        echo ""
        echo "  WARNING: Không kết nối được đến $MASTER_TS_IP"
        echo ""
        echo "  Kiểm tra theo thứ tự:"
        echo "    1. Tailscale đang hoạt động: tailscale status"
        echo "    2. Máy master đang online và kết nối cùng dải Tailscale"
        echo "    3. Địa chỉ IP chính xác chưa - chạy 'tailscale ip -4' trên master"
        echo "    4. Master đã khởi chạy tập lệnh start-master.sh chưa"
        echo ""
        echo "  Tiếp tục sau 5 giây..."
        sleep 5
    else
        echo "  Tailscale OK [OK] (Máy chủ phản hồi nhưng dịch vụ chưa sẵn sàng)"
    fi
fi

# --- [2/4] Chờ dịch vụ NameNode sẵn sàng ---
echo ""
echo "  [2/4] Đợi NameNode tại $MASTER_TS_IP:9870..."
echo "  (Đợi tối đa 3 phút - đảm bảo máy master đã chạy start-master.sh)"

if [ -n "$CHECK_TOOL" ]; then
    TIMEOUT=180
    ELAPSED=0
    until check_port "$MASTER_TS_IP" 9870 3; do
        if [ "$ELAPSED" -ge "$TIMEOUT" ]; then
            echo ""
            echo "  ERROR: NameNode $MASTER_TS_IP:9870 không phản hồi sau ${TIMEOUT}s"
            echo ""
            echo "  Kiểm tra trạng thái NameNode trên máy master."
            echo ""
            exit 1
        fi
        printf "  Chờ NameNode... (%ds/%ds)\r" "$ELAPSED" "$TIMEOUT"
        sleep 5
        ELAPSED=$((ELAPSED + 5))
    done
    echo "  NameNode sẵn sàng [OK]                     "
else
    echo "  (Không tìm thấy curl/nc - tiếp tục sau 5 giây)"
    sleep 5
fi

# --- [3/4] Build các Docker image ---
echo ""
echo "  [3/4] Build Docker images (bỏ qua nếu đã hoàn tất hoặc đã có)..."
docker compose -f "$DOCKER_DIR/docker-compose.worker.yml" \
    --env-file "$DOCKER_DIR/.env" \
    build --quiet 2>/dev/null || {
    echo "  (Build hoàn tất hoặc sử dụng cấu hình sẵn có - tiếp tục)"
}

# --- [4/4] Khởi động DataNode và Spark Worker ---
echo ""
echo "  [4/4] Khởi động DataNode + Spark Worker $WORKER_ID..."
docker compose -f "$DOCKER_DIR/docker-compose.worker.yml" \
    --env-file "$DOCKER_DIR/.env" \
    up -d

# --- Hiển thị thông tin kết nối sau khi khởi chạy ---
WORKER_HOST="$(hostname 2>/dev/null || echo 'localhost')"

echo ""
echo "============================================================"
echo "  Worker $WORKER_ID đã khởi chạy! Chờ 20-30 giây để đồng bộ."
echo ""
echo "  Theo dõi nhật ký hoạt động (Logs):"
echo "    docker compose -f docker-compose.worker.yml logs -f"
echo ""
echo "  Giao diện Web UI trên máy này:"
echo "    DataNode UI  : http://$WORKER_HOST:9864"
echo "    Spark Worker : http://$WORKER_HOST:$SPARK_WORKER_PORT"
echo "    NodeManager  : http://$WORKER_HOST:8042"
echo ""
echo "  Kiểm tra đăng ký kết nối trên máy master:"
echo "    HDFS  -> http://$MASTER_TS_IP:9870  (mục Datanodes)"
echo "    YARN  -> http://$MASTER_TS_IP:8088/cluster/nodes"
echo "    Spark -> http://$MASTER_TS_IP:8080  (danh sách Workers)"
echo "============================================================"