#!/usr/bin/env bash
# ============================================================
#  start-worker.sh — Khởi động Worker node
#  Tương thích: Git Bash (Windows), WSL2, macOS, Linux
# ============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"

# ── Kiểm tra .env ─────────────────────────────────────────────
if [ ! -f "$DOCKER_DIR/.env" ]; then
    echo ""
    echo "  ERROR: Thiếu file .env"
    echo "  Chạy: cp $DOCKER_DIR/.env.example $DOCKER_DIR/.env"
    echo "  Rồi điền MASTER_TS_IP và WORKER_ID vào .env"
    echo ""
    exit 1
fi

set -a
# Tính SPARK_WORKER_PORT từ WORKER_ID nếu chưa set trong .env
# worker-1 → 8081, worker-2 → 8082, ...
# shellcheck disable=SC1090
source "$DOCKER_DIR/.env"
set +a

# Tự tính SPARK_WORKER_PORT nếu chưa có trong .env
if [ -z "${SPARK_WORKER_PORT:-}" ]; then
    SPARK_WORKER_PORT="808${WORKER_ID:-1}"
fi

# ── Kiểm tra port có đang bị chiếm trên máy host không ────────
# Nếu bị chiếm (vd: 8082 luôn in-use) → fallback sang 8083.
# Dùng bash /dev/tcp (Git Bash, Linux, macOS, WSL2 đều hỗ trợ).
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

# ── Kiểm tra biến bắt buộc ───────────────────────────────────
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

# ── Detect tool kiểm tra kết nối (ưu tiên curl vì Git Bash luôn có) ──
#
#  Thứ tự ưu tiên: curl → nc → bỏ qua
#  Lý do dùng curl thay ping/nc:
#    - ping  : Git Bash dùng Windows ping.exe, flag khác Linux → không dùng được
#    - nc    : Git Bash KHÔNG có sẵn, phải cài thêm MinGW
#    - curl  : Git Bash LUÔN có sẵn (đi kèm Git for Windows)
#
CHECK_TOOL=""
if command -v curl > /dev/null 2>&1; then
    CHECK_TOOL="curl"
elif command -v nc > /dev/null 2>&1; then
    CHECK_TOOL="nc"
fi

# Hàm kiểm tra 1 host:port
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
        return 0   # không có tool → giả sử OK, tiếp tục
    fi
}

# ── [1/4] Kiểm tra kết nối Tailscale ────────────────────────
echo ""
echo "  [1/4] Kiểm tra kết nối Tailscale đến $MASTER_TS_IP..."

if [ -z "$CHECK_TOOL" ]; then
    echo "  WARNING: Không tìm thấy curl hay nc — bỏ qua kiểm tra."
    echo "  Đảm bảo Tailscale đang chạy và master đã start trước khi tiếp tục."
elif check_port "$MASTER_TS_IP" 9870 5; then
    echo "  Kết nối OK ✓ (NameNode port 9870 respond)"
elif check_port "$MASTER_TS_IP" 8080 5; then
    echo "  Kết nối OK ✓ (Spark port 8080 respond)"
else
    # Thử port 9092 (Kafka) — master có nhiều port exposed
    if ! check_port "$MASTER_TS_IP" 9092 5 && ! check_port "$MASTER_TS_IP" 8888 5; then
        echo ""
        echo "  WARNING: Không kết nối được đến $MASTER_TS_IP"
        echo ""
        echo "  Kiểm tra theo thứ tự:"
        echo "    1. Tailscale đang chạy:  tailscale status"
        echo "    2. Máy master đang online và đã join cùng tailnet"
        echo "    3. IP đúng chưa — hỏi bạn master chạy: tailscale ip -4"
        echo "    4. Master đã chạy start-master.sh chưa?"
        echo "       (master cần start TRƯỚC worker)"
        echo ""
        echo "  Tiếp tục sau 5 giây..."
        sleep 5
    else
        echo "  Tailscale OK ✓ (host reachable nhưng NameNode chưa start)"
    fi
fi

# ── [2/4] Đợi NameNode sẵn sàng ─────────────────────────────
echo ""
echo "  [2/4] Đợi NameNode tại $MASTER_TS_IP:9870..."
echo "  (Đợi tối đa 3 phút — nếu master chưa start hãy chạy start-master.sh trên máy master)"

if [ -n "$CHECK_TOOL" ]; then
    TIMEOUT=180
    ELAPSED=0
    until check_port "$MASTER_TS_IP" 9870 3; do
        if [ "$ELAPSED" -ge "$TIMEOUT" ]; then
            echo ""
            echo "  ERROR: NameNode $MASTER_TS_IP:9870 không phản hồi sau ${TIMEOUT}s"
            echo ""
            echo "  Kiểm tra trên máy master:"
            echo "    docker compose -f docker-compose.master.yml ps"
            echo "    docker compose -f docker-compose.master.yml logs namenode"
            echo ""
            exit 1
        fi
        printf "  Chờ NameNode... (%ds/%ds)\r" "$ELAPSED" "$TIMEOUT"
        sleep 5
        ELAPSED=$((ELAPSED + 5))
    done
    echo "  NameNode sẵn sàng ✓                        "
else
    echo "  (Không có curl/nc — bỏ qua, tiếp tục sau 5 giây)"
    sleep 5
fi

# ── [3/4] Build images ───────────────────────────────────────
echo ""
echo "  [3/4] Build Docker images (lần đầu mất 5-10 phút)..."
docker compose -f "$DOCKER_DIR/docker-compose.worker.yml" \
    --env-file "$DOCKER_DIR/.env" \
    build --quiet 2>/dev/null || {
    echo "  (Build không cần thiết hoặc đã có sẵn — tiếp tục)"
}

# ── [4/4] Start worker ────────────────────────────────────────
echo ""
echo "  [4/4] Khởi động DataNode + Spark Worker $WORKER_ID..."
docker compose -f "$DOCKER_DIR/docker-compose.worker.yml" \
    --env-file "$DOCKER_DIR/.env" \
    up -d

# ── Thông tin sau khi start ───────────────────────────────────
WORKER_HOST="$(hostname 2>/dev/null || echo 'localhost')"

echo ""
echo "============================================================"
echo "  Worker $WORKER_ID đã start! Chờ 20-30 giây rồi kiểm tra."
echo ""
echo "  Xem log realtime (để biết DataNode đã đăng ký chưa):"
echo "    docker compose -f docker-compose.worker.yml logs -f"
echo ""
echo "  UI trên máy này (dùng Tailscale IP hoặc localhost):"
echo "    DataNode UI  : http://$WORKER_HOST:9864"
echo "    Spark Worker : http://$WORKER_HOST:$SPARK_WORKER_PORT"
echo "    NodeManager  : http://$WORKER_HOST:8042"
echo ""
echo "  Kiểm tra đăng ký trên master:"
echo "    HDFS  → http://$MASTER_TS_IP:9870  (tab Datanodes)"
echo "    YARN  → http://$MASTER_TS_IP:8088/cluster/nodes"
echo "    Spark → http://$MASTER_TS_IP:8080  (Workers)"
echo "============================================================"
