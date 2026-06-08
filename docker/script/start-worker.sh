#!/usr/bin/env bash
# ============================================================
#  start-worker.sh — Khởi động Worker node
#
#  Usage:
#    ./scripts/start-worker.sh
#
#  Yêu cầu trong .env trên máy worker:
#    MASTER_TS_IP   — Tailscale IP của máy MASTER
#    WORKER_ID      — 1 hoặc 2
#    WORKER_MEM     — RAM container (vd: 4G)
#    WORKER_CORES   — Số core Spark Worker (vd: 2)
#    YARN_MEM_MB    — RAM YARN NodeManager (vd: 3072)
#
#  Thứ tự khởi động BẮT BUỘC:
#    1. Chạy start-master.sh trên máy master TRƯỚC
#    2. Chờ NameNode sẵn sàng (http://MASTER_TS_IP:9870)
#    3. Chạy start-worker.sh trên máy worker
#
#  Hoạt động trên: macOS, WSL2 (Windows), Linux
# ============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"

# ── Kiểm tra .env tồn tại ─────────────────────────────────────
if [ ! -f "$DOCKER_DIR/.env" ]; then
    echo ""
    echo "  ERROR: Thiếu file .env"
    echo "  Xem .env.example để biết cách cấu hình:"
    echo "    cp $DOCKER_DIR/.env.example $DOCKER_DIR/.env"
    echo ""
    echo "  Điền vào .env:"
    echo "    MASTER_TS_IP = Tailscale IP của máy master (lấy từ bạn master)"
    echo "    WORKER_ID    = 1 hoặc 2"
    echo ""
    exit 1
fi

# ── Load biến môi trường từ .env ──────────────────────────────
set -a
# shellcheck disable=SC1090
source "$DOCKER_DIR/.env"
set +a

# ── Kiểm tra các biến bắt buộc ───────────────────────────────
if [ -z "${MASTER_TS_IP:-}" ] || [ "$MASTER_TS_IP" = "100.x.x.x" ]; then
    echo ""
    echo "  ERROR: Chưa điền MASTER_TS_IP trong .env"
    echo "  Lấy Tailscale IP của máy master từ bạn trong nhóm."
    echo ""
    exit 1
fi

if [ -z "${WORKER_ID:-}" ]; then
    echo ""
    echo "  ERROR: Chưa điền WORKER_ID trong .env"
    echo "  Đặt WORKER_ID=1 cho worker thứ nhất, WORKER_ID=2 cho worker thứ hai."
    echo ""
    exit 1
fi

echo ""
echo "============================================================"
echo "  Khởi động Worker $WORKER_ID"
echo "  Kết nối đến Master: $MASTER_TS_IP"
echo "============================================================"

# ── Kiểm tra kết nối Tailscale đến master ────────────────────
echo ""
echo "  [1/4] Kiểm tra kết nối Tailscale đến $MASTER_TS_IP..."

# Dùng nc thay ping vì:
#   - ping -W (timeout) không có trên macOS (dùng -t)
#   - ping có thể bị chặn bởi firewall/Tailscale ACL
#   - nc -z kiểm tra port trực tiếp, tin cậy hơn trên mọi OS
NC_OK=false
if command -v nc > /dev/null 2>&1; then
    # Thử kết nối port 9870 (NameNode Web UI) để xác nhận Tailscale hoạt động
    if nc -z -w 3 "$MASTER_TS_IP" 9870 2>/dev/null; then
        echo "  Kết nối Tailscale OK ✓ (port 9870 respond)"
        NC_OK=true
    elif nc -z -w 3 "$MASTER_TS_IP" 22 2>/dev/null || \
         nc -z -w 3 "$MASTER_TS_IP" 80 2>/dev/null; then
        echo "  Tailscale OK ✓ (host reachable — master service chưa start)"
        NC_OK=true
    else
        echo ""
        echo "  WARNING: Không kết nối được đến $MASTER_TS_IP"
        echo ""
        echo "  Kiểm tra theo thứ tự:"
        echo "    1. Tailscale đang chạy trên máy này:"
        echo "         tailscale status"
        echo "    2. Máy master đang online và đã join cùng tailnet"
        echo "    3. Đúng IP chưa — IP Tailscale của master:"
        echo "         (hỏi bạn master chạy: tailscale ip -4)"
        echo "    4. Master đã chạy start-master.sh chưa?"
        echo ""
        echo "  Tiếp tục sau 5 giây..."
        sleep 5
    fi
else
    echo "  WARNING: Lệnh 'nc' không tìm thấy."
    echo "  Trên Windows: chạy script trong WSL2 (không phải PowerShell/CMD)"
    echo "  Bỏ qua kiểm tra kết nối..."
fi

# ── Kiểm tra NameNode Web UI sẵn sàng ────────────────────────
echo ""
echo "  [2/4] Đợi NameNode tại $MASTER_TS_IP:9870..."
echo "  (Nếu master chưa start → script này sẽ đợi tối đa 3 phút)"

if command -v nc > /dev/null 2>&1; then
    TIMEOUT=180
    ELAPSED=0
    until nc -z "$MASTER_TS_IP" 9870 2>/dev/null; do
        if [ "$ELAPSED" -ge "$TIMEOUT" ]; then
            echo ""
            echo "  ERROR: NameNode tại $MASTER_TS_IP:9870 không phản hồi sau ${TIMEOUT}s"
            echo "  Kiểm tra master đã chạy: ./scripts/start-master.sh"
            exit 1
        fi
        echo "  Chờ NameNode... ($ELAPSED/${TIMEOUT}s)"
        sleep 5
        ELAPSED=$((ELAPSED + 5))
    done
    echo "  NameNode sẵn sàng ✓"
else
    echo "  WARNING: Lệnh 'nc' không có — bỏ qua kiểm tra NameNode."
    echo "  Trên Windows: chạy script này trong WSL2 hoặc Git Bash."
    echo "  Đảm bảo master đã chạy trước khi tiếp tục."
    sleep 5
fi

# ── Build images (nếu chưa có) ───────────────────────────────
echo ""
echo "  [3/4] Build/pull Docker images..."
docker compose -f "$DOCKER_DIR/docker-compose.worker.yml" \
    --env-file "$DOCKER_DIR/.env" \
    build --quiet 2>/dev/null || true

# ── Khởi động worker services ─────────────────────────────────
echo ""
echo "  [4/4] Khởi động DataNode + Spark Worker $WORKER_ID..."
docker compose -f "$DOCKER_DIR/docker-compose.worker.yml" \
    --env-file "$DOCKER_DIR/.env" \
    up -d

# ── Thông báo kết quả ─────────────────────────────────────────
echo ""
echo "============================================================"
echo "  Worker $WORKER_ID đang khởi động..."
echo "  Chờ 20-30 giây rồi kiểm tra:"
echo ""
echo "  DataNode UI  : http://$(hostname 2>/dev/null || echo 'localhost'):9864"
echo "  Spark Worker : http://$(hostname 2>/dev/null || echo 'localhost'):808${WORKER_ID}"
echo "  NodeManager  : http://$(hostname 2>/dev/null || echo 'localhost'):8042"
echo ""
echo "  Kiểm tra đăng ký trên master:"
echo "    HDFS:  http://$MASTER_TS_IP:9870/dfshealth.html#tab-datanode"
echo "    YARN:  http://$MASTER_TS_IP:8088/cluster/nodes"
echo "    Spark: http://$MASTER_TS_IP:8080"
echo ""
echo "  Xem log worker:"
echo "    docker compose -f docker-compose.worker.yml logs -f"
echo "============================================================"