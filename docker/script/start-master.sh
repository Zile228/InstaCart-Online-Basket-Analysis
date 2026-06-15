#!/usr/bin/env bash
# ============================================================
#  start-master.sh - Khởi động Master node
#
#  Cách dùng:
#    ./scripts/start-master.sh                    # Không kèm virtual worker
#    ./scripts/start-master.sh --with-virtual-worker  # Kèm virtual worker
#
#  Yêu cầu:
#    - File .env đã khai báo MASTER_TS_IP
#    - Docker Desktop đang chạy
#    - Tailscale đang kết nối
#
#  Hỗ trợ trên: macOS, WSL2 (Windows), Linux
# ============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"

# --- Kiểm tra sự tồn tại của file .env ---
if [ ! -f "$DOCKER_DIR/.env" ]; then
    echo ""
    echo "  ERROR: Thiếu file .env"
    echo "  Chạy lệnh sau rồi điền MASTER_TS_IP:"
    echo "    cp $DOCKER_DIR/.env.example $DOCKER_DIR/.env"
    echo "    # Lấy Tailscale IP: tailscale ip -4"
    echo ""
    exit 1
fi

# --- Tải các biến môi trường từ file .env ---
set -a
# shellcheck disable=SC1090
source "$DOCKER_DIR/.env"
set +a

# --- Kiểm tra cấu hình MASTER_TS_IP ---
if [ -z "${MASTER_TS_IP:-}" ] || [ "$MASTER_TS_IP" = "100.x.x.x" ]; then
    echo ""
    echo "  ERROR: Chưa cấu hình MASTER_TS_IP trong .env"
    echo "  Lấy Tailscale IP của máy này:"
    echo "    tailscale ip -4"
    echo ""
    exit 1
fi

# --- Kiểm tra trạng thái hoạt động của Tailscale ---
if ! tailscale status > /dev/null 2>&1; then
    echo ""
    echo "  WARNING: Không tìm thấy lệnh 'tailscale' hoặc Tailscale chưa chạy."
    echo "  Worker machines sẽ không kết nối được."
    echo "  Khuyến nghị: cài và khởi động Tailscale trước."
    echo ""
fi

echo ""
echo "============================================================"
echo "  Khởi động Master Cluster"
echo "  Tailscale IP: $MASTER_TS_IP"
echo "============================================================"

# --- Tạo (Build) các Docker image nếu chưa có ---
echo ""
echo "  [1/3] Build Docker images (bỏ qua nếu đã có)..."
docker compose -f "$DOCKER_DIR/docker-compose.master.yml" \
    --env-file "$DOCKER_DIR/.env" \
    build --quiet

# --- Kiểm tra tùy chọn khởi chạy virtual worker ---
USE_VIRTUAL_WORKER=false
if [ "${1:-}" = "--with-virtual-worker" ] || [ "${ENABLE_VIRTUAL_WORKER:-false}" = "true" ]; then
    USE_VIRTUAL_WORKER=true
fi

# --- Khởi động các dịch vụ ---
echo ""
if [ "$USE_VIRTUAL_WORKER" = "true" ]; then
    echo "  [2/3] Khởi động master + virtual worker..."
    docker compose -f "$DOCKER_DIR/docker-compose.master.yml" \
        --env-file "$DOCKER_DIR/.env" \
        --profile virtual-worker \
        up -d
else
    echo "  [2/3] Khởi động master (không có virtual worker)..."
    docker compose -f "$DOCKER_DIR/docker-compose.master.yml" \
        --env-file "$DOCKER_DIR/.env" \
        up -d
fi

# --- Hiển thị thông báo kết quả ---
echo ""
echo "  [3/3] Cluster đang khởi động..."
echo ""
echo "============================================================"
echo "  Chờ 30-60 giây rồi kiểm tra các Web UI:"
echo ""
echo "  NameNode UI  : http://$MASTER_TS_IP:9870"
echo "  YARN UI      : http://$MASTER_TS_IP:8088"
echo "  Spark UI     : http://$MASTER_TS_IP:8080"
echo "  Jupyter      : http://$MASTER_TS_IP:8888"
echo ""
if [ "$USE_VIRTUAL_WORKER" = "true" ]; then
    echo "  Virtual Worker (Spark): http://localhost:8083"
    echo ""
fi
echo "  Kiểm tra trạng thái cluster:"
echo "    ./script/status.sh"
echo ""
echo "  Xem log:"
echo "    docker compose -f docker-compose.master.yml logs -f"
echo "============================================================"