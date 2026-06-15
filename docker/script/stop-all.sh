#!/usr/bin/env bash
# ============================================================
#  stop-all.sh - Dừng tất cả containers
#
#  Thứ tự dừng:
#    1. Worker (datanode, spark-worker) - dừng trước
#    2. Master (namenode, spark-master, jupyter) - dừng sau
#
#  Cách dùng:
#    ./scripts/stop-all.sh          # Dừng và xóa containers (giữ lại volumes)
#    ./scripts/stop-all.sh --clean  # Dừng, xóa containers và volumes (mất dữ liệu HDFS!)
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"

CLEAN_MODE=false
if [ "${1:-}" = "--clean" ]; then
    CLEAN_MODE=true
    echo ""
    echo "  WARNING: --clean sẽ xóa toàn bộ dữ liệu HDFS (Docker volumes)!"
    echo "  Nhấn Ctrl+C trong vòng 5 giây để hủy..."
    sleep 5
fi

echo ""
echo "============================================================"
echo "  Dừng cluster..."
echo "============================================================"

# --- Dừng các dịch vụ worker (nếu đang chạy trên máy này) ---
if docker compose -f "$DOCKER_DIR/docker-compose.worker.yml" ps -q 2>/dev/null | grep -q .; then
    echo ""
    echo "  Dừng các dịch vụ Worker..."
    if [ "$CLEAN_MODE" = true ]; then
        docker compose -f "$DOCKER_D