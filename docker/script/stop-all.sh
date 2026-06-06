#!/usr/bin/env bash
# ============================================================
#  stop-all.sh — Dừng tất cả containers
#
#  Thứ tự dừng:
#    1. Worker (datanode, spark-worker) — dừng trước
#    2. Master (namenode, spark-master, kafka, jupyter) — dừng sau
#
#  Usage:
#    ./scripts/stop-all.sh          # Dừng và xoá containers (giữ volumes)
#    ./scripts/stop-all.sh --clean  # Dừng, xoá containers VÀ volumes (mất data HDFS!)
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"

CLEAN_MODE=false
if [ "${1:-}" = "--clean" ]; then
    CLEAN_MODE=true
    echo ""
    echo "  WARNING: --clean sẽ xoá toàn bộ HDFS data (Docker volumes)!"
    echo "  Nhấn Ctrl+C trong 5 giây để huỷ..."
    sleep 5
fi

echo ""
echo "============================================================"
echo "  Dừng cluster..."
echo "============================================================"

# ── Dừng worker (nếu đang chạy trên máy này) ─────────────────
if docker compose -f "$DOCKER_DIR/docker-compose.worker.yml" ps -q 2>/dev/null | grep -q .; then
    echo ""
    echo "  Dừng Worker services..."
    if [ "$CLEAN_MODE" = true ]; then
        docker compose -f "$DOCKER_DIR/docker-compose.worker.yml" \
            --env-file "$DOCKER_DIR/.env" down -v
    else
        docker compose -f "$DOCKER_DIR/docker-compose.worker.yml" \
            --env-file "$DOCKER_DIR/.env" down
    fi
    echo "  Worker stopped ✓"
fi

# ── Dừng master ───────────────────────────────────────────────
if docker compose -f "$DOCKER_DIR/docker-compose.master.yml" ps -q 2>/dev/null | grep -q .; then
    echo ""
    echo "  Dừng Master services..."
    if [ "$CLEAN_MODE" = true ]; then
        docker compose -f "$DOCKER_DIR/docker-compose.master.yml" \
            --env-file "$DOCKER_DIR/.env" \
            --profile virtual-worker down -v
    else
        docker compose -f "$DOCKER_DIR/docker-compose.master.yml" \
            --env-file "$DOCKER_DIR/.env" \
            --profile virtual-worker down
    fi
    echo "  Master stopped ✓"
fi

echo ""
if [ "$CLEAN_MODE" = true ]; then
    echo "  Cluster đã dừng và volumes đã xoá."
    echo "  Lần start tiếp theo sẽ format lại HDFS từ đầu."
else
    echo "  Cluster đã dừng. Data HDFS vẫn còn trong Docker volumes."
fi
echo "============================================================"