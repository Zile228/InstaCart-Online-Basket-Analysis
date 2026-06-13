#!/usr/bin/env bash
# status.sh — Kiểm tra trạng thái cluster
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"

echo ""
echo "=== Container Status ==="
docker compose -f "$DOCKER_DIR/docker-compose.yml" ps

echo ""
echo "=== HDFS Status ==="
docker exec namenode hdfs dfsadmin -report 2>/dev/null | head -30 || echo "NameNode chưa sẵn sàng"

echo ""
echo "=== YARN Nodes ==="
docker exec namenode yarn node -list 2>/dev/null || echo "YARN chưa sẵn sàng"
