#!/usr/bin/env bash
# status.sh - Kiem tra trang thai cua toan bo cluster
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"

echo ""
echo "--- Trang thai cac Container ---"
docker compose -f "$DOCKER_DIR/docker-compose.yml" ps

echo ""
echo "--- Trang thai he thong HDFS ---"
docker exec namenode hdfs dfsadmin -report 2>/dev/null | head -30 || echo "NameNode chua san sang"

echo ""
echo "--- Danh sach Node cua YARN ---"
docker exec namenode yarn node -list 2>/dev/null || echo "YARN chua san sang"