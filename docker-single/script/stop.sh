#!/usr/bin/env bash
# stop.sh — Dừng và xoá containers (giữ volumes)
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"

echo "Dừng cluster..."
docker compose -f "$DOCKER_DIR/docker-compose.yml" down
echo "Xong. Volumes vẫn còn (dùng 'docker compose down -v' để xoá luôn data)."
