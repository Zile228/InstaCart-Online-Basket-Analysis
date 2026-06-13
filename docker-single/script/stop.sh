#!/usr/bin/env bash
# stop.sh - Dung va xoa cac container (nhung van giu lai volume de khong mat du lieu)
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"

echo "Dang dung cluster..."
docker compose -f "$DOCKER_DIR/docker-compose.yml" down
echo "Da dung xong. Cac volume du lieu van duoc giu lai (neu muon xoa sach hoan toan data, ban hay dung lenh: docker compose down -v)."