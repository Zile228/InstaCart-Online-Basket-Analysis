#!/usr/bin/env bash
set -e
DOCKER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "------------------------------------------------"
echo "  Bat dau khoi dong cluster (4 containers)"
echo "  namenode -> datanode -> jupyter"
echo "------------------------------------------------"
docker compose -f "$DOCKER_DIR/docker-compose.yml" up -d --build

echo ""
echo "  NameNode UI : http://localhost:9870"
echo "  YARN UI     : http://localhost:8088"
echo "  Jupyter     : http://localhost:8888"
echo "  Spark UI    : http://localhost:4040  (khi job dang chay)"
echo ""
echo "  Xem log bang lenh: docker compose logs -f"