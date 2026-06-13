#!/usr/bin/env bash
set -e
DOCKER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "============================================================"
echo "  Khởi động cluster (4 containers)"
echo "  namenode → datanode → kafka → jupyter"
echo "============================================================"
docker compose -f "$DOCKER_DIR/docker-compose.yml" up -d --build

echo ""
echo "  NameNode UI : http://localhost:9870"
echo "  YARN UI     : http://localhost:8088"
echo "  Jupyter     : http://localhost:8888"
echo "  Spark UI    : http://localhost:4040  (khi job đang chạy)"
echo "  Kafka       : localhost:9092"
echo ""
echo "  Xem log: docker compose logs -f"
