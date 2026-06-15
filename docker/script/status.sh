#!/usr/bin/env bash
# ============================================================
#  status.sh — Kiểm tra trạng thái toàn bộ cluster
#
#  Usage: ./scripts/status.sh
#
#  Hoạt động trên: macOS, WSL2, Linux
#  Yêu cầu: nc (netcat), curl, python3
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"

# Load .env (không báo lỗi nếu không có)
if [ -f "$DOCKER_DIR/.env" ]; then
    set -a
    # shellcheck disable=SC1090
    source "$DOCKER_DIR/.env"
    set +a
fi

MASTER="${MASTER_TS_IP:-localhost}"

# ── Kiểm tra nc có sẵn không ─────────────────────────────────
# Git Bash (Windows) thường không có nc → fallback sang bash /dev/tcp
CHECK_NC=true
if ! command -v nc > /dev/null 2>&1; then
    echo "  nc không có — dùng bash /dev/tcp fallback"
    CHECK_NC=false
fi

# ── Helper: TCP check (nc hoặc bash fallback) ─────────────────
tcp_check() {
    local host=$1 port=$2
    if [ "$CHECK_NC" = "true" ]; then
        nc -z "$host" "$port" 2>/dev/null
    else
        (bash -c "echo >/dev/tcp/$host/$port" 2>/dev/null)
    fi
}

# ── Helper: kiểm tra 1 service ───────────────────────────────
check_service() {
    local name=$1
    local host=$2
    local port=$3
    local url=$4

    if tcp_check "$host" "$port"; then
        printf "  %-28s \033[32m✓ UP\033[0m    %s\n" "$name" "$url"
    else
        printf "  %-28s \033[31m✗ DOWN\033[0m  port $port không respond\n" "$name"
    fi
}

echo ""
echo "============================================================"
echo "  Cluster Status — Master: $MASTER"
echo "============================================================"

echo ""
echo "--- Master Services ---"
check_service "NameNode Web UI"       "$MASTER" 9870 "http://$MASTER:9870"
check_service "NameNode RPC (HDFS)"   "$MASTER" 9000 "hdfs://$MASTER:9000"
check_service "YARN ResourceManager"  "$MASTER" 8088 "http://$MASTER:8088"
check_service "YARN RM Scheduler"     "$MASTER" 8032 "$MASTER:8032"
check_service "Spark Master UI"       "$MASTER" 8080 "http://$MASTER:8080"
check_service "Spark Master RPC"      "$MASTER" 7077 "spark://$MASTER:7077"
check_service "Jupyter"               "$MASTER" 8888 "http://$MASTER:8888"

# ── DataNodes đang đăng ký ────────────────────────────────────
echo ""
echo "--- HDFS DataNodes ---"
DN_JSON=$(curl -s --max-time 5 \
    "http://$MASTER:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState" 2>/dev/null)

if [ -n "$DN_JSON" ]; then
    LIVE=$(echo "$DN_JSON" | python3 -c \
        "import sys,json; d=json.load(sys.stdin)['beans'][0]; print(d.get('NumLiveDataNodes','?'))" 2>/dev/null)
    DEAD=$(echo "$DN_JSON" | python3 -c \
        "import sys,json; d=json.load(sys.stdin)['beans'][0]; print(d.get('NumDeadDataNodes','?'))" 2>/dev/null)
    STALE=$(echo "$DN_JSON" | python3 -c \
        "import sys,json; d=json.load(sys.stdin)['beans'][0]; print(d.get('NumStaleDataNodes','?'))" 2>/dev/null)
    printf "  Live: \033[32m%s\033[0m  |  Dead: \033[31m%s\033[0m  |  Stale: %s\n" \
        "${LIVE:-?}" "${DEAD:-?}" "${STALE:-?}"
    echo "  Detail: http://$MASTER:9870/dfshealth.html#tab-datanode"
else
    echo "  Không lấy được thông tin (NameNode chưa sẵn sàng hoặc down)"
fi

# ── Spark Workers đang đăng ký ────────────────────────────────
echo ""
echo "--- Spark Workers ---"
SPARK_JSON=$(curl -s --max-time 5 "http://$MASTER:8080/json/" 2>/dev/null)

if [ -n "$SPARK_JSON" ]; then
    python3 -c "
import sys, json
try:
    d = json.loads('''$SPARK_JSON''')
    alive  = d.get('aliveworkers', '?')
    cores  = d.get('cores', '?')
    mem    = d.get('memory', '?')
    status = d.get('status', '?')
    print(f'  Status: {status}  |  Workers alive: {alive}  |  Cores: {cores}  |  Memory: {mem} MB')
    for w in d.get('workers', []):
        wstate = w.get('state','?')
        whost  = w.get('host','?')
        wcores = w.get('cores','?')
        wmem   = w.get('memory','?')
        print(f'    - {whost}  state={wstate}  cores={wcores}  mem={wmem}MB')
except Exception as e:
    print(f'  Parse error: {e}')
" 2>/dev/null || echo "  Không lấy được thông tin (Spark Master chưa sẵn sàng)"
else
    echo "  Không lấy được thông tin (Spark Master down hoặc không reachable)"
fi

# ── Local Docker containers ───────────────────────────────────
echo ""
echo "--- Local Docker Containers ---"
docker ps --format "  {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null \
    | grep -E "(namenode|spark|jupyter|datanode|virtual)" \
    | sed 's/\t/  /g' \
    || echo "  (Không có container nào đang chạy)"

echo ""
echo "============================================================"