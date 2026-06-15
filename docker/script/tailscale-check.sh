#!/usr/bin/env bash
# ============================================================
#  tailscale-check.sh — Kiểm tra kết nối Tailscale trong nhóm
#
#  Usage: ./scripts/tailscale-check.sh
#
#  Script kiểm tra:
#    1. Tailscale đang chạy không
#    2. IP của máy này
#    3. Danh sách máy trong tailnet
#    4. Ping từng máy trong .env (MASTER, WORKER1, WORKER2)
#    5. Kiểm tra ports cần thiết
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"

# Load .env
if [ -f "$DOCKER_DIR/.env" ]; then
    set -a
    # shellcheck disable=SC1090
    source "$DOCKER_DIR/.env"
    set +a
fi

echo ""
echo "============================================================"
echo "  Tailscale Connectivity Check"
echo "============================================================"

# ── Kiểm tra Tailscale đang chạy ─────────────────────────────
echo ""
echo "--- Tailscale Status ---"
if ! command -v tailscale > /dev/null 2>&1; then
    echo "  ✗ Lệnh 'tailscale' không tìm thấy."
    echo "  Cài đặt: https://tailscale.com/download"
    exit 1
fi

MY_IP=$(tailscale ip -4 2>/dev/null || echo "")
if [ -z "$MY_IP" ]; then
    echo "  ✗ Tailscale chưa kết nối. Chạy: tailscale up"
    exit 1
fi

echo "  ✓ Tailscale đang chạy"
echo "  IP của máy này: $MY_IP"

# ── Hiển thị danh sách tailnet ───────────────────────────────
echo ""
echo "--- Máy trong Tailnet ---"
tailscale status 2>/dev/null | head -20 || echo "  (không lấy được thông tin)"

# ── Kiểm tra kết nối đến các máy trong .env ──────────────────
echo ""
echo "--- Ping kiểm tra kết nối ---"

ping_check() {
    local name=$1
    local ip=$2
    if [ -z "$ip" ] || [ "$ip" = "100.x.x.x" ]; then
        printf "  %-20s  (chưa cấu hình trong .env)\n" "$name"
        return
    fi
    # Thử cả hai flag -W (Linux) và -t (macOS)
    if ping -c 1 -W 2 "$ip" > /dev/null 2>&1 || \
       ping -c 1 -t 2 "$ip" > /dev/null 2>&1; then
        printf "  %-20s  \033[32m✓ Reachable\033[0m  (%s)\n" "$name" "$ip"
    else
        printf "  %-20s  \033[31m✗ Unreachable\033[0m (%s)\n" "$name" "$ip"
    fi
}

ping_check "Master"    "${MASTER_TS_IP:-}"
ping_check "Worker 1"  "${WORKER1_TS_IP:-}"
ping_check "Worker 2"  "${WORKER2_TS_IP:-}"

# ── Kiểm tra ports master ─────────────────────────────────────
if [ -n "${MASTER_TS_IP:-}" ] && [ "$MASTER_TS_IP" != "100.x.x.x" ]; then
    echo ""
    echo "--- Kiểm tra ports Master ($MASTER_TS_IP) ---"
    if command -v nc > /dev/null 2>&1; then
        check_port() {
            local port=$1
            local name=$2
            if nc -z -w 3 "$MASTER_TS_IP" "$port" 2>/dev/null; then
                printf "  Port %-6s  \033[32m✓ Open\033[0m   (%s)\n" "$port" "$name"
            else
                printf "  Port %-6s  \033[31m✗ Closed\033[0m (%s)\n" "$port" "$name"
            fi
        }
        check_port 9870 "HDFS Web UI"
        check_port 9000 "HDFS RPC"
        check_port 8088 "YARN UI"
        check_port 8080 "Spark UI"
        check_port 7077 "Spark RPC"
        check_port 8888 "Jupyter"
    else
        echo "  (Lệnh 'nc' không có — bỏ qua kiểm tra port)"
        echo "  Trên Windows: chạy trong WSL2 hoặc cài netcat"
    fi
fi

echo ""
echo "============================================================"
echo "  Nếu máy không reachable:"
echo "    1. Đảm bảo cả hai đều đăng nhập cùng tailnet"
echo "    2. Kiểm tra: https://login.tailscale.com/admin/machines"
echo "    3. Thử: tailscale ping <IP-máy-kia>"
echo "============================================================"