#!/usr/bin/env bash
# ============================================================
#  tailscale-check.sh - Kiểm tra kết nối Tailscale trong nhóm
#
#  Cách dùng: ./scripts/tailscale-check.sh
#
#  Quy trình kiểm tra:
#    1. Kiểm tra trạng thái hoạt động của Tailscale
#    2. Lấy IP của máy hiện tại
#    3. Hiển thị danh sách các máy trong tailnet
#    4. Gửi ping kiểm tra từng máy trong .env (MASTER, WORKER1, WORKER2)
#    5. Kiểm tra trạng thái các cổng kết nối (ports) cần thiết
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"

# Tải các cấu hình từ file .env
if [ -f "$DOCKER_DIR/.env" ]; then
    set -a
    # shellcheck disable=SC1090
    source "$DOCKER_DIR/.env"
    set +a
fi

echo ""
echo "============================================================"
echo "  Kiểm tra kết nối mạng Tailscale"
echo "============================================================"

# --- Kiểm tra Tailscale đang chạy ---
echo ""
echo "--- Trạng thái hoạt động của Tailscale ---"
if ! command -v tailscale > /dev/null 2>&1; then
    echo "  [Lỗi] Không tìm thấy lệnh 'tailscale'."
    echo "  Tải và cài đặt tại: https://tailscale.com/download"
    exit 1
fi

MY_IP=$(tailscale ip -4 2>/dev/null || echo "")
if [ -z "$MY_IP" ]; then
    echo "  [Lỗi] Tailscale chưa kết nối. Vui lòng chạy lệnh: tailscale up"
    exit 1
fi

echo "  [OK] Tailscale đang hoạt động"
echo "  IP của máy này: $MY_IP"

# --- Hiển thị danh sách các máy trong dải mạng Tailscale (Tailnet) ---
echo ""
echo "--- Danh sách máy trong Tailnet ---"
tailscale status 2>/dev/null | head -20 || echo "  (Không lấy được thông tin)"

# --- Kiểm tra kết nối đến các máy chủ khai báo trong .env ---
echo ""
echo "--- Ping kiểm tra kết nối ---"

ping_check() {
    local name=$1
    local ip=$2
    if [ -z "$ip" ] || [ "$ip" = "100.x.x.x" ]; then
        printf "  %-20s  (Chưa cấu hình trong file .env)\n" "$name"
        return
    fi
    # Thử kiểm tra bằng ping với cả hệ điều hành Linux (-W) và macOS (-t)
    if ping -c 1 -W 2 "$ip" > /dev/null 2>&1 || \
       ping -c 1 -t 2 "$ip" > /dev/null 2>&1; then
        printf "  %-20s  \033[32m[Reachable]\033[0m  (%s)\n" "$name" "$ip"
    else
        printf "  %-20s  \033[31m[Unreachable]\033[0m (%s)\n" "$name" "$ip"
    fi
}

ping_check "Master"    "${MASTER_TS_IP:-}"
ping_check "Worker 1"  "${WORKER1_TS_IP:-}"
ping_check "Worker 2"  "${WORKER2_TS_IP:-}"

# --- Kiểm tra các cổng kết nối (ports) của máy Master ---
if [ -n "${MASTER_TS_IP:-}" ] && [ "$MASTER_TS_IP" != "100.x.x.x" ]; then
    echo ""
    echo "--- Kiểm tra cổng kết nối Master ($MASTER_TS_IP) ---"
    if command -v nc > /dev/null 2>&1; then
        check_port() {
            local port=$1
            local name=$2
            if nc -z -w 3 "$MASTER_TS_IP" "$port" 2>/dev/null; then
                printf "  Cổng %-6s  \033[32m[Mở]\033[0m   (%s)\n" "$port" "$name"
            else
                printf "  Cổng %-6s  \033[31m[Đóng]\033[0m (%s)\n" "$port" "$name"
            fi
        }
        check_port 9870 "HDFS Web UI"
        check_port 9000 "HDFS RPC"
        check_port 8088 "YARN UI"
        check_port 8080 "Spark UI"
        check_port 7077 "Spark RPC"
        check_port 8888 "Jupyter"
    else
        echo "  (Hệ thống không có lệnh 'nc' - bỏ qua kiểm tra cổng)"
        echo "  Trên Windows: Vui lòng chạy trong WSL2 hoặc cài đặt thêm tiện ích netcat"
    fi
fi

echo ""
echo "============================================================"
echo "  Nếu thiết bị không phản hồi (Unreachable):"
echo "    1. Đảm bảo cả hai máy đều đăng nhập chung một tài khoản Tailscale"
echo "    2. Kiểm tra danh sách thiết bị tại: https://login.tailscale.com/admin/machines"
echo "    3. Thử lệnh: tailscale ping <IP-máy-đối-phương>"
echo "============================================================"