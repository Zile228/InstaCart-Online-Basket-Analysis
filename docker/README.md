# Hướng dẫn vận hành Hadoop-Spark-Kafka Cluster (nhom05)

> **Stack:** Hadoop 3.4.3 · Spark 4.1.1 · Kafka 4.1.2 · Jupyter  
> **Mạng:** Tailscale VPN  
> **OS:** Windows (Docker Desktop + WSL2) hoặc macOS

---

## Mục lục

1. [Cài đặt một lần (tất cả máy)](#1-cài-đặt-một-lần-tất-cả-máy)
2. [Cấu hình `.env`](#2-cấu-hình-env)
3. [Chạy máy MASTER](#3-chạy-máy-master)
4. [Chạy máy WORKER](#4-chạy-máy-worker)
5. [Kiểm tra cluster](#5-kiểm-tra-cluster)
6. [Khi thiếu bạn nhóm — Virtual Worker](#6-khi-thiếu-bạn-nhóm--virtual-worker)
7. [Dừng cluster](#7-dừng-cluster)
8. [Xử lý sự cố](#8-xử-lý-sự-cố)

---

## 1. Cài đặt một lần (tất cả máy)

Thực hiện **một lần duy nhất** trên từng máy trước khi dùng lần đầu.

### Bước 1.1 — Cài Tailscale

| OS | Link tải |
|---|---|
| Windows | https://tailscale.com/download/windows |
| macOS | https://tailscale.com/download/macos |

**Sau khi cài xong:**

1. Đăng nhập **cùng một tài khoản Tailscale** với cả nhóm (tạo một tài khoản chung, hoặc một người tạo rồi invite qua https://login.tailscale.com/admin/machines)
2. Kiểm tra máy đã join tailnet: https://login.tailscale.com/admin/machines
3. Lấy Tailscale IP của máy mình:

```bash
# macOS / WSL2 / Git Bash
tailscale ip -4

# Windows PowerShell
tailscale ip -4
```

> IP Tailscale luôn có dạng `100.x.x.x`. **Ghi lại và chia sẻ với cả nhóm.**

### Bước 1.2 — Cài Docker Desktop

- Windows: https://www.docker.com/products/docker-desktop/
- macOS: https://www.docker.com/products/docker-desktop/

**Cấu hình Docker Desktop (quan trọng):**

Vào **Settings → Resources** và cấp:
- **Máy master:** RAM ≥ 6GB, CPU ≥ 4 cores
- **Máy worker:** RAM ≥ 5GB, CPU ≥ 3 cores

### Bước 1.3 — Clone repo

```bash
git clone https://github.com/<nhom>/bigdata-cluster.git
cd bigdata-cluster/docker
```

---

## 2. Cấu hình `.env`

Tạo file `.env` từ template (chỉ làm một lần):

```bash
cp env.example .env
```

Rồi mở file `.env` và điền giá trị thực:

### Máy MASTER — điền vào `.env`

```env
# IP Tailscale của MÁY NÀY (máy master)
MASTER_TS_IP=100.xx.xx.xx      # ← thay bằng IP lấy từ: tailscale ip -4

# IP Tailscale của các máy WORKER (lấy từ các bạn worker)
WORKER1_TS_IP=100.xx.xx.xx     # ← IP của bạn worker-1
WORKER2_TS_IP=100.xx.xx.xx     # ← IP của bạn worker-2 (nếu có)

# Không cần thiết trên master
WORKER_ID=0

# Virtual worker: tắt nếu có đủ người, bật nếu thiếu
ENABLE_VIRTUAL_WORKER=false

# Replication: 1 nếu 1 worker, 2 nếu 2 worker
DFS_REPLICATION=1
```

### Máy WORKER — điền vào `.env`

```env
# IP Tailscale của MÁY MASTER (lấy từ bạn master)
MASTER_TS_IP=100.xx.xx.xx      # ← IP của máy master, KHÔNG phải IP máy mình

# ID của worker này
WORKER_ID=1                     # ← 1 cho worker đầu tiên, 2 cho cái thứ hai

# Tài nguyên — điều chỉnh theo RAM máy
WORKER_MEM=4G                   # Máy 8GB dùng 4G, máy 16GB dùng 6G
WORKER_CORES=2
YARN_MEM_MB=3072                # Máy 8GB dùng 3072, máy 16GB dùng 6144
```

> ⚠️ File `.env` đã được thêm vào `.gitignore`. **Không commit lên git.**

---

## 3. Chạy máy MASTER

> Máy master phải **khởi động trước** — worker cần kết nối đến master khi start.

### Trên macOS

```bash
cd bigdata-cluster/docker
chmod +x script/*.sh
./script/start-master.sh
```

### Trên Windows (dùng WSL2)

Mở **WSL2 terminal** (Ubuntu), không dùng PowerShell hay CMD:

```bash
cd /mnt/c/Users/<username>/bigdata-cluster/docker
chmod +x script/*.sh
./script/start-master.sh
```

### Lần build đầu tiên

Build Docker images mất khoảng **5–10 phút**. Các lần sau nhanh hơn vì đã cache.

### Kiểm tra master đã sẵn sàng

Chờ khoảng **60 giây** rồi mở các link sau trong browser:

| Service | URL | Trạng thái bình thường |
|---|---|---|
| NameNode UI | `http://MASTER_TS_IP:9870` | "Active" |
| YARN UI | `http://MASTER_TS_IP:8088` | Hiển thị cluster |
| Spark UI | `http://MASTER_TS_IP:8080` | "ALIVE", Workers: 0 |
| Jupyter | `http://MASTER_TS_IP:8888` | Notebook interface |

> Thay `MASTER_TS_IP` bằng IP Tailscale thực của máy master.  
> Ví dụ: `http://100.88.12.34:9870`

---

## 4. Chạy máy WORKER

> Chỉ chạy **sau khi master đã sẵn sàng** (NameNode UI đã mở được).

### Trên macOS

```bash
cd bigdata-cluster/docker
chmod +x script/*.sh
./script/start-worker.sh
```

### Trên Windows (dùng WSL2)

```bash
cd /mnt/c/Users/<username>/bigdata-cluster/docker
chmod +x script/*.sh
./script/start-worker.sh
```

### Script tự động làm gì

1. Kiểm tra `.env` và `MASTER_TS_IP`
2. Kiểm tra kết nối Tailscale đến master
3. Đợi NameNode (`MASTER_TS_IP:9870`) sẵn sàng (tối đa 3 phút)
4. Build images nếu chưa có
5. Start DataNode + NodeManager + Spark Worker

### Kiểm tra worker đã đăng ký

Mở các link sau (thay bằng IP thực):

| Nơi kiểm tra | URL |
|---|---|
| HDFS DataNodes | `http://MASTER_TS_IP:9870` → tab "Datanodes" |
| YARN Nodes | `http://MASTER_TS_IP:8088/cluster/nodes` |
| Spark Workers | `http://MASTER_TS_IP:8080` → "Workers: 1" |
| DataNode UI (trực tiếp) | `http://WORKER_TS_IP:9864` |

---

## 5. Kiểm tra cluster

```bash
./script/status.sh
```

Kết quả mong đợi khi cluster hoạt động đầy đủ:

```
===== Cluster Status =====
Master: 100.xx.xx.xx

--- Master Services ---
  NameNode Web UI                ✓ UP    http://100.xx.xx.xx:9870
  NameNode RPC                   ✓ UP    hdfs://100.xx.xx.xx:9000
  YARN ResourceMgr               ✓ UP    http://100.xx.xx.xx:8088
  Spark Master                   ✓ UP    http://100.xx.xx.xx:8080
  Spark Master RPC               ✓ UP    spark://100.xx.xx.xx:7077
  Kafka                          ✓ UP    100.xx.xx.xx:9092
  Jupyter                        ✓ UP    http://100.xx.xx.xx:8888

--- Kiểm tra DataNodes ---
"NumLiveDataNodes":1

--- Kiểm tra Spark Workers ---
  Workers alive: 1  |  Cores: 2  |  Memory: 4096MB
```

---

## 6. Khi thiếu bạn nhóm — Virtual Worker

Nếu chỉ có một máy (master) và cần cluster hoạt động:

```bash
# Cách 1: Bật ngay khi start
./script/start-master.sh --with-virtual-worker

# Cách 2: Bật sau khi master đã chạy
docker compose -f docker-compose.master.yml --env-file .env \
  --profile virtual-worker up -d virtual-datanode virtual-spark-worker
```

Virtual worker có giới hạn: **512MB RAM, 1 core** — chỉ để cluster không trống, không dùng cho job nặng.

**Tắt virtual worker khi bạn nhóm đã join:**

```bash
docker compose -f docker-compose.master.yml --env-file .env \
  --profile virtual-worker stop virtual-datanode virtual-spark-worker
```

---

## 7. Dừng cluster

```bash
# [Máy worker] — dừng worker trước
docker compose -f docker-compose.worker.yml down

# [Máy master] — dừng master sau
docker compose -f docker-compose.master.yml down

# Hoặc dùng script stop-all (chạy trên từng máy)
./script/stop-all.sh
```

---

## 8. Xử lý sự cố

### ❌ `start-worker.sh` báo warning không kết nối được

**Nguyên nhân hay gặp:**

1. **Tailscale chưa bật** — kiểm tra:
   ```bash
   tailscale status
   ```
   Phải thấy máy master trong danh sách với trạng thái `active`.

2. **Sai `MASTER_TS_IP` trong `.env`** — xác nhận lại IP với bạn master:
   ```bash
   # Bạn master chạy lệnh này và chia sẻ kết quả
   tailscale ip -4
   ```

3. **Master chưa chạy** — đảm bảo `start-master.sh` đã hoàn thành và NameNode UI mở được tại `http://MASTER_TS_IP:9870`.

4. **Đang chạy trên PowerShell/CMD** — phải dùng WSL2 (Windows) hoặc Terminal (macOS). Lệnh `nc` không có trong PowerShell.

---

### ❌ DataNode không đăng ký / DataNode UI không mở được

**Kiểm tra log:**
```bash
docker logs datanode-1 --tail 50
```

**Hay gặp:** DataNode chờ NameNode nhưng `MASTER_TS_IP` trong `.env` sai.

**Sửa:**
```bash
# Xem IP thực
cat .env | grep MASTER_TS_IP

# Nếu sai, sửa rồi restart
docker compose -f docker-compose.worker.yml down
# sửa .env
docker compose -f docker-compose.worker.yml up -d
```

**DataNode UI** (`http://WORKER_TS_IP:9864`) không mở được nếu dùng IP máy thường thay vì IP Tailscale. Phải dùng IP Tailscale của máy worker.

---

### ❌ Spark Worker không kết nối Master

**Kiểm tra log:**
```bash
docker logs spark-worker-1 --tail 30
```

**Nguyên nhân:** Port 7077 trên master bị firewall chặn.

**Sửa trên máy master (macOS):**
```bash
# Không cần làm gì — Tailscale mặc định cho phép tất cả port
# Nếu vẫn lỗi, kiểm tra Tailscale ACL tại login.tailscale.com/admin/acls
```

**Sửa trên Windows:** Tắt Windows Defender Firewall tạm thời để test, hoặc thêm rule cho port 7077.

---

### ❌ OOM — container bị kill

Triệu chứng: container tự restart, log có `Killed`.

**Sửa:** Vào Docker Desktop → Settings → Resources → tăng RAM lên 6–8GB.

Hoặc giảm `WORKER_MEM` trong `.env`:
```env
WORKER_MEM=2G
YARN_MEM_MB=2048
```

---

### ❌ Kafka consumer từ worker không kết nối

**Kiểm tra:** `KAFKA_ADVERTISED_LISTENERS` trong `docker-compose.master.yml` phải chứa `MASTER_TS_IP` thực (không phải `100.x.x.x`).

```bash
# Xem giá trị đang dùng
docker exec kafka env | grep KAFKA_ADVERTISED
```

Phải thấy: `PLAINTEXT_HOST://100.xx.xx.xx:9092` với IP thực.

---

### ❌ `nc: command not found` trên Windows

**Nguyên nhân:** Đang chạy trong PowerShell hoặc CMD thay vì WSL2.

**Sửa:** Mở WSL2:
```
# Nhấn Win + R, gõ: wsl
# Hoặc mở "Ubuntu" từ Start menu
```

---

### 🔍 Xem log container

```bash
# Master
docker compose -f docker-compose.master.yml logs -f namenode
docker compose -f docker-compose.master.yml logs -f spark-master
docker compose -f docker-compose.master.yml logs -f kafka

# Worker
docker compose -f docker-compose.worker.yml logs -f datanode
docker compose -f docker-compose.worker.yml logs -f spark-worker

# Tất cả cùng lúc
docker compose -f docker-compose.master.yml logs -f
docker compose -f docker-compose.worker.yml logs -f
```

---

## Tóm tắt nhanh

```
Lần đầu (mọi máy):
  1. Cài Tailscale → đăng nhập cùng tài khoản → lấy IP (tailscale ip -4)
  2. Cài Docker Desktop → cấp đủ RAM
  3. git clone repo → cp env.example .env → điền .env

Mỗi lần chạy:
  [Master]  ./script/start-master.sh           ← chạy TRƯỚC
  [Worker]  ./script/start-worker.sh           ← chạy SAU khi master sẵn sàng

Kiểm tra:
  ./script/status.sh
  http://MASTER_TS_IP:9870   ← HDFS
  http://MASTER_TS_IP:8080   ← Spark
  http://MASTER_TS_IP:8888   ← Jupyter

Dừng:
  [Worker]  docker compose -f docker-compose.worker.yml down
  [Master]  docker compose -f docker-compose.master.yml down
```
