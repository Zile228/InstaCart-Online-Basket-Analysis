# Docker Cluster — Hướng dẫn sử dụng

## Stack

| Service | Image | Port |
|---------|-------|------|
| NameNode | `instacart-hadoop:3.4.3` (custom) | 9870, 9000 |
| DataNode ×2 | `instacart-hadoop:3.4.3` (custom) | — |
| Spark Master | `instacart-spark:4.1.1` (custom) | 8080, 7077 |
| Spark Worker ×2 | `instacart-spark:4.1.1` (custom) | 8081, 8082 |
| Kafka | `apache/kafka:4.1.2` (KRaft, no Zookeeper) | 9092 |
| Jupyter | custom (Python 3.11 + PySpark 4.1.1) | 8888 |

---

## Quick Start

### Bước 1 — Build images (chỉ cần làm 1 lần, ~10-15 phút)

```bash
cd docker/

# Build Hadoop image 
docker build -f hadoop/Dockerfile -t instacart-hadoop:3.4.3 hadoop/

# Build Spark image 
docker build -f spark/Dockerfile -t instacart-spark:4.1.1 spark/

# Build Jupyter image
docker build -f jupyter/Dockerfile -t instacart-jupyter:latest jupyter/
```

### Bước 2 — Khởi động cluster

```bash
cd docker/
docker-compose up -d

# Đợi ~60-90 giây cho cluster ổn định
docker-compose ps
```

Phải thấy tất cả containers ở trạng thái `running` (không có `Exit`).

### Bước 3 — Kiểm tra UI

| UI | URL | Phải thấy |
|----|-----|-----------|
| HDFS NameNode | http://localhost:9870 | Tab "Datanodes" → 2 live datanodes |
| Spark Master | http://localhost:8080 | 2 workers registered |
| YARN | http://localhost:8088 | ResourceManager running |
| Jupyter | http://localhost:8888 | Notebook server (no token) |

### Bước 4 — Upload data

```bash
# Copy CSV files vào namenode container (từ thư mục data/)
docker cp data/orders.csv namenode:/tmp/
docker cp data/order_products__prior.csv namenode:/tmp/
docker cp data/order_products__train.csv namenode:/tmp/
docker cp data/products.csv namenode:/tmp/
docker cp data/aisles.csv namenode:/tmp/
docker cp data/departments.csv namenode:/tmp/

# Chạy script upload
docker exec namenode bash /home/nhom05/work/01_preprocessing/01_upload_to_hdfs.sh

# Hoặc dùng Windows .cmd script
src\01_preprocessing\01_upload_to_hdfs_windows.cmd
```

### Bước 5 — Chạy Feature Engineering

Mở http://localhost:8888 → `work/01_preprocessing/02_feature_engineering.py`

Chạy từng cell (# %% markers) hoặc toàn bộ script.

---

## RAM Requirements

| Cấu hình | Tổng RAM cần |
|----------|-------------|
| Full (2 worker × 2G) | ~10-12 GB |
| Light (2 worker × 1G) | ~6-8 GB |
| Minimal (1 worker × 1G) | ~5-6 GB |

**Nếu máy ≤ 16GB RAM**, giảm trong `docker-compose.yml`:
```yaml
SPARK_WORKER_MEMORY: 1G
```
Và trong `spark-defaults.conf`:
```
spark.executor.memory   1g
```

---

## Lỗi thường gặp

| Lỗi | Nguyên nhân | Cách sửa |
|-----|------------|---------|
| DataNode không kết nối được | `ip-hostname-check` | Đã fix trong `hdfs-site.xml` (PATCH 8) |
| `ClassNotFoundException kafka` | Sai Scala version | Dùng `_2.13` không phải `_2.12` (PATCH 2) |
| Spark Worker exit(1) | Hết RAM | Giảm `SPARK_WORKER_MEMORY=1G` |
| Kafka fails to start | CLUSTER_ID conflict | `docker-compose down -v` rồi up lại |
| Jupyter không load | Port 8888 bị chiếm | Đổi port sang `8889:8888` |

## Dừng cluster

```bash
docker-compose down

# Xóa volumes (reset hoàn toàn HDFS data):
docker-compose down -v
```

---

## Spark submit example

```bash
# Từ bên trong spark-master container:
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  --executor-memory 1g \
  /opt/spark/work-dir/your_script.py
```
