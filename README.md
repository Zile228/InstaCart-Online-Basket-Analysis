# Instacart Online Grocery Basket Analysis

> **Môn học:**  Dữ liệu lớn và ứng dụng

> **Nhóm:** 05

Thành viên:
1. Thái Hoài An - 31231025020
2. Nguyễn Thị Thùy Dương - 31231022904
3. Nguyễn Duy Tân - 31231023384
4. Lê Vy - 31231022128

> **Dataset:** [Instacart Online Grocery Basket Analysis - Yasserh (Kaggle)](https://www.kaggle.com/datasets/yasserh/instacart-online-grocery-basket-analysis-dataset)

---

## Mục lục

1. [Giới thiệu đề tài](#1-giới-thiệu-đề-tài)
2. [Bộ dữ liệu](#2-bộ-dữ-liệu)
3. [Kiến trúc hệ thống](#3-kiến-trúc-hệ-thống)
4. [Công nghệ sử dụng](#4-công-nghệ-sử-dụng)
5. [Cấu trúc thư mục](#5-cấu-trúc-thư-mục)
6. [Phân công công việc](#6-phân-công-công-việc)
7. [Hướng dẫn chạy nhanh](#7-hướng-dẫn-chạy-nhanh)

---

## 1. Giới thiệu

Đồ án thực hiện phân tích hành vi mua sắm nhu yếu phẩm của hơn 200.000 khách hàng trên nền tảng **Instacart**, bao gồm hơn **3.4 triệu đơn hàng** và **32.4 triệu chi tiết sản phẩm**. Mục tiêu triển khai của nhóm bao gồm:

- Xây dựng hạ tầng phân tán **Hadoop + Spark** trên cụm **multi-node** giả lập bằng Docker Compose.
- Phân tích hành vi mua sắm bằng **Spark SQL**.
- Học máy phân tán với **Spark MLlib**: phân cụm khách hàng (K-Means) và dự báo sản phẩm mua lại (Random Forest).
- Mô phỏng luồng đơn hàng thời gian thực bằng **Kafka + Spark Structured Streaming**.
- Demo kết quả trực tuyến qua **Vercel** (Next.js frontend) + **Railway** (FastAPI backend) + **Supabase** (PostgreSQL cloud).

---

## 2. Bộ dữ liệu

### Nguồn gốc

| Thông tin | Chi tiết |
|---|---|
| Tên dataset | Instacart Online Grocery Basket Analysis |
| Tác giả | Yasserh |
| Nền tảng | Kaggle |
| Link | https://www.kaggle.com/datasets/yasserh/instacart-online-grocery-basket-analysis-dataset |

### Các bảng dữ liệu

| File | Số dòng | Vai trò | Các cột chính |
|---|---|---|---|
| `orders.csv` | ~3,400,000 | Bảng sự kiện đơn hàng | `order_id`, `user_id`, `order_dow`, `order_hour_of_day`, `days_since_prior_order` |
| `order_products__prior.csv` | ~32,400,000 | Fact table chi tiết sản phẩm | `order_id`, `product_id`, `add_to_cart_order`, `reordered` |
| `products.csv` | 49,688 | Dimension - sản phẩm | `product_id`, `product_name`, `aisle_id`, `department_id` |
| `aisles.csv` | 134 | Dimension - quầy hàng | `aisle_id`, `aisle` |
| `departments.csv` | 21 | Dimension - ngành hàng | `department_id`, `department` |


---

## 3. Kiến trúc hệ thống

```
┌─────────────────────────────────────────────────────────────┐
│              Docker Compose — Multi-node Cluster (Local)     │
│                                                              │
│  ┌──────────────┐    ┌───────────────┐    ┌──────────────┐  │
│  │ Hadoop        │    │ Spark Cluster  │    │ Kafka + DB   │  │
│  │ NameNode:9870 │    │ Master:8080    │    │ Broker:9092  │  │
│  │ DataNode x2   │◄──►│ Worker x2      │    │ Postgres     │  │
│  │ HDFS Storage  │    │ Jupyter:8888   │    │ FastAPI:8000 │  │
│  └──────────────┘    └───────────────┘    └──────────────┘  │
└────────────────────────────┬────────────────────────────────┘
                             │ migrate results
                ┌────────────▼────────────────────────────────┐
                │                   Cloud (Free Tier)          │
                │  Supabase (PostgreSQL) ◄─► Railway (FastAPI) │
                │                              │               │
                │                         Vercel (Next.js)     │
                └─────────────────────────────────────────────┘
```


### Luồng dữ liệu chính

1. Dataset CSV -> upload lên **HDFS** (partitioned).
2. **Spark SQL** đọc từ HDFS -> phân tích -> ghi kết quả vào PostgreSQL local.
3. **Spark MLlib** đọc từ HDFS -> train model -> lưu model + metrics vào PostgreSQL local.
4. **Kafka Producer** đọc `orders_test` (mô phỏng stream) -> Kafka Topic -> **Spark Structured Streaming** consume -> predict realtime -> ghi vào PostgreSQL.
5. **Sync script** đẩy kết quả từ PostgreSQL local lên **Supabase** (cloud).
6. **FastAPI** (Railway) đọc Supabase -> **Vercel** frontend gọi API -> hiển thị dashboard.

---

## 4. Công nghệ sử dụng

| Tầng | Công nghệ | Phiên bản |
|---|---|---|
| Container | Docker + Docker Compose | ... |
| Distributed Storage | Apache Hadoop HDFS | ... |
| Distributed Computing | Apache Spark + PySpark | ... |
| Message Broker | Apache Kafka + Zookeeper | ... |
| Notebook | Jupyter Lab (PySpark kernel) | Latest |
| Local Database | PostgreSQL | ... |
| Cloud Database | Supabase (PostgreSQL) | Free tier |
| Backend API | FastAPI + SQLAlchemy | Latest |
| Frontend | Next.js (React) | ... |
| Deploy Frontend | Vercel | Free tier |
| Deploy Backend | Railway | Free tier |
| Language | Python 3. ... | — |

---

## 5. Cấu trúc thư mục

```
Nhom_05/
├── README.md                        <- File này
├── docker-compose.yml               <- Cấu hình toàn bộ cluster
│
├── infra/                           <- Cấu hình Hadoop & Spark (M1)
│   ├── hadoop/
│   │   ├── Dockerfile
│   │   ├── core-site.xml
│   │   ├── hdfs-site.xml
│   │   ├── mapred-site.xml
│   │   └── yarn-site.xml
│   └── spark/
│       ├── Dockerfile
│       └── spark-defaults.conf
│
├── src/                             <- Toàn bộ source code
│   ├── 01_data_engineering/         <- M1: Hạ tầng & tiền xử lý
│   │   ├── upload_to_hdfs.sh        <- Script upload CSV lên HDFS
│   │   ├── verify_hdfs.sh           <- Script kiểm tra HDFS
│   │   └── 01_preprocessing.ipynb  <- EDA + làm sạch + partition
│   │
│   ├── 02_spark_sql/                <- M2: Phân tích SQL
│   │   └── 02_spark_sql_queries.ipynb  <- Thực hiện truy vấn nâng cao
│   │
│   ├── 03_mllib/                    <- M3: Học máy
│   │   ├── 03a_kmeans_clustering.ipynb     <- Phân cụm khách hàng
│   │   └── 03b_reorder_prediction.ipynb    <- Dự báo mua lại
│   │
│   └── 04_streaming/               <- M4: Streaming
│       ├── kafka_producer.py        <- Mô phỏng luồng đơn hàng
│       ├── spark_streaming.ipynb    <- Spark Structured Streaming
│       └── sync_to_supabase.py      <- Đẩy kết quả lên cloud
│
├── backend/                         <- M4: FastAPI (deploy Railway)
│   ├── main.py
│   ├── routers/
│   │   ├── analytics.py             <- Endpoints SQL results
│   │   ├── predictions.py           <- Endpoints ML predictions
│   │   └── streaming.py             <- SSE endpoint realtime
│   ├── models.py
│   ├── database.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── frontend/                        <- M4: Next.js (deploy Vercel)
│   ├── app/
│   │   ├── page.tsx                 <- Dashboard tổng quan
│   │   ├── analytics/page.tsx       <- Kết quả SQL
│   │   ├── ml/page.tsx              <- Kết quả ML
│   │   └── streaming/page.tsx       <- Demo realtime
│   ├── components/
│   ├── package.json
│   └── next.config.js
│
├── data/                            <- Không commit lên Git (gitignore)
│   └── .gitkeep
│
└── docs/
    ├── report/                      <- File Word báo cáo
    └── slides/                      <- File PowerPoint
```

---

## 6. Phân công công việc

### Bảng phân công

| Thành viên | Vai trò | Phạm vi công việc | Branch |
|---|---|---|---|
| Lê Vy **(M1)** | Data Engineer | Docker Compose, Hadoop config, HDFS upload, EDA + Preprocessing pipeline | `/m1-infra` |
| Nguyễn Duy Tân **(M2)** | Data Analyst | Spark SQL queries (Window Functions, Multi-join, Subquery) | `/m2-sql` |
| Nguyễn Thị Thùy Dương **(M3)** | ML Engineer | K-Means clustering (Silhouette), RandomForest reorder prediction | `/m3-mllib` |
| Thái Hoài An **(M4)** | Streaming + Web | Kafka producer, Spark Streaming, FastAPI, Vercel frontend, cloud deploy | `/m4-streaming` |

### Chi tiết công việc từng thành viên

**Lê Vy - Data Engineer**
- ...

**Nguyễn Duy Tân - Data Analyst**
- ...

**Nguyễn Thị Thùy Dương - ML Engineer**
- ...

**Thái Hoài An - Streaming + Web**
- ...

