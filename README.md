# Instacart Online Basket Analysis

## 1. Nội dung đồ án

Đồ án phân tích hành vi mua hàng trên bộ dữ liệu Instacart, sử dụng hệ sinh thái Big Data.

- Dữ liệu được lưu trữ trên **Hadoop HDFS**.
- Xử lý và phân tích bằng **Apache Spark**, **PySpark** và **Spark SQL**.
- Nội dung chính: tiền xử lý dữ liệu, feature engineering, EDA bằng Spark SQL, trực quan hóa insight và huấn luyện mô hình với MLlib.
- Các bài toán chính:
  - Phân tích hành vi mua hàng
  - Sản phẩm được mua lại nhiều nhất
  - Sản phẩm thường được mua cùng nhau
  - Phân khúc khách hàng
  - Dự đoán khả năng reorder

---

## 2. Danh sách thành viên

| STT | MSSV | Họ và Tên | Phân công công việc |
|--:|---|---|---|
| 1 | 31231025020 | Thái Hoài An | Phụ trách feature engineering và MLlib, xây dựng đặc trưng cho dữ liệu, chuẩn bị dataset cho mô hình reorder prediction, customer segmentation và market basket analysis. |
| 12 | 31231022904 | Nguyễn Thị Thuỳ Dương | Phụ trách Spark SQL và hỗ trợ thiết lập worker, thực hiện các truy vấn EDA, phân tích sản phẩm mua chung, department matrix và trực quan hóa kết quả. |
| 37 | 31231023384 | Nguyễn Duy Tân | Phụ trách Spark SQL và hỗ trợ thiết lập worker, xây dựng các truy vấn phân tích, viết nhận xét biểu đồ và kiểm tra notebook khi chạy trên Spark/HDFS. |
| 45 | 31231022128 | Lê Vy | Phụ trách cấu hình hạ tầng single-node và master, thiết lập Docker, Hadoop, Spark, Jupyter, HDFS và hỗ trợ cấu hình cluster. |

---

## 3. Cấu trúc repo

```text
InstaCart-Online-Basket-Analysis/
├── data/                   # Dữ liệu gốc Instacart (không commit)
├── docker/                 # Cấu hình môi trường cluster master/worker
├── docker-single/          # Cấu hình môi trường single-node
├── src/
│   ├── 01_preprocessing/   # Upload dữ liệu lên HDFS và feature engineering
│   ├── 02_spark_sql/       # Notebook Spark SQL và EDA
│   └── 03_ml/              # Script và notebook MLlib
├── .gitignore
└── README.md
```

---

## 4. Hướng dẫn cài đặt

### 4.1. Chuẩn bị dữ liệu

Đặt các file CSV vào thư mục `data/`:

```text
data/
├── orders.csv
├── order_products__prior.csv
├── order_products__train.csv
├── products.csv
├── aisles.csv
└── departments.csv
```

### 4.2. Chạy từ thư mục gốc

```bash
cd InstaCart-Online-Basket-Analysis/
```

### 4.3. Upload dữ liệu lên HDFS

```bash
docker cp data/. namenode:/tmp/instacart-data/
docker cp src/01_preprocessing/01_upload_to_hdfs.sh namenode:/tmp/
docker exec -e DATA_DIR=/tmp/instacart-data namenode bash /tmp/01_upload_to_hdfs.sh
```

- Copy dữ liệu vào container `namenode`.
- Copy script upload dữ liệu.
- Chạy script để đưa dữ liệu lên HDFS.

---

## 5. Công nghệ sử dụng

- **Docker**, Docker Compose
- **Hadoop HDFS**
- **Apache Spark**, PySpark, Spark SQL
- **Spark MLlib**
- **Jupyter Notebook**
- Python, Pandas, Matplotlib, Seaborn
- Tailscale

---

## 6. Ghi chú

- Không commit thư mục `data/`.
- Không commit file `.env` chứa IP thật.
- Khi chạy cluster, cần start **master** trước rồi mới start **worker**.
- Nếu máy yếu, nên dùng môi trường **single-node**.
