# Instacart MLlib Pipeline

Folder này chứa 3 script ML chính, tách từ workflow Colab và chạy lại qua `local_train_mllib.py` để giữ kết quả nhất quán.

## 1. Chuẩn bị dữ liệu

Đặt đủ 6 file CSV Instacart trong thư mục `data/` ở root project:

```text
InstaCart-Online-Basket-Analysis/
  data/
    orders.csv
    order_products__prior.csv
    order_products__train.csv
    products.csv
    aisles.csv
    departments.csv
```

Khi chạy trong Docker, thư mục này được mount vào container tại:

```text
/home/nhom05/data
```

## 2. Chạy bằng Docker

Khởi động Docker stack trước:

```bash
cd docker
./script/start-master.sh --with-virtual-worker
```

Sau đó chạy từng bài toán:

```bash
docker exec jupyter python3 /home/nhom05/work/03_ml/01_reorder_classifier.py
docker exec jupyter python3 /home/nhom05/work/03_ml/02_customer_segmentation.py
docker exec jupyter python3 /home/nhom05/work/03_ml/03_market_basket_fpgrowth.py
```

## 3. Chạy trực tiếp local

Từ root project:

```bash
python3 src/03_ml/01_reorder_classifier.py
python3 src/03_ml/02_customer_segmentation.py
python3 src/03_ml/03_market_basket_fpgrowth.py
```

## 4. Output

Mặc định mỗi script ghi output vào `local_outputs/`:

```text
local_outputs/
  01_reorder_classifier_seed42/
  02_customer_segmentation_seed42/
  03_market_basket_seed42/
```

Trong mỗi output folder có:

```text
features/   parquet feature tables
models/     Spark MLlib models
reports/    JSON metrics and summaries
```

File summary chính:

```text
reports/summary.json
```

## 5. Tham số cố định

Các script đã cố định theo cấu hình Colab đã chạy:

```text
seed = 42
sample-fraction = 1.0
driver-memory = 8g
shuffle-partitions = 64
default-parallelism = 64
feature-config = selected_features_for_mllib.generated.json
```

Không đổi các tham số này nếu mục tiêu là giữ kết quả nhất quán giữa các máy.

## 6. Ghi chú

- `local_train_mllib.py` là source of truth. Ba script `01`, `02`, `03` chỉ là runner chọn từng task.
- `selected_features_for_mllib.generated.json` là feature config đã dùng trong Colab.
- Nếu Docker bị OOM, tăng `JUPYTER_MEM` trong `docker/.env`, ví dụ:

```text
JUPYTER_MEM=10g
```
