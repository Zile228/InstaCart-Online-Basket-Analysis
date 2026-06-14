# ============================================================
#  02_feature_engineering.py
#  Instacart Market Basket Analysis — Feature Engineering
#  PySpark 4.1.1, HDFS hdfs://namenode:9000
#
#  Chạy cách 1 (trong Jupyter):  Mở file, run từng cell
#  Chạy cách 2 (spark-submit):
#    spark-submit \
#      --master spark://spark-master:7077 \
#      --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
#      02_feature_engineering.py
#
#  Convert sang .ipynb:
#    pip install jupytext
#    jupytext --to notebook 02_feature_engineering.py
# ============================================================

# %% [markdown]
# # Instacart Feature Engineering
# Notebook này tính toán 3 nhóm features chính:
# - **User features**: hành vi tổng hợp của từng user
# - **Product features**: đặc trưng của từng sản phẩm
# - **User-Product interaction features**: tương tác cặp (user, product)
#
# Sau đó xây dựng **Training Dataset** cho mô hình reorder classification.

# %%
# ============================================================
#  CELL 1 — Khởi tạo SparkSession
# ============================================================
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType

# Đọc HDFS namenode từ env (Docker: hdfs://namenode:9000, local: hdfs://localhost:9000)
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")
SPARK_MASTER   = os.getenv("SPARK_MASTER",  "spark://spark-master:7077")

print(f"HDFS NameNode : {HDFS_NAMENODE}")
print(f"Spark Master  : {SPARK_MASTER}")

spark = SparkSession.builder \
    .appName("Instacart-FeatureEngineering") \
    .master(SPARK_MASTER) \
    .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE) \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

# Reduce log noise — show only WARN and above
spark.sparkContext.setLogLevel("WARN")

print(f"\nSpark version : {spark.version}")
print(f"Scala version : {spark.sparkContext._jvm.scala.util.Properties.versionString()}")
print("SparkSession initialized successfully ✓")


def read_instacart_csv(path):
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("quote", '"')
        .option("escape", "\\")
        .option("multiLine", True)
        .csv(path)
    )


def read_products_csv(path):
    import csv
    from io import StringIO
    from pyspark.sql.types import StructField, StringType, StructType

    text = "\n".join(spark.sparkContext.textFile(path).collect())
    reader = csv.DictReader(StringIO(text))
    rows = [
        (
            int(row["product_id"]),
            row["product_name"],
            int(row["aisle_id"]),
            int(row["department_id"]),
        )
        for row in reader
    ]
    schema = StructType(
        [
            StructField("product_id", IntegerType(), False),
            StructField("product_name", StringType(), True),
            StructField("aisle_id", IntegerType(), True),
            StructField("department_id", IntegerType(), True),
        ]
    )
    return spark.createDataFrame(rows, schema)

# %% [markdown]
# ## CELL 2 — Load 6 bảng từ HDFS

# %%
# ============================================================
#  CELL 2 — Load raw data từ HDFS
# ============================================================
BASE = f"{HDFS_NAMENODE}/instacart/raw"
FEATURES_BASE = f"{HDFS_NAMENODE}/instacart/features"

print(f"Loading data from: {BASE}")
print("-" * 50)

# Load 6 CSV tables. `products.csv` contains escaped quotes and commas inside
# product names, so keep the same CSV options everywhere for schema stability.
orders      = read_instacart_csv(f"{BASE}/orders.csv")
prior       = read_instacart_csv(f"{BASE}/order_products__prior.csv")
train       = read_instacart_csv(f"{BASE}/order_products__train.csv")
products    = read_products_csv(f"{BASE}/products.csv")
aisles      = read_instacart_csv(f"{BASE}/aisles.csv")
departments = read_instacart_csv(f"{BASE}/departments.csv")

# Verify shape of each table
tables = [
    ("orders",      orders),
    ("prior",       prior),
    ("train",       train),
    ("products",    products),
    ("aisles",      aisles),
    ("departments", departments),
]

print("\n{:<25} {:>12} {:>8}".format("Table", "Rows", "Cols"))
print("-" * 47)
for name, df in tables:
    row_count = df.count()
    col_count = len(df.columns)
    print("{:<25} {:>12,} {:>8}".format(name, row_count, col_count))

print("\nSchemas:")
for name, df in tables:
    print(f"\n--- {name} ---")
    df.printSchema()

# %% [markdown]
# ## CELL 3 — Join prior với orders và products

# %%
# ============================================================
#  CELL 3 — Build enriched prior DataFrame
#  Joins: prior ← orders_prior ← products ← aisles ← departments
# ============================================================

# Filter orders to prior set only
orders_prior = orders.filter(F.col("eval_set") == "prior")

# Build full products table with aisle and department names
products_full = products \
    .join(aisles,      on="aisle_id",      how="left") \
    .join(departments, on="department_id", how="left")

# Join prior transactions with order metadata and product info
prior_full = prior \
    .join(
        orders_prior.select(
            "order_id", "user_id", "order_number",
            "days_since_prior_order", "order_dow", "order_hour_of_day"
        ),
        on="order_id",
        how="inner"
    ) \
    .join(
        products_full.select(
            "product_id", "product_name",
            "aisle_id", "department_id",
            "aisle", "department"
        ),
        on="product_id",
        how="inner"
    )

# Handle nulls: days_since_prior_order is null for a user's very first order
prior_full = prior_full.fillna({"days_since_prior_order": 0.0})

# Add is_organic flag: 1 if product name contains "Organic", else 0
prior_full = prior_full.withColumn(
    "is_organic",
    F.when(F.col("product_name").contains("Organic"), 1).otherwise(0)
)

# Cache — this DataFrame is used in all 3 feature groups (Cells 4, 5, 6)
# Caching prevents recomputation from scratch each time
prior_full.cache()
total_rows = prior_full.count()

print(f"prior_full rows : {total_rows:,}")
print(f"prior_full cols : {len(prior_full.columns)}")
print("\nSchema:")
prior_full.printSchema()
print("\nSample rows:")
prior_full.show(3, truncate=True)

# %% [markdown]
# ## CELL 4 — User-Level Features

# %%
# ============================================================
#  CELL 4 — USER-LEVEL FEATURES
#  Tính 10 feature tổng hợp cho mỗi user từ prior_full
# ============================================================

# --- Base aggregations ---
user_base = prior_full.groupBy("user_id").agg(
    F.countDistinct("order_id").alias("u_total_orders"),
    F.count("*").alias("u_total_items"),
    F.countDistinct("product_id").alias("u_distinct_products"),
    # Reorder stats
    F.sum(F.col("reordered").cast(DoubleType())).alias("_reorder_sum"),
    # Days between orders (nulls already filled with 0 above)
    F.avg(F.col("days_since_prior_order")).alias("u_avg_days_since_prior"),
    F.stddev(F.col("days_since_prior_order")).alias("u_std_days_since_prior"),
    # Organic ratio
    F.sum(F.col("is_organic").cast(DoubleType())).alias("_organic_sum"),
    # Keep these in sync with src/03_ml/local_train_mllib.py.
    F.countDistinct("aisle_id").alias("u_unique_aisles"),
    F.countDistinct("department_id").alias("u_unique_departments"),
    F.sum(F.when(F.col("department") == "produce", 1.0).otherwise(0.0)).alias("_produce_sum"),
    F.sum(F.when(F.col("department") == "dairy eggs", 1.0).otherwise(0.0)).alias("_dairy_sum")
)

# Derived columns
user_base = user_base \
    .withColumn(
        "u_avg_basket_size",
        F.col("u_total_items") / F.col("u_total_orders")
    ) \
    .withColumn(
        "u_reorder_rate",
        F.col("_reorder_sum") / F.col("u_total_items")
    ) \
    .withColumn(
        "u_organic_ratio",
        F.col("_organic_sum") / F.col("u_total_items")
    ) \
    .withColumn(
        "u_produce_ratio",
        F.col("_produce_sum") / F.col("u_total_items")
    ) \
    .withColumn(
        "u_dairy_ratio",
        F.col("_dairy_sum") / F.col("u_total_items")
    ) \
    .drop("_reorder_sum", "_organic_sum", "_produce_sum", "_dairy_sum")

# Handle null std (users with only 1 order have no variance)
user_base = user_base.fillna({"u_std_days_since_prior": 0.0})

# --- Preferred day of week (mode) ---
#  Count orders per (user, day), pick the day with max count
dow_counts = prior_full \
    .groupBy("user_id", "order_dow") \
    .agg(F.count("*").alias("_cnt"))

dow_window = Window.partitionBy("user_id").orderBy(
    F.desc("_cnt"), F.asc("order_dow")  # asc as tiebreaker
)
preferred_dow = dow_counts \
    .withColumn("_rn", F.row_number().over(dow_window)) \
    .filter(F.col("_rn") == 1) \
    .select("user_id", F.col("order_dow").alias("u_preferred_dow"))

# --- Preferred hour of day (mode) ---
hour_counts = prior_full \
    .groupBy("user_id", "order_hour_of_day") \
    .agg(F.count("*").alias("_cnt"))

hour_window = Window.partitionBy("user_id").orderBy(
    F.desc("_cnt"), F.asc("order_hour_of_day")
)
preferred_hour = hour_counts \
    .withColumn("_rn", F.row_number().over(hour_window)) \
    .filter(F.col("_rn") == 1) \
    .select("user_id", F.col("order_hour_of_day").alias("u_preferred_hour"))

# --- Combine all user features ---
user_features = user_base \
    .join(preferred_dow,  on="user_id", how="left") \
    .join(preferred_hour, on="user_id", how="left")

# Cache user_features — used again in Cell 6 and Cell 7
user_features.cache()
user_count = user_features.count()

print(f"user_features rows : {user_count:,}")
print(f"user_features cols : {len(user_features.columns)}")
print("\nSchema:")
user_features.printSchema()
print("\nSample (5 rows):")
user_features.show(5, truncate=False)

# --- Save to HDFS ---
user_features.write.parquet(
    f"{FEATURES_BASE}/user_features.parquet",
    mode="overwrite"
)
print(f"\n✓ Saved user_features to {FEATURES_BASE}/user_features.parquet")

# %% [markdown]
# ## CELL 5 — Product-Level Features

# %%
# ============================================================
#  CELL 5 — PRODUCT-LEVEL FEATURES
#  Tính 7 feature tổng hợp cho mỗi sản phẩm từ prior_full
# ============================================================

product_features = prior_full.groupBy("product_id").agg(
    F.count("*").alias("p_total_orders"),
    F.sum(F.col("reordered").cast(DoubleType())).alias("_reorder_sum"),
    F.countDistinct("user_id").alias("p_unique_users"),
    F.avg(F.col("add_to_cart_order")).alias("p_avg_add_to_cart_order"),
    # Use first() — these are static attributes of the product
    F.first("is_organic").alias("p_is_organic"),
    F.first("department_id").alias("p_department_id"),
    F.first("aisle_id").alias("p_aisle_id")
)

# Compute reorder rate
product_features = product_features \
    .withColumn(
        "p_reorder_rate",
        F.col("_reorder_sum") / F.col("p_total_orders")
    ) \
    .drop("_reorder_sum")

# Cache — used in Cell 7 (training dataset join)
product_features.cache()
product_count = product_features.count()

print(f"product_features rows : {product_count:,}")
print(f"product_features cols : {len(product_features.columns)}")
print("\nSchema:")
product_features.printSchema()
print("\nTop 10 products by total orders:")
product_features.orderBy(F.desc("p_total_orders")).show(10)

# --- Save to HDFS ---
product_features.write.parquet(
    f"{FEATURES_BASE}/product_features.parquet",
    mode="overwrite"
)
print(f"\n✓ Saved product_features to {FEATURES_BASE}/product_features.parquet")

# %% [markdown]
# ## CELL 6 — User-Product Interaction Features

# %%
# ============================================================
#  CELL 6 — USER-PRODUCT INTERACTION FEATURES (quan trọng nhất)
#  7 feature cho mỗi cặp (user_id, product_id)
# ============================================================

# --- Base metrics from prior_full ---
up_base = prior_full.groupBy("user_id", "product_id").agg(
    F.count("*").alias("up_order_count"),
    F.avg(F.col("add_to_cart_order")).alias("up_avg_position"),
    F.min("order_number").alias("up_first_order_number"),
    F.max("order_number").alias("up_last_order_number")
)

# --- Join with user_features to get u_total_orders for rate calculations ---
#  up_reorder_rate = how often user buys THIS product across ALL their orders
up_features = up_base.join(
    user_features.select("user_id", "u_total_orders"),
    on="user_id",
    how="inner"
)

# --- Derived interaction metrics ---
up_features = up_features \
    .withColumn(
        "up_reorder_rate",
        F.col("up_order_count") / F.col("u_total_orders")
    ) \
    .withColumn(
        # How many orders ago was the last purchase of this product?
        # High value = user hasn't bought it recently (low recency)
        "up_orders_since_last",
        F.col("u_total_orders") - F.col("up_last_order_number")
    ) \
    .withColumn(
        # Purchase rate since first time user bought this product
        # Accounts for the "discovery" period
        "up_order_rate_since_first",
        F.col("up_order_count") / (
            F.col("u_total_orders") - F.col("up_first_order_number") + 1
        )
    ) \
    .drop("u_total_orders")  # already in user_features; drop to avoid duplication

# Cache — used in Cell 7 and Cell 9
up_features.cache()
up_count = up_features.count()

print(f"up_features rows : {up_count:,}")
print(f"up_features cols : {len(up_features.columns)}")
print("\nSchema:")
up_features.printSchema()
print("\nSample (5 rows):")
up_features.show(5)

# --- Save to HDFS ---
up_features.write.parquet(
    f"{FEATURES_BASE}/user_product_features.parquet",
    mode="overwrite"
)
print(f"\n✓ Saved user_product_features to {FEATURES_BASE}/user_product_features.parquet")

# %% [markdown]
# ## CELL 7 — Build Training Dataset

# %%
# ============================================================
#  CELL 7 — BUILD TRAINING DATASET
#
#  Logic:
#   1. Candidates = all (user, product) pairs user bought in prior
#                   for users who appear in the TRAIN eval set
#   2. Labels     = which of those products appear in train orders
#                   (positive=1) vs not (negative=0)
#   3. Final      = candidates + labels + user_features + product_features
#
#  This is correct negative sampling: only products the user has
#  seen before are considered. We predict if they'll reorder.
# ============================================================

# Orders in the train set (each user's "last" order we want to predict)
orders_train = orders.filter(F.col("eval_set") == "train")

# Positive labels: (user, product) pairs that ACTUALLY appear in train orders
# with reordered=1 (confirmed reorder) — note: train also has reordered=0 for new items
train_positive = train \
    .join(
        orders_train.select("order_id", "user_id"),
        on="order_id",
        how="inner"
    ) \
    .select("user_id", "product_id", F.col("reordered").cast(IntegerType()))

# Unique users in the train set (we only build candidates for these users)
train_user_ids = orders_train.select("user_id").distinct()

print(f"Users in train set: {train_user_ids.count():,}")
print(f"Positive labels in train: {train_positive.count():,}")

# Candidates: ALL (user, product) pairs from prior history
#             but only for users who have a train order
candidates = up_features \
    .join(train_user_ids, on="user_id", how="inner")

print(f"Total candidate (user, product) pairs: {candidates.count():,}")

# Join candidates with actual labels
# Products NOT in train get reordered=0 (they are the negatives)
training_data = candidates \
    .join(
        train_positive.select("user_id", "product_id", "reordered"),
        on=["user_id", "product_id"],
        how="left"
    ) \
    .fillna({"reordered": 0})

# Enrich with user-level and product-level features
training_data = training_data \
    .join(user_features,    on="user_id",    how="left") \
    .join(product_features, on="product_id", how="left")

# Write training dataset to HDFS
training_data.write.parquet(
    f"{FEATURES_BASE}/train_dataset.parquet",
    mode="overwrite"
)

# Count label distribution
total         = training_data.count()
pos_count     = training_data.filter(F.col("reordered") == 1).count()
neg_count     = training_data.filter(F.col("reordered") == 0).count()
pos_rate      = pos_count / total * 100

print("\n" + "=" * 50)
print("  TRAINING DATASET SUMMARY")
print("=" * 50)
print(f"  Total rows     : {total:>12,}")
print(f"  Positive (=1)  : {pos_count:>12,}  ({pos_rate:.1f}%)")
print(f"  Negative (=0)  : {neg_count:>12,}  ({100-pos_rate:.1f}%)")
print(f"  Columns        : {len(training_data.columns)}")
print("=" * 50)

print("\nSchema:")
training_data.printSchema()
print("\nSample rows:")
training_data.show(5, truncate=True)

print(f"\n✓ Saved train_dataset to {FEATURES_BASE}/train_dataset.parquet")

# %% [markdown]
# ## CELL 8 — RFV Features for Customer Segmentation

# %%
# ============================================================
#  CELL 8 — RFV FEATURES (Recency, Frequency, Volume)
#  Used by KMeans customer segmentation in Notebook 03
#
#  R = Recency   : days since last order (lower = more active)
#  F = Frequency : total orders placed
#  V = Volume    : average basket size
# ============================================================

# --- Recency: days_since_prior_order of the user's LAST order ---
#  Use Window to find the order with the highest order_number per user
w_last = Window.partitionBy("user_id").orderBy(F.desc("order_number"))

recency_df = orders_prior \
    .withColumn("_rn", F.row_number().over(w_last)) \
    .filter(F.col("_rn") == 1) \
    .select(
        "user_id",
        F.col("days_since_prior_order").alias("recency")
    ) \
    .fillna({"recency": 0.0})  # First-ever order has no prior → treat as 0

# --- Combine R, F, V ---
rfv_features = user_features.select(
    "user_id",
    F.col("u_total_orders").alias("frequency"),
    F.col("u_avg_basket_size").alias("volume"),
    F.col("u_reorder_rate"),
    F.col("u_organic_ratio"),
    F.col("u_distinct_products"),
    F.col("u_unique_departments"),
    F.col("u_produce_ratio"),
    F.col("u_dairy_ratio")
).join(recency_df, on="user_id", how="left") \
 .fillna({"recency": 0.0})

# Cache for use in segmentation notebook
rfv_features.cache()
rfv_count = rfv_features.count()

print(f"rfv_features rows : {rfv_count:,}")
print(f"rfv_features cols : {len(rfv_features.columns)}")
print("\nSchema:")
rfv_features.printSchema()
print("\nDescriptive statistics:")
rfv_features.select("recency","frequency","volume","u_reorder_rate","u_organic_ratio") \
    .describe().show()

# --- Save to HDFS ---
rfv_features.write.parquet(
    f"{FEATURES_BASE}/rfv_features.parquet",
    mode="overwrite"
)
print(f"\n✓ Saved rfv_features to {FEATURES_BASE}/rfv_features.parquet")

# NOTE: StandardScaler normalization is done in 02_customer_segmentation.ipynb
# since it's part of the MLlib Pipeline there.

# %% [markdown]
# ## CELL 9 — Export CSV for Supabase

# %%
# ============================================================
#  CELL 9 — EXPORT CSV FILES FOR SUPABASE UPLOAD
#  Converts small Spark DataFrames to Pandas and saves as CSV.
#  These files are later uploaded by upload_to_supabase.py
# ============================================================
import os
import pandas as pd

EXPORT_PATH = os.getenv("EXPORT_DIR", "/home/nhom05/work/exports")
os.makedirs(EXPORT_PATH, exist_ok=True)
print(f"Export directory: {EXPORT_PATH}")
print("-" * 50)

# ── Export 1: Hourly Heatmap (168 rows: 7 days × 24 hours) ──
print("Exporting hourly_heatmap.csv ...")
hourly_heatmap = orders \
    .groupBy("order_dow", "order_hour_of_day") \
    .agg(F.count("*").alias("order_count")) \
    .orderBy("order_dow", "order_hour_of_day")

hourly_heatmap_pd = hourly_heatmap.toPandas()
hourly_heatmap_pd.to_csv(f"{EXPORT_PATH}/hourly_heatmap.csv", index=False)
print(f"  ✓ hourly_heatmap.csv — {len(hourly_heatmap_pd):,} rows")

# ── Export 2: Top 500 Products ──────────────────────────────
print("Exporting top_products.csv ...")
top_products = prior_full \
    .groupBy(
        "product_id", "product_name", "department", "aisle", "is_organic"
    ) \
    .agg(
        F.count("*").alias("total_orders"),
        F.sum(F.col("reordered").cast(DoubleType())).alias("reorder_count")
    ) \
    .withColumn(
        "reorder_rate",
        F.round(F.col("reorder_count") / F.col("total_orders"), 4)
    ) \
    .drop("reorder_count") \
    .orderBy(F.desc("total_orders")) \
    .limit(500)

top_products_pd = top_products.toPandas()
top_products_pd.to_csv(f"{EXPORT_PATH}/top_products.csv", index=False)
print(f"  ✓ top_products.csv — {len(top_products_pd):,} rows")

# ── Export 3: Department Statistics (21 departments) ────────
print("Exporting department_stats.csv ...")
dept_stats = prior_full \
    .groupBy("department_id", "department") \
    .agg(
        F.count("*").alias("total_orders"),
        F.round(
            F.sum(F.col("reordered").cast(DoubleType())) / F.count("*"), 4
        ).alias("reorder_rate"),
        F.countDistinct("product_id").alias("unique_products")
    ) \
    .orderBy(F.desc("total_orders"))

dept_stats_pd = dept_stats.toPandas()
dept_stats_pd.to_csv(f"{EXPORT_PATH}/department_stats.csv", index=False)
print(f"  ✓ department_stats.csv — {len(dept_stats_pd):,} rows")

print("\n" + "=" * 50)
print("  All CSV exports completed!")
print(f"  Location: {EXPORT_PATH}/")
print("=" * 50)

# %% [markdown]
# ## CELL 10 — Summary & Validation

# %%
# ============================================================
#  CELL 10 — VALIDATION SUMMARY
#  Đọc lại tất cả parquet files để xác nhận schema và row counts
# ============================================================

print("=" * 60)
print("  FEATURE ENGINEERING — FINAL VALIDATION")
print("=" * 60)

parquet_files = {
    "user_features"         : f"{FEATURES_BASE}/user_features.parquet",
    "product_features"      : f"{FEATURES_BASE}/product_features.parquet",
    "user_product_features" : f"{FEATURES_BASE}/user_product_features.parquet",
    "rfv_features"          : f"{FEATURES_BASE}/rfv_features.parquet",
    "train_dataset"         : f"{FEATURES_BASE}/train_dataset.parquet",
}

print(f"\n{'Table':<30} {'Rows':>12} {'Cols':>6}")
print("-" * 50)

for name, path in parquet_files.items():
    try:
        df = spark.read.parquet(path)
        rows = df.count()
        cols = len(df.columns)
        print(f"{name:<30} {rows:>12,} {cols:>6}")
    except Exception as e:
        print(f"{name:<30} {'ERROR':>12} — {e}")

# --- Detailed schema for train_dataset ---
print("\n--- train_dataset schema ---")
spark.read.parquet(parquet_files["train_dataset"]).printSchema()

# --- Sample from train_dataset ---
print("--- train_dataset sample (5 rows) ---")
spark.read.parquet(parquet_files["train_dataset"]).show(5, truncate=True)

# --- Label distribution check ---
print("--- train_dataset label distribution ---")
td = spark.read.parquet(parquet_files["train_dataset"])
td.groupBy("reordered").count() \
  .withColumn("pct", F.round(F.col("count") / td.count() * 100, 2)) \
  .orderBy("reordered") \
  .show()

# --- HDFS storage usage ---
print("\n--- HDFS storage summary ---")
import subprocess
result = subprocess.run(
    ["hdfs", "dfs", "-du", "-h", "/instacart/"],
    capture_output=True, text=True
)
print(result.stdout or result.stderr)

print("\n" + "=" * 60)
print("  Feature Engineering COMPLETE ✓")
print("  Next steps:")
print("    → src/02_sql/spark_sql_analysis.ipynb   (Người 2)")
print("    → src/03_ml/01_reorder_classifier.ipynb (Người 3)")
print("=" * 60)

# ── Clean up Spark session ──────────────────────────────────
spark.stop()
print("\nSparkSession stopped.")
