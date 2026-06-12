# ============================================================
#  03_als_recommender.py
#  Instacart — ALS Collaborative Filtering + FP-Growth
#  PySpark MLlib 4.1.1
#
#  Notebook này gồm 2 phần:
#    PART A — ALS: dự đoán sản phẩm user chưa mua nhưng có thể thích
#    PART B — FP-Growth: tìm association rules "ai mua A thường mua B"
#
#  Chạy trong Jupyter: mỗi cell phân cách bởi # %%
#  Chạy spark-submit:
#    spark-submit \
#      --master spark://spark-master:7077 \
#      --executor-memory 2g \
#      03_als_recommender.py
# ============================================================

# %% [markdown]
# # PART A — ALS Collaborative Filtering
# Dùng Alternating Least Squares để học latent factors từ lịch sử mua hàng,
# sau đó gợi ý top-N sản phẩm cho mỗi user.

# %%
# ============================================================
#  CELL 1 — SparkSession
# ============================================================
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.fpm import FPGrowth

HDFS      = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")
SPARK_URL = os.getenv("SPARK_MASTER",  "spark://spark-master:7077")
FEAT      = f"{HDFS}/instacart/features"
RAW       = f"{HDFS}/instacart/raw"
MODELS    = f"{HDFS}/instacart/models"

spark = SparkSession.builder \
    .appName("Instacart-ALS-FPGrowth") \
    .master(SPARK_URL) \
    .config("spark.hadoop.fs.defaultFS", HDFS) \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(f"Spark version : {spark.version}")
print(f"HDFS          : {HDFS}")

# %%
# ============================================================
#  CELL 2 — Load dữ liệu cần thiết
# ============================================================
# ALS cần: user_id (Int), product_id (Int), rating (Float)
# Ta dùng up_order_count làm implicit rating
# FP-Growth cần: baskets (mỗi đơn hàng là 1 list sản phẩm)

print("Loading user-product features...")
up_features = spark.read.parquet(f"{FEAT}/user_product_features.parquet")
up_features.cache()
print(f"  user_product_features: {up_features.count():,} rows")


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
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

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


print("Loading prior orders (for FP-Growth baskets)...")
prior = read_instacart_csv(f"{RAW}/order_products__prior.csv")
prior.cache()
print(f"  prior: {prior.count():,} rows")

print("Loading products...")
products = read_products_csv(f"{RAW}/products.csv")

print("All data loaded ✓")

# %%
# ============================================================
#  CELL 3 — Chuẩn bị ALS ratings matrix
#
#  Dùng up_order_count làm implicit rating:
#    - user mua sản phẩm 5 lần → rating cao hơn 1 lần
#    - ALS sẽ học latent factors từ ma trận này
# ============================================================
print("=== PART A: ALS Collaborative Filtering ===")
print("\nPreparing ratings matrix...")

ratings = up_features.select(
    F.col("user_id").cast("int"),
    F.col("product_id").cast("int"),
    F.col("up_order_count").cast("float").alias("rating")
).filter(F.col("rating").isNotNull())

# Chia train/test 80-20 theo user (group split để tránh data leakage)
# Lấy 80% user làm train, 20% làm test
user_ids = ratings.select("user_id").distinct()
train_users, test_users = user_ids.randomSplit([0.8, 0.2], seed=42)

ratings_train = ratings.join(train_users, on="user_id")
ratings_test  = ratings.join(test_users,  on="user_id")

print(f"  Train pairs : {ratings_train.count():,}")
print(f"  Test pairs  : {ratings_test.count():,}")
print(f"  Unique users: {ratings.select('user_id').distinct().count():,}")
print(f"  Unique prods: {ratings.select('product_id').distinct().count():,}")

# %%
# ============================================================
#  CELL 4 — Train ALS model (implicit feedback)
#
#  Tham số:
#    rank=20      → số latent factors (độ "chi tiết" của representation)
#    maxIter=10   → số vòng lặp ALS
#    regParam=0.1 → regularization để tránh overfitting
#    implicitPrefs=True → chỉ có positive feedback (không có "dislike")
#    coldStartStrategy="drop" → bỏ user/item không có trong train
# ============================================================
print("\nTraining ALS model (implicit feedback)...")

als = ALS(
    rank=20,
    maxIter=10,
    regParam=0.1,
    userCol="user_id",
    itemCol="product_id",
    ratingCol="rating",
    implicitPrefs=True,          # dùng implicit (count-based), không phải rating 1-5
    coldStartStrategy="drop",    # bỏ cold-start users/items khi predict
    nonnegative=True,            # latent factors không âm
    numUserBlocks=10,
    numItemBlocks=10,
    seed=42
)

als_model = als.fit(ratings_train)
print(f"  ✓ Model trained — rank={als.getRank()}, iter={als.getMaxIter()}")

# %%
# ============================================================
#  CELL 5 — Đánh giá model (RMSE trên test set)
#
#  Với implicit feedback, RMSE đo độ lệch giữa predicted score
#  và actual count. Số tuyệt đối ít ý nghĩa hơn so với ranking
#  metrics (NDCG, Precision@K), nhưng là proxy nhanh để so sánh.
# ============================================================
print("\nEvaluating ALS model...")

predictions_test = als_model.transform(ratings_test)
# Lọc bỏ NaN predictions (cold-start)
predictions_test = predictions_test.filter(F.col("prediction").isNotNull())

evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction"
)
rmse = evaluator.evaluate(predictions_test)
print(f"  RMSE (test)   : {rmse:.4f}")
print(f"  Note: với implicit feedback, RMSE ~ {rmse:.2f} orders là bình thường")
print(f"        Metric quan trọng hơn là Precision@K (xem bên dưới)")

# Sample predictions
print("\nSample predictions vs actual (first 10):")
predictions_test.select("user_id", "product_id", "rating", "prediction") \
    .orderBy("user_id", F.desc("rating")) \
    .show(10)

# %%
# ============================================================
#  CELL 6 — Generate Top-10 recommendations cho mỗi user
#
#  recommendForAllUsers(10) → với mỗi user trả về
#  DataFrame: [user_id, recommendations: [{product_id, rating}]]
#
#  Chỉ lấy users trong test set (để evaluation có ý nghĩa)
# ============================================================
print("\nGenerating top-10 recommendations for test users...")

# Recommend cho TẤT CẢ users trong tập test
user_recs = als_model.recommendForUserSubset(test_users, numItems=10)

# Explode recommendations array thành individual rows
user_recs_flat = user_recs.select(
    F.col("user_id"),
    F.explode(F.col("recommendations")).alias("rec")
).select(
    F.col("user_id"),
    F.col("rec.product_id").alias("product_id"),
    F.col("rec.rating").alias("predicted_score")
)

# Join với product names để dễ đọc
user_recs_named = user_recs_flat.join(
    products.select("product_id", "product_name"),
    on="product_id"
)

print(f"  Total recommendations: {user_recs_flat.count():,}")
print("\nSample recommendations (3 users):")
sample_users = test_users.limit(3).select("user_id")
user_recs_named.join(sample_users, on="user_id") \
    .orderBy("user_id", F.desc("predicted_score")) \
    .select("user_id", "product_name", F.round("predicted_score", 4).alias("score")) \
    .show(30, truncate=40)

# %%
# ============================================================
#  CELL 7 — Precision@K Evaluation (manual)
#
#  Với mỗi test user:
#    - Lấy top-K sản phẩm từ ALS recommendations
#    - So sánh với sản phẩm thực tế họ mua trong train (ground truth)
#    - Precision@K = số sản phẩm overlap / K
#
#  Đây là metric thực tế hơn RMSE cho recommendation systems
# ============================================================
print("\nCalculating Precision@K (K=10)...")
K = 10

# Ground truth: sản phẩm từng được mua trong prior (test users only)
ground_truth = ratings_test.join(test_users, on="user_id") \
    .select("user_id", "product_id") \
    .groupBy("user_id") \
    .agg(F.collect_set("product_id").alias("actual_products"))

# Top-K predicted
top_k_pred = user_recs_flat.join(test_users, on="user_id") \
    .groupBy("user_id") \
    .agg(F.collect_list("product_id").alias("pred_products"))

# Tính precision: |actual ∩ predicted| / K
precision_df = ground_truth.join(top_k_pred, on="user_id") \
    .select(
        F.col("user_id"),
        F.size(F.array_intersect(
            F.col("actual_products"),
            F.col("pred_products")
        )).alias("hits"),
    ).select(
        "user_id",
        "hits",
        (F.col("hits") / K).alias(f"precision_at_{K}")
    )

avg_precision = precision_df.agg(
    F.avg(f"precision_at_{K}").alias("mean_precision"),
    F.avg("hits").alias("avg_hits")
).collect()[0]

print(f"  Mean Precision@{K} : {avg_precision['mean_precision']:.4f}")
print(f"  Avg hits per user : {avg_precision['avg_hits']:.2f} / {K}")

# %%
# ============================================================
#  CELL 8 — Save ALS model + recommendations
# ============================================================
print("\nSaving ALS model to HDFS...")
als_model.write().overwrite().save(f"{MODELS}/als_model")
print(f"  ✓ Model saved: {MODELS}/als_model")

print("Saving top-10 recommendations for all users (full dataset)...")
# Train final model trên toàn bộ data
als_final = ALS(
    rank=20, maxIter=10, regParam=0.1,
    userCol="user_id", itemCol="product_id", ratingCol="rating",
    implicitPrefs=True, coldStartStrategy="drop",
    nonnegative=True, seed=42
)
als_model_full = als_final.fit(ratings)

all_recs = als_model_full.recommendForAllUsers(10)
all_recs_flat = all_recs.select(
    F.col("user_id"),
    F.explode(F.col("recommendations")).alias("rec")
).select(
    F.col("user_id"),
    F.col("rec.product_id").alias("product_id"),
    F.col("rec.rating").alias("predicted_score")
)

all_recs_flat.write.parquet(f"{FEAT}/als_recommendations.parquet", mode="overwrite")
print(f"  ✓ Recommendations saved: {FEAT}/als_recommendations.parquet")
print(f"  Total rows: {all_recs_flat.count():,}")

# %% [markdown]
# ---
# # PART B — FP-Growth Association Rules
# Tìm các bộ sản phẩm hay xuất hiện cùng nhau trong 1 đơn hàng,
# và khai thác association rules dạng: {Milk, Eggs} → {Butter}

# %%
# ============================================================
#  CELL 9 — Chuẩn bị Baskets cho FP-Growth
#
#  Mỗi basket = 1 đơn hàng, chứa list product_id
#  FP-Growth cần input: DataFrame với cột "items" kiểu Array
# ============================================================
print("\n=== PART B: FP-Growth Association Rules ===")
print("\nPreparing baskets...")

# Mỗi order_id → list product_ids (chỉ dùng prior set để có đủ data)
baskets = prior.groupBy("order_id") \
    .agg(F.collect_list(F.col("product_id").cast("string")).alias("items")) \
    .filter(F.size("items") >= 2)   # bỏ đơn chỉ có 1 sản phẩm

baskets.cache()
print(f"  Total baskets: {baskets.count():,}")
print(f"  Avg basket size: {baskets.select(F.avg(F.size('items'))).collect()[0][0]:.2f}")
baskets.select(F.size("items").alias("basket_size")).describe().show()

# %%
# ============================================================
#  CELL 10 — Train FP-Growth
#
#  minSupport = 0.01  → itemset phải xuất hiện trong ≥1% đơn hàng
#    (1% của 3.2M = ~32K đơn — tương đương ~32K sản phẩm phổ biến)
#  minConfidence = 0.3 → P(B|A) ≥ 30%
#
#  NOTE: FP-Growth trên 32M rows cần nhiều RAM. Nếu OutOfMemory:
#    - Tăng minSupport lên 0.02 hoặc 0.05
#    - Dùng sample: baskets.sample(fraction=0.1)
# ============================================================
print("\nTraining FP-Growth (this may take 5-15 minutes on full data)...")
print("  minSupport=0.01, minConfidence=0.3")

fpgrowth = FPGrowth(
    itemsCol="items",
    minSupport=0.01,        # 1% — điều chỉnh lên nếu hết RAM
    minConfidence=0.3,      # P(consequent | antecedent) ≥ 30%
    numPartitions=50
)

fp_model = fpgrowth.fit(baskets)
print("  ✓ FP-Growth model trained")

# %%
# ============================================================
#  CELL 11 — Explore Frequent Itemsets và Association Rules
# ============================================================
print("\n--- Frequent Itemsets (top 20 by frequency) ---")
freq_itemsets = fp_model.freqItemsets
print(f"  Total frequent itemsets found: {freq_itemsets.count():,}")

# Join với product names
freq_named = freq_itemsets \
    .filter(F.size("items") == 2) \
    .withColumn("p1", F.col("items")[0].cast("int")) \
    .withColumn("p2", F.col("items")[1].cast("int")) \
    .join(products.withColumnRenamed("product_id","p1").withColumnRenamed("product_name","name1"), on="p1") \
    .join(products.withColumnRenamed("product_id","p2").withColumnRenamed("product_name","name2"), on="p2") \
    .select("name1", "name2", "freq") \
    .orderBy(F.desc("freq"))

print("\nTop 20 most-bought-together pairs:")
freq_named.show(20, truncate=35)

print("\n--- Association Rules ---")
rules = fp_model.associationRules
print(f"  Total rules found: {rules.count():,}")

# Hiển thị rules với product names
rules_named = rules \
    .filter(F.size("antecedent") == 1) \
    .filter(F.size("consequent") == 1) \
    .withColumn("ant_id", F.col("antecedent")[0].cast("int")) \
    .withColumn("con_id", F.col("consequent")[0].cast("int")) \
    .join(products.withColumnRenamed("product_id","ant_id").withColumnRenamed("product_name","if_buy"), on="ant_id") \
    .join(products.withColumnRenamed("product_id","con_id").withColumnRenamed("product_name","then_buy"), on="con_id") \
    .select(
        "if_buy",
        "then_buy",
        F.round("confidence", 3).alias("confidence"),
        F.round("lift", 3).alias("lift"),
        F.round("support", 4).alias("support")
    ) \
    .orderBy(F.desc("lift"))

print("\nTop 20 rules by lift (strongest associations):")
rules_named.show(20, truncate=35)

print("\nTop 20 rules by confidence (most reliable):")
rules_named.orderBy(F.desc("confidence")).show(20, truncate=35)

# %%
# ============================================================
#  CELL 12 — Export kết quả và lưu model
# ============================================================
print("\nExporting results...")

# Export association rules CSV (cho web dashboard)
export_dir = "/home/jovyan/work/exports"

rules_named.orderBy(F.desc("lift")).limit(500) \
    .toPandas().to_csv(f"{export_dir}/association_rules.csv", index=False)
print(f"  ✓ association_rules.csv (top 500 rules by lift)")

# Export top co-purchased pairs
freq_named.limit(500) \
    .toPandas().to_csv(f"{export_dir}/top_pairs.csv", index=False)
print(f"  ✓ top_pairs.csv (top 500 pairs)")

# Save FP-Growth model
fp_model.write().overwrite().save(f"{MODELS}/fpgrowth_model")
print(f"  ✓ FP-Growth model saved: {MODELS}/fpgrowth_model")

# Save frequent itemsets to HDFS (cho API)
freq_itemsets.write.parquet(f"{FEAT}/frequent_itemsets.parquet", mode="overwrite")
rules.write.parquet(f"{FEAT}/association_rules.parquet", mode="overwrite")
print(f"  ✓ frequent_itemsets.parquet saved")
print(f"  ✓ association_rules.parquet saved")

# %%
# ============================================================
#  CELL 13 — Tóm tắt
# ============================================================
print("\n" + "═" * 60)
print("  NOTEBOOK 3 — ALS + FP-Growth COMPLETE ✓")
print("═" * 60)
print(f"  ALS Model RMSE      : {rmse:.4f}")
print(f"  ALS Precision@10    : {avg_precision['mean_precision']:.4f}")
print(f"  Frequent itemsets   : {freq_itemsets.count():,}")
print(f"  Association rules   : {rules.count():,}")
print("─" * 60)
print("  Files saved to HDFS:")
print(f"    {MODELS}/als_model")
print(f"    {MODELS}/fpgrowth_model")
print(f"    {FEAT}/als_recommendations.parquet")
print(f"    {FEAT}/frequent_itemsets.parquet")
print(f"    {FEAT}/association_rules.parquet")
print("  Files exported (CSV):")
print(f"    exports/association_rules.csv")
print(f"    exports/top_pairs.csv")
print("─" * 60)
print("  Next → src/04_streaming/submit_streaming.sh")
print("═" * 60)

spark.stop()
print("\nSparkSession stopped.")
