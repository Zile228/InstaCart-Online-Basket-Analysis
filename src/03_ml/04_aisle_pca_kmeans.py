# ============================================================
#  04_aisle_pca_kmeans.py
#  Instacart — Aisle Preference Segmentation with Spark MLlib
#
#  Input  : hdfs://.../instacart/raw/orders.csv
#           hdfs://.../instacart/raw/order_products__prior.csv
#           hdfs://.../instacart/raw/products.csv
#           hdfs://.../instacart/raw/aisles.csv
#  Output : hdfs://.../instacart/models/aisle_pca_kmeans_model
#           hdfs://.../instacart/features/user_aisle_segments.parquet
#           exports/aisle_cluster_profiles.csv
# ============================================================

# %% [markdown]
# # Notebook 4 — Customer Segmentation by Aisle Preference with MLlib
# Bản MLlib tương đương notebook `Customers Segmentation.ipynb`.
#
# Notebook gốc tạo ma trận user x aisle, giảm chiều bằng PCA, sau đó chạy KMeans.
# Script này giữ cùng ý tưởng nhưng dùng Spark MLlib để xử lý phân tán.

# %%
# ============================================================
#  CELL 1 — SparkSession
# ============================================================
import os

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import PCA, StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")
SPARK_URL = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
EXPORT = os.getenv("EXPORT_DIR", "/home/jovyan/work/exports")
os.makedirs(EXPORT, exist_ok=True)

spark = (
    SparkSession.builder.appName("Instacart-Aisle-PCA-KMeans")
    .master(SPARK_URL)
    .config("spark.hadoop.fs.defaultFS", HDFS)
    .config("spark.executor.memory", "2g")
    .config("spark.executor.cores", "2")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "50")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print(f"Spark {spark.version} ready")
print(f"HDFS   : {HDFS}")
print(f"EXPORT : {EXPORT}")

# %%
# ============================================================
#  CELL 2 — Load and join transaction data
# ============================================================
RAW = f"{HDFS}/instacart/raw"
FEAT = f"{HDFS}/instacart/features"
MODEL = f"{HDFS}/instacart/models"

orders = spark.read.csv(f"{RAW}/orders.csv", header=True, inferSchema=True)
prior = spark.read.csv(
    f"{RAW}/order_products__prior.csv", header=True, inferSchema=True
)
products = spark.read.csv(f"{RAW}/products.csv", header=True, inferSchema=True)
aisles = spark.read.csv(f"{RAW}/aisles.csv", header=True, inferSchema=True)

orders_prior = orders.filter(F.col("eval_set") == "prior").select(
    "order_id", "user_id"
)

transactions = (
    prior.join(orders_prior, "order_id", "inner")
    .join(products.select("product_id", "aisle_id"), "product_id", "inner")
    .cache()
)

print(f"Transactions: {transactions.count():,}")
transactions.show(5)

# %%
# ============================================================
#  CELL 3 — Build normalized user x aisle matrix
# ============================================================
# Count items per user and aisle, then normalize by user total items so clusters
# represent preference mix, not only order volume.
user_aisle_counts = transactions.groupBy("user_id", "aisle_id").agg(
    F.count("*").alias("aisle_item_count")
)

user_totals = transactions.groupBy("user_id").agg(F.count("*").alias("user_total_items"))

user_aisle_ratio = (
    user_aisle_counts.join(user_totals, "user_id", "inner")
    .withColumn("aisle_ratio", F.col("aisle_item_count") / F.col("user_total_items"))
)

aisle_ids = [
    row["aisle_id"]
    for row in aisles.select("aisle_id").orderBy("aisle_id").collect()
]
aisle_columns = [f"aisle_{aisle_id}" for aisle_id in aisle_ids]

pivoted = user_aisle_ratio.groupBy("user_id").pivot("aisle_id", aisle_ids).agg(
    F.first("aisle_ratio")
)

for aisle_id in aisle_ids:
    raw_name = str(aisle_id)
    pivoted = pivoted.withColumnRenamed(raw_name, f"aisle_{aisle_id}")

user_aisle_matrix = pivoted.fillna(0.0).cache()
print(f"Users: {user_aisle_matrix.count():,}")
print(f"Aisle feature columns: {len(aisle_columns)}")
user_aisle_matrix.select("user_id", *aisle_columns[:8]).show(5)

user_aisle_matrix.write.parquet(
    f"{FEAT}/user_aisle_matrix.parquet", mode="overwrite"
)
print(f"Saved user aisle matrix: {FEAT}/user_aisle_matrix.parquet")

# %%
# ============================================================
#  CELL 4 — MLlib Pipeline: VectorAssembler -> StandardScaler -> PCA -> KMeans
# ============================================================
PCA_K = int(os.getenv("AISLE_PCA_K", "10"))
K_FINAL = int(os.getenv("AISLE_CLUSTER_K", "5"))

assembler = VectorAssembler(
    inputCols=aisle_columns,
    outputCol="raw_features",
    handleInvalid="keep",
)
scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="scaled_features",
    withMean=True,
    withStd=True,
)
pca = PCA(k=PCA_K, inputCol="scaled_features", outputCol="pca_features")
kmeans = KMeans(
    k=K_FINAL,
    featuresCol="pca_features",
    predictionCol="cluster",
    maxIter=50,
    seed=42,
)

pipeline = Pipeline(stages=[assembler, scaler, pca, kmeans])
model = pipeline.fit(user_aisle_matrix)
segments = model.transform(user_aisle_matrix).cache()

model.write().overwrite().save(f"{MODEL}/aisle_pca_kmeans_model")
print(f"Saved model: {MODEL}/aisle_pca_kmeans_model")

# %%
# ============================================================
#  CELL 5 — Evaluate and inspect explained variance
# ============================================================
evaluator = ClusteringEvaluator(
    featuresCol="pca_features",
    predictionCol="cluster",
    metricName="silhouette",
    distanceMeasure="squaredEuclidean",
)
silhouette = evaluator.evaluate(segments)

pca_model = model.stages[2]
explained = pca_model.explainedVariance.toArray().tolist()
print(f"Silhouette score: {silhouette:.4f}")
print("PCA explained variance:")
for idx, value in enumerate(explained, start=1):
    print(f"  PC{idx:02d}: {value:.4f}")
print(f"Cumulative explained variance: {sum(explained):.4f}")

segments.groupBy("cluster").count().orderBy("cluster").show()

# %%
# ============================================================
#  CELL 6 — Cluster profiles: top aisles per segment
# ============================================================
cluster_means = segments.groupBy("cluster").agg(
    *[F.avg(col).alias(col) for col in aisle_columns]
).cache()

aisle_name_map = {
    int(row["aisle_id"]): row["aisle"]
    for row in aisles.select("aisle_id", "aisle").collect()
}

profile_rows = []
for row in cluster_means.collect():
    cluster_id = int(row["cluster"])
    ranked = sorted(
        (
            (aisle_name_map.get(aisle_id, f"aisle_{aisle_id}"), float(row[f"aisle_{aisle_id}"] or 0.0))
            for aisle_id in aisle_ids
        ),
        key=lambda item: item[1],
        reverse=True,
    )
    for rank, (aisle_name, avg_ratio) in enumerate(ranked[:10], start=1):
        profile_rows.append((cluster_id, rank, aisle_name, avg_ratio))

profiles = spark.createDataFrame(
    profile_rows,
    schema="cluster int, rank int, aisle string, avg_user_basket_ratio double",
).orderBy("cluster", "rank")

profiles.show(60, truncate=False)
profiles.write.parquet(f"{FEAT}/aisle_cluster_profiles.parquet", mode="overwrite")
profiles.toPandas().to_csv(f"{EXPORT}/aisle_cluster_profiles.csv", index=False)
print(f"Saved profiles CSV: {EXPORT}/aisle_cluster_profiles.csv")

# %%
# ============================================================
#  CELL 7 — Save user segments
# ============================================================
output_segments = segments.select("user_id", "cluster", "pca_features")
output_segments.write.parquet(
    f"{FEAT}/user_aisle_segments.parquet", mode="overwrite"
)

print(f"Saved user aisle segments: {FEAT}/user_aisle_segments.parquet")
print(
    f"""
Aisle PCA + KMeans complete
- PCA components : {PCA_K}
- KMeans k       : {K_FINAL}
- Silhouette     : {silhouette:.4f}
"""
)

spark.stop()
