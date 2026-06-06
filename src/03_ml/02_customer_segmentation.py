# ============================================================
#  02_customer_segmentation.py
#  Instacart — KMeans Customer Segmentation (RFV)
#  PySpark MLlib 4.1.1
#
#  Input  : hdfs://.../instacart/features/rfv_features.parquet
#  Output : hdfs://.../instacart/models/kmeans_rfv_model
#           hdfs://.../instacart/features/user_segments.parquet
#           exports/user_segments.csv
#           exports/elbow_curve.png
#           exports/cluster_scatter.png
#           exports/cluster_profiles.csv
# ============================================================

# %% [markdown]
# # Notebook 2 — KMeans Customer Segmentation
# Phân khúc khách hàng theo 3 chiều **RFV**:
# - **R** Recency  — Bao lâu rồi chưa mua (thấp = active)
# - **F** Frequency — Tổng số đơn hàng (cao = loyal)
# - **V** Volume   — Trung bình số sản phẩm/đơn (cao = high spender)
#
# Pipeline: `VectorAssembler → StandardScaler → KMeans`

# %%
# ============================================================
#  CELL 1 — SparkSession
# ============================================================
import os, time
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

HDFS      = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")
SPARK_URL = os.getenv("SPARK_MASTER",  "spark://spark-master:7077")
EXPORT    = "/home/jovyan/work/exports"
os.makedirs(EXPORT, exist_ok=True)

spark = SparkSession.builder \
    .appName("Instacart-CustomerSegmentation") \
    .master(SPARK_URL) \
    .config("spark.hadoop.fs.defaultFS", HDFS) \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(f"Spark {spark.version} ready")

# %%
# ============================================================
#  CELL 2 — Load RFV Features
# ============================================================
FEAT = f"{HDFS}/instacart/features"

rfv = spark.read.parquet(f"{FEAT}/rfv_features.parquet")
rfv.cache()

total_users = rfv.count()
print(f"Total users : {total_users:,}")
print(f"Columns     : {rfv.columns}")
print("\nSchema:")
rfv.printSchema()

# %%
# ============================================================
#  CELL 3 — EDA: Descriptive Statistics + Null Check
# ============================================================

# Stats for RFV dimensions
print("=== Descriptive Statistics ===")
rfv.select("recency", "frequency", "volume",
           "u_reorder_rate", "u_organic_ratio").describe().show()

# Null counts
print("=== Null Counts ===")
rfv.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in rfv.columns
]).show()

# Fill remaining nulls
rfv = rfv.fillna({
    "recency"        : 0.0,
    "frequency"      : 1.0,
    "volume"         : 1.0,
    "u_reorder_rate" : 0.0,
    "u_organic_ratio": 0.0,
})

# Distribution overview
print("=== Recency distribution (days) ===")
rfv.select(
    F.percentile_approx("recency",   [0.25, 0.5, 0.75, 0.9]).alias("recency_pct"),
    F.percentile_approx("frequency", [0.25, 0.5, 0.75, 0.9]).alias("frequency_pct"),
    F.percentile_approx("volume",    [0.25, 0.5, 0.75, 0.9]).alias("volume_pct")
).show(truncate=False)

# %%
# ============================================================
#  CELL 4 — Build & Fit Preprocessing Pipeline (once)
#  We fit scaler ONCE outside the k-loop to save time.
# ============================================================

# 5 features for clustering (RFV + 2 behavioral extras)
FEATURE_COLS = ["recency", "frequency", "volume",
                "u_reorder_rate", "u_organic_ratio"]

assembler = VectorAssembler(
    inputCols=FEATURE_COLS,
    outputCol="raw_features",
    handleInvalid="skip"
)

scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withMean=True,
    withStd=True
)

# Fit scaler once
assembled_df  = assembler.transform(rfv)
scaler_model  = scaler.fit(assembled_df)
scaled_df     = scaler_model.transform(assembled_df)

# Keep user_id + raw features + scaled features in cache
scaled_df = scaled_df.select(
    "user_id", "recency", "frequency", "volume",
    "u_reorder_rate", "u_organic_ratio", "features"
).cache()
print(f"Scaled dataset cached: {scaled_df.count():,} rows")

# %%
# ============================================================
#  CELL 5 — Elbow Curve + Silhouette (k = 2 to 8)
# ============================================================
print("=" * 55)
print("  Elbow + Silhouette Analysis (k = 2 to 8)")
print("  Note: each k takes ~1-3 minutes on the cluster")
print("=" * 55)

sil_evaluator = ClusteringEvaluator(
    featuresCol="features",
    predictionCol="cluster",
    metricName="silhouette",
    distanceMeasure="squaredEuclidean"
)

k_range    = range(2, 9)
wssse_list = []
sil_list   = []

for k in k_range:
    t0 = time.time()
    km = KMeans(
        featuresCol="features",
        predictionCol="cluster",
        k=k,
        maxIter=30,
        tol=1e-4,
        seed=42
    )
    km_model = km.fit(scaled_df)

    # WSSSE (Within Set Sum of Squared Errors) — elbow metric
    wssse = km_model.summary.trainingCost
    wssse_list.append(wssse)

    # Silhouette score — higher is better, max = 1.0
    preds = km_model.transform(scaled_df)
    sil   = sil_evaluator.evaluate(preds)
    sil_list.append(sil)

    elapsed = time.time() - t0
    print(f"  k={k}  WSSSE={wssse:>15,.0f}  Silhouette={sil:.4f}  [{elapsed:.1f}s]")

# ── Plot elbow + silhouette ───────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(13, 5))

# Elbow
axes[0].plot(list(k_range), wssse_list, marker="o", color="#2563EB", linewidth=2)
axes[0].set_title("Elbow Curve (WSSSE)", fontsize=13)
axes[0].set_xlabel("Number of Clusters (k)")
axes[0].set_ylabel("Within Set Sum of Squared Errors")
axes[0].grid(alpha=0.3)
for k, w in zip(k_range, wssse_list):
    axes[0].annotate(f"k={k}", (k, w), textcoords="offset points",
                     xytext=(0, 8), ha="center", fontsize=9)

# Silhouette
axes[1].plot(list(k_range), sil_list, marker="s", color="#16A34A", linewidth=2)
axes[1].set_title("Silhouette Scores", fontsize=13)
axes[1].set_xlabel("Number of Clusters (k)")
axes[1].set_ylabel("Silhouette Score (higher is better)")
axes[1].grid(alpha=0.3)
for k, s in zip(k_range, sil_list):
    axes[1].annotate(f"{s:.3f}", (k, s), textcoords="offset points",
                     xytext=(0, 8), ha="center", fontsize=9)

plt.suptitle("KMeans Cluster Selection — Instacart RFV", fontsize=14, y=1.01)
plt.tight_layout()
plt.savefig(f"{EXPORT}/elbow_curve.png", dpi=150, bbox_inches="tight")
plt.close()

print(f"\n✓ Elbow curve saved: {EXPORT}/elbow_curve.png")

# Print selection guide
best_sil_k = list(k_range)[sil_list.index(max(sil_list))]
print(f"\nBest silhouette at k={best_sil_k} (score={max(sil_list):.4f})")
print("→ Using k=4 for business interpretability (4 clear customer tiers)")

# %%
# ============================================================
#  CELL 6 — Train Final Model with k=4
# ============================================================
K_FINAL = 4

km_final = KMeans(
    featuresCol="features",
    predictionCol="cluster",
    k=K_FINAL,
    maxIter=50,
    tol=1e-6,
    seed=42
)

t0             = time.time()
km_model_final = km_final.fit(scaled_df)
print(f"Final KMeans (k={K_FINAL}) trained in {time.time()-t0:.1f}s")

# Add cluster labels to all users
segmented = km_model_final.transform(scaled_df)
segmented.cache()
segmented.count()

# Cluster sizes
print("\nCluster size distribution:")
segmented.groupBy("cluster").count() \
    .withColumn("pct", F.round(F.col("count") * 100.0 / total_users, 2)) \
    .orderBy("cluster").show()

# Save model
model_path = f"{HDFS}/instacart/models/kmeans_rfv_model"
km_model_final.write().overwrite().save(model_path)
print(f"✓ KMeans model saved: {model_path}")

# %%
# ============================================================
#  CELL 7 — Cluster Analysis: Mean Feature Values per Cluster
# ============================================================

cluster_profiles = segmented.groupBy("cluster").agg(
    F.count("*").alias("user_count"),
    F.round(F.avg("recency"),        2).alias("avg_recency"),
    F.round(F.avg("frequency"),      2).alias("avg_frequency"),
    F.round(F.avg("volume"),         2).alias("avg_basket_size"),
    F.round(F.avg("u_reorder_rate"), 4).alias("avg_reorder_rate"),
    F.round(F.avg("u_organic_ratio"),4).alias("avg_organic_ratio"),
    F.round(F.min("recency"),        1).alias("min_recency"),
    F.round(F.max("frequency"),      0).alias("max_frequency"),
).orderBy("cluster")

print("=== Cluster Profiles ===")
cluster_profiles.show(truncate=False)

# Export cluster profiles CSV
cluster_profiles.toPandas().to_csv(
    f"{EXPORT}/cluster_profiles.csv", index=False
)
print("✓ cluster_profiles.csv saved")

# %%
# ============================================================
#  CELL 8 — Cluster Naming Based on Characteristics
#
#  Naming logic (auto-assigned by reading cluster centers):
#    High freq + Low recency  → Champion
#    Low freq + High recency → At Risk / Dormant
#    Med freq + Med recency  → Regular Buyer
#    Low freq + Low recency  → New / Occasional
# ============================================================

# Collect profiles to driver for naming
profiles_pd = cluster_profiles.toPandas().set_index("cluster")
profiles_pd = profiles_pd.sort_values("avg_frequency", ascending=False)

print("Cluster summary for naming:")
print(profiles_pd[["user_count", "avg_recency", "avg_frequency",
                    "avg_basket_size", "avg_reorder_rate"]].to_string())

# Auto-name: assign based on rank of frequency + recency
sorted_by_freq     = profiles_pd["avg_frequency"].rank(ascending=False)
sorted_by_recency  = profiles_pd["avg_recency"].rank(ascending=True)   # low recency = active

cluster_name_map = {}
for c_idx, row in profiles_pd.iterrows():
    freq_rank    = sorted_by_freq[c_idx]
    recency_rank = sorted_by_recency[c_idx]
    if freq_rank == 1:
        cluster_name_map[c_idx] = "🏆 Champion"
    elif freq_rank == 2:
        cluster_name_map[c_idx] = "💛 Loyal Regular"
    elif recency_rank in (3, 4):
        cluster_name_map[c_idx] = "⚠️  At Risk / Dormant"
    else:
        cluster_name_map[c_idx] = "🆕 New / Occasional"

print("\nCluster names assigned:")
for k, v in sorted(cluster_name_map.items()):
    print(f"  Cluster {k} → {v}")

# Add name column via UDF
name_udf = F.udf(lambda c: cluster_name_map.get(c, f"Cluster {c}"))
segmented = segmented.withColumn("segment_name", name_udf(F.col("cluster")))

# Final distribution with names
print("\nFinal segmentation:")
segmented.groupBy("cluster", "segment_name").count() \
    .withColumn("pct", F.round(F.col("count") * 100.0 / total_users, 2)) \
    .orderBy("cluster").show(truncate=False)

# %%
# ============================================================
#  CELL 9 — Scatter Plot (Frequency vs Recency, colored by cluster)
# ============================================================

# Sample for visualization (5000 points max to keep plot readable)
sample_pd = segmented.select(
    "recency", "frequency", "volume", "cluster", "segment_name"
).sample(fraction=min(5000.0 / total_users, 1.0), seed=42).toPandas()

cluster_colors = ["#2563EB", "#16A34A", "#DC2626", "#F59E0B"]
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# Plot 1: Frequency vs Recency
for i, (cname, grp) in enumerate(sample_pd.groupby("cluster")):
    seg_label = cluster_name_map.get(cname, f"Cluster {cname}")
    axes[0].scatter(grp["recency"], grp["frequency"],
                    c=cluster_colors[cname % 4],
                    alpha=0.5, s=20, label=seg_label)

axes[0].set_xlabel("Recency (days since last order)", fontsize=11)
axes[0].set_ylabel("Frequency (total orders)", fontsize=11)
axes[0].set_title("Customer Segments: Frequency vs Recency", fontsize=12)
axes[0].legend(fontsize=9, loc="upper right")
axes[0].grid(alpha=0.3)

# Plot 2: Frequency vs Basket Size (Volume)
for i, (cname, grp) in enumerate(sample_pd.groupby("cluster")):
    seg_label = cluster_name_map.get(cname, f"Cluster {cname}")
    axes[1].scatter(grp["frequency"], grp["volume"],
                    c=cluster_colors[cname % 4],
                    alpha=0.5, s=20, label=seg_label)

axes[1].set_xlabel("Frequency (total orders)", fontsize=11)
axes[1].set_ylabel("Volume (avg basket size)", fontsize=11)
axes[1].set_title("Customer Segments: Frequency vs Basket Size", fontsize=12)
axes[1].legend(fontsize=9, loc="upper right")
axes[1].grid(alpha=0.3)

plt.suptitle("Instacart Customer Segmentation — KMeans (k=4)", fontsize=14)
plt.tight_layout()
plt.savefig(f"{EXPORT}/cluster_scatter.png", dpi=150, bbox_inches="tight")
plt.close()
print(f"✓ Scatter plot saved: {EXPORT}/cluster_scatter.png")

# %%
# ============================================================
#  CELL 10 — Save to HDFS + Export CSV
# ============================================================

# Save segmented users to HDFS
seg_path = f"{FEAT}/user_segments.parquet"
segmented.select(
    "user_id", "cluster", "segment_name",
    "recency", "frequency", "volume",
    "u_reorder_rate", "u_organic_ratio"
).write.parquet(seg_path, mode="overwrite")
print(f"✓ Saved user_segments.parquet to HDFS: {seg_path}")

# Export CSV for Supabase
csv_df = segmented.select(
    "user_id", "cluster", "segment_name",
    "recency", "frequency", "volume",
    "u_reorder_rate", "u_organic_ratio"
)
csv_df.toPandas().to_csv(f"{EXPORT}/user_segments.csv", index=False)
print(f"✓ Exported user_segments.csv ({csv_df.count():,} rows)")

print(f"""
╔══════════════════════════════════════════════════════════╗
║       CUSTOMER SEGMENTATION — COMPLETE                 ║
╠══════════════════════════════════════════════════════════╣
║  Algorithm   : KMeans (k=4)                            ║
║  Features    : recency, frequency, volume,             ║
║                reorder_rate, organic_ratio             ║
║  Users       : {total_users:>8,}                               ║
╠══════════════════════════════════════════════════════════╣
║  Cluster 0   : (see cluster_profiles.csv)              ║
║  Cluster 1   : 4 segments → Champion, Loyal,           ║
║  Cluster 2   :              At Risk, New/Occasional    ║
║  Cluster 3   :                                         ║
╠══════════════════════════════════════════════════════════╣
║  Model       : HDFS .../models/kmeans_rfv_model        ║
║  Segments    : HDFS .../features/user_segments.parquet ║
║  Charts      : exports/elbow_curve.png                 ║
║                exports/cluster_scatter.png             ║
╚══════════════════════════════════════════════════════════╝
""")

spark.stop()
