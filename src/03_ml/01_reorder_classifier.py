# ============================================================
#  01_reorder_classifier.py
#  Instacart — Binary Reorder Prediction (GBT Classifier)
#  PySpark MLlib 4.1.1
#
#  Input  : hdfs://.../instacart/features/train_dataset.parquet
#  Output : hdfs://.../instacart/models/reorder_gbt_model
#           hdfs://.../instacart/features/reorder_predictions.parquet
#           exports/reorder_predictions.csv   (top-20 per user)
#           exports/feature_importance.png
# ============================================================

# %% [markdown]
# # Notebook 1 — GBT Reorder Classifier
# Bài toán: **Binary Classification** — dự đoán cặp (user, product) nào
# sẽ xuất hiện trong đơn hàng tiếp theo (reordered = 1 hay 0).
#
# Pipeline: `StringIndexer → OneHotEncoder → VectorAssembler → StandardScaler → GBTClassifier`

# %%
# ============================================================
#  CELL 1 — SparkSession
# ============================================================
import os
import time
import matplotlib
matplotlib.use('Agg')   # non-interactive backend (Docker/server)
import matplotlib.pyplot as plt
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import (VectorAssembler, StandardScaler,
                                 StringIndexer, OneHotEncoder)
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier
from pyspark.ml.evaluation import (BinaryClassificationEvaluator,
                                   MulticlassClassificationEvaluator)

HDFS     = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")
SPARK_URL = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
EXPORT   = "/home/jovyan/work/exports"
os.makedirs(EXPORT, exist_ok=True)

spark = SparkSession.builder \
    .appName("Instacart-ReorderClassifier") \
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

# %% [markdown]
# ## CELL 2 — Load Training Dataset

# %%
# ============================================================
#  CELL 2 — Load data + check label distribution
# ============================================================
FEAT = f"{HDFS}/instacart/features"

data = spark.read.parquet(f"{FEAT}/train_dataset.parquet")
data.cache()

total = data.count()
pos   = data.filter(F.col("reordered") == 1).count()
neg   = data.filter(F.col("reordered") == 0).count()

print(f"Total rows     : {total:>12,}")
print(f"Positive (=1)  : {pos:>12,}  ({pos/total*100:.1f}%)")
print(f"Negative (=0)  : {neg:>12,}  ({neg/total*100:.1f}%)")
print(f"Imbalance ratio: {neg/pos:.2f}:1")
print("\nSchema:")
data.printSchema()
print("\nDescriptive stats (numeric cols):")
data.describe().show()

# %% [markdown]
# ## CELL 3 — Handle Missing Values

# %%
# ============================================================
#  CELL 3 — Null audit and imputation
# ============================================================

# Null count per column
null_counts = data.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in data.columns
])
print("Null counts per column:")
null_counts.show(vertical=True)

# Impute nulls with sensible defaults
data = data.fillna({
    "u_avg_days_since_prior"   : 0.0,
    "u_std_days_since_prior"   : 0.0,
    "u_preferred_dow"          : 0,
    "u_preferred_hour"         : 10,
    "up_avg_position"          : 5.0,
    "p_avg_add_to_cart_order"  : 5.0,
    "p_reorder_rate"           : 0.0,
})

# Cast label to DoubleType (required by GBTClassifier)
data = data.withColumn("label", F.col("reordered").cast(DoubleType()))

print("Null imputation complete. Label column cast to DoubleType.")

# %% [markdown]
# ## CELL 4 — Train / Test Split by USER (no data leakage)

# %%
# ============================================================
#  CELL 4 — User-based split (NOT random row split)
#  Rationale: splitting by user ensures the model has never
#  seen any transaction from a test user during training.
#  Random row split would leak a user's history into training.
# ============================================================

users = data.select("user_id").distinct()
train_users, test_users = users.randomSplit([0.8, 0.2], seed=42)

train_df = data.join(train_users, on="user_id", how="inner")
test_df  = data.join(test_users,  on="user_id", how="inner")

# Cache for repeated use in pipeline fitting + evaluation
train_df.cache()
test_df.cache()

print(f"Train rows  : {train_df.count():>10,}  | Users: {train_users.count():>8,}")
print(f"Test  rows  : {test_df.count():>10,}  | Users: {test_users.count():>8,}")
print(f"Train pos % : {train_df.filter('label=1').count()/train_df.count()*100:.2f}%")
print(f"Test  pos % : {test_df.filter('label=1').count()/test_df.count()*100:.2f}%")

# %% [markdown]
# ## CELL 5 — Build MLlib Pipeline

# %%
# ============================================================
#  CELL 5 — MLlib Pipeline
#  StringIndexer → OneHotEncoder → VectorAssembler
#  → StandardScaler → GBTClassifier
# ============================================================

# ── Numerical features ────────────────────────────────────
# Ordered: UP features (most predictive), then USER, then PRODUCT
numerical_features = [
    # User-Product interactions (strongest signal)
    "up_order_count",
    "up_reorder_rate",
    "up_avg_position",
    "up_first_order_number",
    "up_last_order_number",
    "up_orders_since_last",
    "up_order_rate_since_first",
    # User-level features
    "u_total_orders",
    "u_avg_basket_size",
    "u_reorder_rate",
    "u_avg_days_since_prior",
    "u_std_days_since_prior",
    "u_organic_ratio",
    "u_preferred_dow",
    "u_preferred_hour",
    # Product-level features
    "p_total_orders",
    "p_reorder_rate",
    "p_unique_users",
    "p_avg_add_to_cart_order",
    "p_is_organic",
    "p_aisle_id",
]

# ── Categorical: department (21 categories) ───────────────
dept_indexer = StringIndexer(
    inputCol="p_department_id",
    outputCol="dept_idx",
    handleInvalid="keep"
)
dept_encoder = OneHotEncoder(
    inputCol="dept_idx",
    outputCol="dept_vec",
    dropLast=True     # 21 depts → 20 binary columns
)

# ── Assembler: combine all features into one vector ───────
assembler = VectorAssembler(
    inputCols=numerical_features + ["dept_vec"],
    outputCol="raw_features",
    handleInvalid="skip"
)

# ── Scaler: normalize to zero-mean, unit-variance ─────────
scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withMean=True,
    withStd=True
)

# ── GBT Classifier ────────────────────────────────────────
# maxIter=50: enough trees for good accuracy
# maxDepth=5: balance between model complexity and overfitting
# Nếu quá chậm (>30 phút), giảm xuống maxIter=30, maxDepth=4
gbt = GBTClassifier(
    featuresCol="features",
    labelCol="label",
    predictionCol="prediction",
    probabilityCol="probability",
    maxIter=50,
    maxDepth=5,
    stepSize=0.1,
    subSamplingRate=0.8,   # row subsampling per tree
    featureSubsetStrategy="sqrt",
    seed=42
)

# ── Assemble full pipeline ─────────────────────────────────
pipeline = Pipeline(stages=[
    dept_indexer,
    dept_encoder,
    assembler,
    scaler,
    gbt
])

print("Pipeline constructed:")
for i, stage in enumerate(pipeline.getStages()):
    print(f"  [{i}] {stage.__class__.__name__}")

# %% [markdown]
# ## CELL 6 — Train the Model

# %%
# ============================================================
#  CELL 6 — Fit pipeline on training data
# ============================================================
print("Starting GBTClassifier training...")
print("(This may take 10-30 minutes on 2-worker cluster)")
print("-" * 50)

t_start = time.time()
model = pipeline.fit(train_df)
elapsed = time.time() - t_start

print(f"Training complete! Elapsed: {elapsed/60:.1f} minutes")

# Save model to HDFS
model_path = f"{HDFS}/instacart/models/reorder_gbt_model"
model.write().overwrite().save(model_path)
print(f"Model saved to: {model_path}")

# %% [markdown]
# ## CELL 7 — Evaluate on Test Set

# %%
# ============================================================
#  CELL 7 — Evaluation metrics
#  AUC-ROC, AUC-PR, F1, Accuracy, Precision, Recall
#  + Confusion Matrix
# ============================================================

predictions = model.transform(test_df)

# Extract positive-class probability for Supabase export
predictions = predictions.withColumn(
    "reorder_probability",
    F.round(F.element_at(F.col("probability").cast("array<double>"), 2), 4)
)
predictions.cache()

# ── AUC-ROC ──────────────────────────────────────────────
auc_eval = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)
auc_roc = auc_eval.evaluate(predictions)

# ── AUC-PR ───────────────────────────────────────────────
pr_eval = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderPR"
)
auc_pr = pr_eval.evaluate(predictions)

# ── Multiclass metrics ────────────────────────────────────
mc_eval = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction"
)
metrics = {}
for metric in ["f1", "accuracy", "weightedPrecision", "weightedRecall"]:
    metrics[metric] = mc_eval.evaluate(
        predictions, {mc_eval.metricName: metric}
    )

print("=" * 50)
print("  MODEL EVALUATION RESULTS")
print("=" * 50)
print(f"  AUC-ROC           : {auc_roc:.4f}  {'✓ Good' if auc_roc > 0.75 else '⚠ Check features'}")
print(f"  AUC-PR            : {auc_pr:.4f}")
print(f"  F1 Score          : {metrics['f1']:.4f}")
print(f"  Accuracy          : {metrics['accuracy']:.4f}")
print(f"  Weighted Precision: {metrics['weightedPrecision']:.4f}")
print(f"  Weighted Recall   : {metrics['weightedRecall']:.4f}")
print("=" * 50)

# ── Confusion matrix ─────────────────────────────────────
print("\nConfusion Matrix (label × prediction):")
cm = predictions.groupBy("label", "prediction").count() \
    .orderBy("label", "prediction")
cm.show()

# ── Compute precision/recall for positive class manually ─
tp = predictions.filter("label=1.0 AND prediction=1.0").count()
fp = predictions.filter("label=0.0 AND prediction=1.0").count()
fn = predictions.filter("label=1.0 AND prediction=0.0").count()
tn = predictions.filter("label=0.0 AND prediction=0.0").count()

precision_pos = tp / (tp + fp) if (tp + fp) > 0 else 0
recall_pos    = tp / (tp + fn) if (tp + fn) > 0 else 0
f1_pos        = 2 * precision_pos * recall_pos / (precision_pos + recall_pos) \
                if (precision_pos + recall_pos) > 0 else 0

print(f"\nPositive class (reordered=1) metrics:")
print(f"  TP={tp:,}  FP={fp:,}  FN={fn:,}  TN={tn:,}")
print(f"  Precision : {precision_pos:.4f}")
print(f"  Recall    : {recall_pos:.4f}")
print(f"  F1        : {f1_pos:.4f}")

# %% [markdown]
# ## CELL 8 — Feature Importance

# %%
# ============================================================
#  CELL 8 — Extract and visualize GBT feature importances
# ============================================================

gbt_model   = model.stages[-1]  # GBT is the last stage
importances = gbt_model.featureImportances.toArray()

# Map indices to feature names
# Order: numerical_features first, then dept_vec (OHE columns)
n_num  = len(numerical_features)
n_dept = len(importances) - n_num   # remaining = dept one-hot dims

feature_names = (
    numerical_features
    + [f"dept_ohe_{i}" for i in range(n_dept)]
)

# Sort by importance (descending)
feat_imp_pairs = sorted(
    zip(feature_names, importances),
    key=lambda x: x[1],
    reverse=True
)

print("Top 15 Feature Importances (GBT):")
print("-" * 45)
for name, imp in feat_imp_pairs[:15]:
    bar = "█" * int(imp * 200)
    print(f"  {name:<35} {imp:.4f}  {bar}")

# ── Matplotlib bar chart ──────────────────────────────────
top_n = 18
names  = [x[0] for x in feat_imp_pairs[:top_n]]
values = [x[1] for x in feat_imp_pairs[:top_n]]

fig, ax = plt.subplots(figsize=(10, 7))
colors  = ["#2563EB" if "up_" in n else
           "#16A34A" if "u_"  in n else
           "#DC2626"
           for n in names]
bars = ax.barh(names[::-1], values[::-1], color=colors[::-1], edgecolor="white")
ax.set_xlabel("Feature Importance (GBT)", fontsize=12)
ax.set_title("Top 18 Feature Importances — Reorder Classifier\n"
             "Blue=UP interaction  |  Green=User  |  Red=Product", fontsize=12)
ax.axvline(x=0, color="black", linewidth=0.5)
for bar, val in zip(bars, values[::-1]):
    ax.text(val + 0.001, bar.get_y() + bar.get_height()/2,
            f"{val:.4f}", va="center", fontsize=8)

plt.tight_layout()
chart_path = f"{EXPORT}/feature_importance.png"
plt.savefig(chart_path, dpi=150, bbox_inches="tight")
plt.close()
print(f"\n✓ Feature importance chart saved: {chart_path}")

# %% [markdown]
# ## CELL 9 — Save Predictions to HDFS

# %%
# ============================================================
#  CELL 9 — Save full predictions to HDFS
#  (Streaming notebook (Người 4) reads reorder_probability from here)
# ============================================================

# Select only the columns needed downstream
pred_slim = predictions.select(
    "user_id",
    "product_id",
    "reordered",          # actual label
    "prediction",         # model prediction (0.0 or 1.0)
    "reorder_probability" # positive-class probability
)

pred_path = f"{FEAT}/reorder_predictions.parquet"
pred_slim.write.parquet(pred_path, mode="overwrite")
print(f"✓ Full predictions saved to HDFS: {pred_path}")

# Distribution of predicted probabilities
print("\nProbability distribution:")
pred_slim.select(
    F.round("reorder_probability", 1).alias("prob_bin")
).groupBy("prob_bin").count() \
 .orderBy("prob_bin").show()

# %% [markdown]
# ## CELL 10 — Export CSV for Supabase

# %%
# ============================================================
#  CELL 10 — Export top-20 reorder predictions per user to CSV
#  Full dataset is too large (~millions of rows) for CSV.
#  Export only high-confidence positives: probability > 0.5
# ============================================================
from pyspark.sql.window import Window

w_prob = Window.partitionBy("user_id").orderBy(F.desc("reorder_probability"))

top_preds = pred_slim \
    .filter(F.col("reorder_probability") > 0.5) \
    .withColumn("rank", F.row_number().over(w_prob)) \
    .filter(F.col("rank") <= 20) \
    .drop("rank")

# Join product names for readability
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


products = read_products_csv(f"{HDFS}/instacart/raw/products.csv")
departments = read_instacart_csv(f"{HDFS}/instacart/raw/departments.csv")
products_full = products.join(departments, "department_id")

top_preds_named = top_preds.join(
    products_full.select("product_id", "product_name", "department"),
    on="product_id",
    how="left"
).orderBy("user_id", F.desc("reorder_probability"))

csv_path = f"{EXPORT}/reorder_predictions.csv"
top_preds_named.toPandas().to_csv(csv_path, index=False)
count = top_preds_named.count()

print(f"✓ Saved reorder_predictions.csv — {count:,} rows (top-20 per user, prob > 0.5)")
print(f"  Path: {csv_path}")

# ── Final summary ─────────────────────────────────────────
print(f"""
╔══════════════════════════════════════════════════════════╗
║        GBT REORDER CLASSIFIER — COMPLETE               ║
╠══════════════════════════════════════════════════════════╣
║  AUC-ROC  : {auc_roc:.4f}                                    ║
║  AUC-PR   : {auc_pr:.4f}                                    ║
║  F1 Score : {metrics['f1']:.4f}                                    ║
║  Accuracy : {metrics['accuracy']:.4f}                                    ║
╠══════════════════════════════════════════════════════════╣
║  Model saved  : HDFS .../models/reorder_gbt_model       ║
║  Predictions  : HDFS .../features/reorder_predictions   ║
║  Chart        : exports/feature_importance.png          ║
║  CSV export   : exports/reorder_predictions.csv         ║
╚══════════════════════════════════════════════════════════╝
""")

spark.stop()
