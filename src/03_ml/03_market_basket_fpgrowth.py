# ============================================================
#  03_market_basket_fpgrowth.py
#  Instacart — Market Basket Analysis with Spark MLlib FPGrowth
#
#  Input  : hdfs://.../instacart/raw/order_products__prior.csv
#           hdfs://.../instacart/raw/products.csv
#  Output : hdfs://.../instacart/models/basket_fpgrowth_model
#           hdfs://.../instacart/features/basket_freq_itemsets.parquet
#           hdfs://.../instacart/features/basket_association_rules.parquet
#           exports/basket_association_rules.csv
# ============================================================

# %% [markdown]
# # Notebook 3 — Market Basket Analysis with MLlib FPGrowth
# Bản MLlib tương đương notebook `Market Basket Analysis.ipynb`.
#
# Mục tiêu:
# - Gom mỗi `order_id` thành một basket sản phẩm.
# - Huấn luyện `pyspark.ml.fpm.FPGrowth`.
# - Xuất frequent itemsets và association rules, kèm tên sản phẩm để đọc kết quả.

# %%
# ============================================================
#  CELL 1 — SparkSession
# ============================================================
import os
from typing import List

from pyspark.ml.fpm import FPGrowth
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")
SPARK_URL = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
EXPORT = os.getenv("EXPORT_DIR", "/home/jovyan/work/exports")
os.makedirs(EXPORT, exist_ok=True)

spark = (
    SparkSession.builder.appName("Instacart-MarketBasket-FPGrowth")
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
#  CELL 2 — Load raw tables
# ============================================================
RAW = f"{HDFS}/instacart/raw"
FEAT = f"{HDFS}/instacart/features"
MODEL = f"{HDFS}/instacart/models"

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


prior = read_instacart_csv(f"{RAW}/order_products__prior.csv")
products = read_products_csv(f"{RAW}/products.csv")

print(f"prior rows    : {prior.count():,}")
print(f"products rows : {products.count():,}")
prior.printSchema()

# %%
# ============================================================
#  CELL 3 — Build baskets
# ============================================================
# FPGrowth expects array<string> or array<numeric>. String IDs are safer for
# joining/displaying and avoid treating product_id as a continuous variable.
baskets = (
    prior.groupBy("order_id")
    .agg(F.sort_array(F.collect_set(F.col("product_id").cast("string"))).alias("items"))
    .filter(F.size("items") >= 2)
    .cache()
)

basket_count = baskets.count()
print(f"Basket count (2+ items): {basket_count:,}")
baskets.select("order_id", "items").show(5, truncate=False)

baskets.write.parquet(f"{FEAT}/baskets.parquet", mode="overwrite")
print(f"Saved baskets: {FEAT}/baskets.parquet")

# %%
# ============================================================
#  CELL 4 — Fit FPGrowth
# ============================================================
# Full Instacart has many products. Start with conservative support to keep
# runtime and rule count manageable, then lower it if you need more long-tail rules.
MIN_SUPPORT = float(os.getenv("FP_MIN_SUPPORT", "0.003"))
MIN_CONFIDENCE = float(os.getenv("FP_MIN_CONFIDENCE", "0.2"))

fp = FPGrowth(
    itemsCol="items",
    minSupport=MIN_SUPPORT,
    minConfidence=MIN_CONFIDENCE,
)

model = fp.fit(baskets)
model.write().overwrite().save(f"{MODEL}/basket_fpgrowth_model")
print(f"Saved model: {MODEL}/basket_fpgrowth_model")

# %%
# ============================================================
#  CELL 5 — Frequent itemsets
# ============================================================
freq_itemsets = model.freqItemsets.orderBy(F.desc("freq")).cache()
freq_count = freq_itemsets.count()

print(f"Frequent itemsets: {freq_count:,}")
freq_itemsets.show(20, truncate=False)

freq_itemsets.write.parquet(
    f"{FEAT}/basket_freq_itemsets.parquet", mode="overwrite"
)
print(f"Saved frequent itemsets: {FEAT}/basket_freq_itemsets.parquet")

# %%
# ============================================================
#  CELL 6 — Association rules with product names
# ============================================================
product_map = {
    str(row["product_id"]): row["product_name"]
    for row in products.select("product_id", "product_name").collect()
}


def product_names(product_ids: List[str]) -> List[str]:
    return [product_map.get(str(product_id), str(product_id)) for product_id in product_ids]


names_udf = F.udf(product_names, "array<string>")

rules_named = (
    model.associationRules
    .withColumn("antecedent_names", names_udf("antecedent"))
    .withColumn("consequent_names", names_udf("consequent"))
    .orderBy(F.desc("lift"), F.desc("confidence"))
    .cache()
)

rule_count = rules_named.count()
print(f"Association rules: {rule_count:,}")
rules_named.select(
    "antecedent_names",
    "consequent_names",
    F.round("confidence", 4).alias("confidence"),
    F.round("lift", 4).alias("lift"),
    F.round("support", 5).alias("support"),
).show(30, truncate=False)

rules_named.write.parquet(
    f"{FEAT}/basket_association_rules.parquet", mode="overwrite"
)
print(f"Saved rules: {FEAT}/basket_association_rules.parquet")

# %%
# ============================================================
#  CELL 7 — Export readable CSV
# ============================================================
top_rules = rules_named.select(
    F.concat_ws(" + ", "antecedent_names").alias("antecedent"),
    F.concat_ws(" + ", "consequent_names").alias("consequent"),
    F.round("confidence", 6).alias("confidence"),
    F.round("lift", 6).alias("lift"),
    F.round("support", 6).alias("support"),
).limit(500)

csv_path = f"{EXPORT}/basket_association_rules.csv"
top_rules.toPandas().to_csv(csv_path, index=False)
print(f"Saved CSV: {csv_path}")

print(
    f"""
FPGrowth complete
- baskets          : {basket_count:,}
- minSupport       : {MIN_SUPPORT}
- minConfidence    : {MIN_CONFIDENCE}
- frequent itemsets: {freq_count:,}
- rules            : {rule_count:,}
"""
)

spark.stop()
