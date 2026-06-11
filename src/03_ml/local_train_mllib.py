#!/usr/bin/env python3
"""
Local Spark MLlib training pipeline for Instacart.

Tasks:
  1. Reorder prediction: binary classification for (user_id, product_id)
  2. Customer segmentation: KMeans over RFV-style user features
  3. Basket association rules: FPGrowth over order baskets

The existing notebooks/scripts in this repo target HDFS. This script is meant
for local development and validation using CSV files in a local data directory.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import time
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    GBTClassifier,
    LogisticRegression,
    RandomForestClassifier,
)
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import BinaryClassificationEvaluator, ClusteringEvaluator
from pyspark.ml.feature import (
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType


REQUIRED_FILES = {
    "orders": "orders.csv",
    "prior": "order_products__prior.csv",
    "train": "order_products__train.csv",
    "products": "products.csv",
    "aisles": "aisles.csv",
    "departments": "departments.csv",
}

NUMERIC_REORDER_FEATURES = [
    "up_order_count",
    "up_reorder_rate",
    "up_avg_position",
    "up_first_order_number",
    "up_last_order_number",
    "up_orders_since_last",
    "up_order_rate_since_first",
    "u_total_orders",
    "u_avg_basket_size",
    "u_reorder_rate",
    "u_avg_days_since_prior",
    "u_std_days_since_prior",
    "u_organic_ratio",
    "u_preferred_dow",
    "u_preferred_hour",
    "p_total_orders",
    "p_reorder_rate",
    "p_unique_users",
    "p_avg_add_to_cart_order",
    "p_is_organic",
    "p_aisle_id",
]

RFV_FEATURES = [
    "recency",
    "frequency",
    "volume",
    "u_reorder_rate",
    "u_organic_ratio",
    "u_distinct_products",
    "u_unique_departments",
    "u_produce_ratio",
    "u_dairy_ratio",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Local feature engineering and Spark MLlib training for Instacart."
    )
    parser.add_argument("--data-dir", default="data", help="Directory containing Instacart CSV files.")
    parser.add_argument(
        "--output-dir",
        default="local_outputs/mllib",
        help="Directory for local features, models, and reports.",
    )
    parser.add_argument("--master", default="local[*]", help="Spark master, default local[*].")
    parser.add_argument(
        "--tasks",
        default="all",
        help="Comma-separated tasks: all,reorder,segmentation,basket",
    )
    parser.add_argument(
        "--models",
        default="lr,rf,gbt",
        help="Comma-separated reorder models to compare: lr,rf,gbt.",
    )
    parser.add_argument(
        "--feature-config",
        default=None,
        help="Optional JSON from sklearn_feature_research.py with selected MLlib features.",
    )
    parser.add_argument(
        "--sample-fraction",
        type=float,
        default=1.0,
        help="Optional user-level sampling fraction for faster local iterations.",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--k-min", type=int, default=2)
    parser.add_argument("--k-max", type=int, default=8)
    parser.add_argument("--min-support", type=float, default=0.003)
    parser.add_argument("--min-confidence", type=float, default=0.2)
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Remove output directory before running.",
    )
    return parser.parse_args()


def create_spark(master: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName("Instacart-Local-MLlib")
        .master(master)
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", "3g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def validate_data_dir(data_dir: Path) -> Dict[str, Path]:
    missing = [
        filename
        for filename in REQUIRED_FILES.values()
        if not (data_dir / filename).exists()
    ]
    if missing:
        missing_list = "\n  - ".join(missing)
        raise FileNotFoundError(
            f"Missing Instacart CSV files in {data_dir}:\n  - {missing_list}\n"
            "Expected Kaggle Instacart files with their original names."
        )
    return {name: data_dir / filename for name, filename in REQUIRED_FILES.items()}


def read_csvs(spark: SparkSession, data_dir: Path, sample_fraction: float, seed: int) -> Dict[str, DataFrame]:
    paths = validate_data_dir(data_dir)
    dfs = {
        name: spark.read.csv(str(path), header=True, inferSchema=True)
        for name, path in paths.items()
    }

    if 0 < sample_fraction < 1:
        sampled_users = (
            dfs["orders"].select("user_id").distinct().sample(False, sample_fraction, seed)
        )
        dfs["orders"] = dfs["orders"].join(sampled_users, "user_id", "inner")
        order_ids = dfs["orders"].select("order_id")
        dfs["prior"] = dfs["prior"].join(order_ids, "order_id", "inner")
        dfs["train"] = dfs["train"].join(order_ids, "order_id", "inner")

    return dfs


def safe_write(df: DataFrame, path: Path, mode: str = "overwrite") -> None:
    df.write.mode(mode).parquet(str(path))


def build_feature_tables(dfs: Dict[str, DataFrame], feature_dir: Path) -> Dict[str, DataFrame]:
    orders = dfs["orders"]
    prior = dfs["prior"]
    train = dfs["train"]
    products = dfs["products"]
    aisles = dfs["aisles"]
    departments = dfs["departments"]

    orders_prior = orders.filter(F.col("eval_set") == "prior")

    products_full = (
        products.join(aisles, on="aisle_id", how="left")
        .join(departments, on="department_id", how="left")
    )

    prior_full = (
        prior.join(
            orders_prior.select(
                "order_id",
                "user_id",
                "order_number",
                "days_since_prior_order",
                "order_dow",
                "order_hour_of_day",
            ),
            on="order_id",
            how="inner",
        )
        .join(
            products_full.select(
                "product_id",
                "product_name",
                "aisle_id",
                "department_id",
                "aisle",
                "department",
            ),
            on="product_id",
            how="inner",
        )
        .fillna({"days_since_prior_order": 0.0})
        .withColumn(
            "is_organic",
            F.when(F.lower(F.col("product_name")).contains("organic"), 1).otherwise(0),
        )
    )
    prior_full.cache()
    prior_full.count()

    user_base = prior_full.groupBy("user_id").agg(
        F.countDistinct("order_id").alias("u_total_orders"),
        F.count("*").alias("u_total_items"),
        F.countDistinct("product_id").alias("u_distinct_products"),
        F.sum(F.col("reordered").cast(DoubleType())).alias("_reorder_sum"),
        F.avg("days_since_prior_order").alias("u_avg_days_since_prior"),
        F.stddev("days_since_prior_order").alias("u_std_days_since_prior"),
        F.sum(F.col("is_organic").cast(DoubleType())).alias("_organic_sum"),
        F.countDistinct("aisle_id").alias("u_unique_aisles"),
        F.countDistinct("department_id").alias("u_unique_departments"),
        F.sum(F.when(F.col("department") == "produce", 1.0).otherwise(0.0)).alias("_produce_sum"),
        F.sum(F.when(F.col("department") == "dairy eggs", 1.0).otherwise(0.0)).alias("_dairy_sum"),
    )

    user_base = (
        user_base.withColumn("u_avg_basket_size", F.col("u_total_items") / F.col("u_total_orders"))
        .withColumn("u_reorder_rate", F.col("_reorder_sum") / F.col("u_total_items"))
        .withColumn("u_organic_ratio", F.col("_organic_sum") / F.col("u_total_items"))
        .withColumn("u_produce_ratio", F.col("_produce_sum") / F.col("u_total_items"))
        .withColumn("u_dairy_ratio", F.col("_dairy_sum") / F.col("u_total_items"))
        .drop("_reorder_sum", "_organic_sum", "_produce_sum", "_dairy_sum")
        .fillna({"u_std_days_since_prior": 0.0})
    )

    dow_counts = prior_full.groupBy("user_id", "order_dow").agg(F.count("*").alias("_cnt"))
    dow_window = Window.partitionBy("user_id").orderBy(F.desc("_cnt"), F.asc("order_dow"))
    preferred_dow = (
        dow_counts.withColumn("_rn", F.row_number().over(dow_window))
        .filter(F.col("_rn") == 1)
        .select("user_id", F.col("order_dow").alias("u_preferred_dow"))
    )

    hour_counts = prior_full.groupBy("user_id", "order_hour_of_day").agg(F.count("*").alias("_cnt"))
    hour_window = Window.partitionBy("user_id").orderBy(F.desc("_cnt"), F.asc("order_hour_of_day"))
    preferred_hour = (
        hour_counts.withColumn("_rn", F.row_number().over(hour_window))
        .filter(F.col("_rn") == 1)
        .select("user_id", F.col("order_hour_of_day").alias("u_preferred_hour"))
    )

    user_features = (
        user_base.join(preferred_dow, "user_id", "left")
        .join(preferred_hour, "user_id", "left")
        .cache()
    )

    product_features = (
        prior_full.groupBy("product_id")
        .agg(
            F.count("*").alias("p_total_orders"),
            F.sum(F.col("reordered").cast(DoubleType())).alias("_reorder_sum"),
            F.countDistinct("user_id").alias("p_unique_users"),
            F.avg("add_to_cart_order").alias("p_avg_add_to_cart_order"),
            F.first("is_organic").alias("p_is_organic"),
            F.first("department_id").alias("p_department_id"),
            F.first("aisle_id").alias("p_aisle_id"),
        )
        .withColumn("p_reorder_rate", F.col("_reorder_sum") / F.col("p_total_orders"))
        .drop("_reorder_sum")
        .cache()
    )

    up_base = prior_full.groupBy("user_id", "product_id").agg(
        F.count("*").alias("up_order_count"),
        F.avg("add_to_cart_order").alias("up_avg_position"),
        F.min("order_number").alias("up_first_order_number"),
        F.max("order_number").alias("up_last_order_number"),
    )

    up_features = (
        up_base.join(user_features.select("user_id", "u_total_orders"), "user_id", "inner")
        .withColumn("up_reorder_rate", F.col("up_order_count") / F.col("u_total_orders"))
        .withColumn("up_orders_since_last", F.col("u_total_orders") - F.col("up_last_order_number"))
        .withColumn(
            "up_order_rate_since_first",
            F.col("up_order_count")
            / (F.col("u_total_orders") - F.col("up_first_order_number") + F.lit(1)),
        )
        .drop("u_total_orders")
        .cache()
    )

    orders_train = orders.filter(F.col("eval_set") == "train")
    train_positive = (
        train.join(orders_train.select("order_id", "user_id"), "order_id", "inner")
        .select("user_id", "product_id", F.col("reordered").cast(IntegerType()))
    )
    train_user_ids = orders_train.select("user_id").distinct()
    candidates = up_features.join(train_user_ids, "user_id", "inner")

    train_dataset = (
        candidates.join(train_positive, ["user_id", "product_id"], "left")
        .fillna({"reordered": 0})
        .join(user_features, "user_id", "left")
        .join(product_features, "product_id", "left")
        .cache()
    )

    w_last = Window.partitionBy("user_id").orderBy(F.desc("order_number"))
    recency_df = (
        orders_prior.withColumn("_rn", F.row_number().over(w_last))
        .filter(F.col("_rn") == 1)
        .select("user_id", F.col("days_since_prior_order").alias("recency"))
        .fillna({"recency": 0.0})
    )
    rfv_features = (
        user_features.select(
            "user_id",
            F.col("u_total_orders").alias("frequency"),
            F.col("u_avg_basket_size").alias("volume"),
            "u_reorder_rate",
            "u_organic_ratio",
            "u_distinct_products",
            "u_unique_departments",
            "u_produce_ratio",
            "u_dairy_ratio",
        )
        .join(recency_df, "user_id", "left")
        .fillna({"recency": 0.0})
        .cache()
    )

    baskets = prior.groupBy("order_id").agg(
        F.sort_array(F.collect_set(F.col("product_id").cast("string"))).alias("items")
    )

    feature_dir.mkdir(parents=True, exist_ok=True)
    feature_tables = {
        "prior_full": prior_full,
        "user_features": user_features,
        "product_features": product_features,
        "user_product_features": up_features,
        "train_dataset": train_dataset,
        "rfv_features": rfv_features,
        "baskets": baskets,
    }
    for name, df in feature_tables.items():
        safe_write(df, feature_dir / name)

    return feature_tables


def add_class_weights(df: DataFrame) -> Tuple[DataFrame, Dict[str, float]]:
    counts = {row["label"]: row["count"] for row in df.groupBy("label").count().collect()}
    pos = float(counts.get(1.0, 0.0))
    neg = float(counts.get(0.0, 0.0))
    total = pos + neg
    if pos == 0 or neg == 0:
        return df.withColumn("class_weight", F.lit(1.0)), {"positive": pos, "negative": neg}
    pos_weight = total / (2.0 * pos)
    neg_weight = total / (2.0 * neg)
    weighted = df.withColumn(
        "class_weight",
        F.when(F.col("label") == 1.0, F.lit(pos_weight)).otherwise(F.lit(neg_weight)),
    )
    return weighted, {"positive": pos, "negative": neg, "pos_weight": pos_weight, "neg_weight": neg_weight}


def split_by_user(df: DataFrame, seed: int) -> Tuple[DataFrame, DataFrame]:
    users = df.select("user_id").distinct().withColumn("_rand", F.rand(seed))
    train_users = users.filter(F.col("_rand") < 0.8).select("user_id")
    test_users = users.filter(F.col("_rand") >= 0.8).select("user_id")

    if test_users.count() == 0:
        ranked = users.withColumn("_rn", F.row_number().over(Window.orderBy("_rand")))
        test_users = ranked.filter(F.col("_rn") == 1).select("user_id")
        train_users = ranked.filter(F.col("_rn") > 1).select("user_id")

    train_df = df.join(train_users, "user_id", "inner")
    test_df = df.join(test_users, "user_id", "inner")
    return train_df, test_df


def build_reorder_pipeline(model_name: str, seed: int, numerical_features: List[str]) -> Pipeline:
    dept_indexer = StringIndexer(
        inputCol="p_department_id",
        outputCol="dept_idx",
        handleInvalid="keep",
    )
    dept_encoder = OneHotEncoder(
        inputCol="dept_idx",
        outputCol="dept_vec",
        dropLast=True,
    )

    if model_name == "lr":
        assembler_output = "raw_features"
        estimator_features = "features"
    else:
        assembler_output = "features"
        estimator_features = "features"

    assembler = VectorAssembler(
        inputCols=numerical_features + ["dept_vec"],
        outputCol=assembler_output,
        handleInvalid="keep",
    )

    stages = [dept_indexer, dept_encoder, assembler]

    if model_name == "lr":
        stages.append(
            StandardScaler(
                inputCol="raw_features",
                outputCol="features",
                withMean=False,
                withStd=True,
            )
        )
        estimator = LogisticRegression(
            featuresCol=estimator_features,
            labelCol="label",
            weightCol="class_weight",
            maxIter=60,
            regParam=0.01,
            elasticNetParam=0.0,
        )
    elif model_name == "rf":
        estimator = RandomForestClassifier(
            featuresCol=estimator_features,
            labelCol="label",
            weightCol="class_weight",
            numTrees=80,
            maxDepth=8,
            subsamplingRate=0.8,
            seed=seed,
        )
    elif model_name == "gbt":
        estimator = GBTClassifier(
            featuresCol=estimator_features,
            labelCol="label",
            weightCol="class_weight",
            maxIter=50,
            maxDepth=5,
            stepSize=0.1,
            subsamplingRate=0.8,
            seed=seed,
        )
    else:
        raise ValueError(f"Unknown reorder model: {model_name}")

    stages.append(estimator)
    return Pipeline(stages=stages)


def evaluate_binary(predictions: DataFrame) -> Dict[str, float]:
    roc_eval = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    )
    pr_eval = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderPR"
    )

    auc_roc = roc_eval.evaluate(predictions)
    auc_pr = pr_eval.evaluate(predictions)

    cm = {
        (float(row["label"]), float(row["prediction"])): row["count"]
        for row in predictions.groupBy("label", "prediction").count().collect()
    }
    tp = float(cm.get((1.0, 1.0), 0))
    fp = float(cm.get((0.0, 1.0), 0))
    fn = float(cm.get((1.0, 0.0), 0))
    tn = float(cm.get((0.0, 0.0), 0))
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    accuracy = (tp + tn) / (tp + tn + fp + fn) if tp + tn + fp + fn else 0.0

    return {
        "auc_roc": auc_roc,
        "auc_pr": auc_pr,
        "precision_pos": precision,
        "recall_pos": recall,
        "f1_pos": f1,
        "accuracy": accuracy,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
    }


def train_reorder(
    train_dataset: DataFrame,
    output_dir: Path,
    model_names: Iterable[str],
    seed: int,
    numerical_features: List[str],
) -> Dict[str, object]:
    data = train_dataset.fillna(
        {
            "u_avg_days_since_prior": 0.0,
            "u_std_days_since_prior": 0.0,
            "u_preferred_dow": 0,
            "u_preferred_hour": 10,
            "up_avg_position": 5.0,
            "p_avg_add_to_cart_order": 5.0,
            "p_reorder_rate": 0.0,
            "p_department_id": -1,
            "p_aisle_id": -1,
        }
    ).withColumn("label", F.col("reordered").cast(DoubleType()))

    train_df, test_df = split_by_user(data, seed)
    train_df, class_weights = add_class_weights(train_df)
    test_df = test_df.withColumn("class_weight", F.lit(1.0)).cache()
    train_df = train_df.cache()

    label_counts = {
        "train": {str(row["label"]): row["count"] for row in train_df.groupBy("label").count().collect()},
        "test": {str(row["label"]): row["count"] for row in test_df.groupBy("label").count().collect()},
    }

    results = []
    best = None
    models_dir = output_dir / "models" / "reorder"
    models_dir.mkdir(parents=True, exist_ok=True)

    for model_name in model_names:
        start = time.time()
        pipeline = build_reorder_pipeline(model_name, seed, numerical_features)
        model = pipeline.fit(train_df)
        predictions = model.transform(test_df).cache()
        metrics = evaluate_binary(predictions)
        elapsed = time.time() - start
        record = {
            "model": model_name,
            "elapsed_seconds": elapsed,
            **metrics,
        }
        results.append(record)
        model_path = models_dir / model_name
        model.write().overwrite().save(str(model_path))
        if best is None or record["auc_pr"] > best["auc_pr"]:
            best = record

    report = {
        "task": "reorder_prediction",
        "selection_metric": "auc_pr",
        "best_model": best,
        "class_weights": class_weights,
        "label_counts": label_counts,
        "all_models": sorted(results, key=lambda x: x["auc_pr"], reverse=True),
        "numeric_features": numerical_features,
        "categorical_features": ["p_department_id"],
    }
    write_json(report, output_dir / "reports" / "reorder_report.json")
    return report


def train_segmentation(
    rfv: DataFrame,
    output_dir: Path,
    k_min: int,
    k_max: int,
    seed: int,
    feature_cols: List[str],
) -> Dict[str, object]:
    fill_defaults = {col: 0.0 for col in feature_cols}
    fill_defaults.update({"frequency": 1.0, "volume": 1.0})
    rfv = rfv.fillna(fill_defaults).cache()
    user_count = rfv.count()
    if user_count < 3:
        report = {"task": "customer_segmentation", "skipped": True, "reason": "Need at least 3 users."}
        write_json(report, output_dir / "reports" / "segmentation_report.json")
        return report

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features", handleInvalid="keep")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
    scaled = scaler.fit(assembler.transform(rfv)).transform(assembler.transform(rfv)).cache()

    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="cluster",
        metricName="silhouette",
        distanceMeasure="squaredEuclidean",
    )

    max_k = min(k_max, user_count - 1)
    k_scores = []
    best_model = None
    best_score = None
    best_k = None
    best_predictions = None

    for k in range(max(k_min, 2), max_k + 1):
        start = time.time()
        km = KMeans(featuresCol="features", predictionCol="cluster", k=k, maxIter=50, seed=seed)
        model = km.fit(scaled)
        predictions = model.transform(scaled).cache()
        silhouette = evaluator.evaluate(predictions)
        score = {
            "k": k,
            "silhouette": silhouette,
            "training_cost": model.summary.trainingCost,
            "elapsed_seconds": time.time() - start,
        }
        k_scores.append(score)
        if best_score is None or silhouette > best_score:
            best_model = model
            best_score = silhouette
            best_k = k
            best_predictions = predictions

    assert best_model is not None and best_predictions is not None
    model_path = output_dir / "models" / "segmentation_kmeans"
    best_model.write().overwrite().save(str(model_path))

    profiles = (
        best_predictions.groupBy("cluster")
        .agg(
            F.count("*").alias("user_count"),
            F.round(F.avg("recency"), 4).alias("avg_recency"),
            F.round(F.avg("frequency"), 4).alias("avg_frequency"),
            F.round(F.avg("volume"), 4).alias("avg_basket_size"),
            F.round(F.avg("u_reorder_rate"), 4).alias("avg_reorder_rate"),
            F.round(F.avg("u_organic_ratio"), 4).alias("avg_organic_ratio"),
        )
        .orderBy("cluster")
    )
    safe_write(best_predictions.select("user_id", "cluster", *RFV_FEATURES), output_dir / "features" / "user_segments")
    safe_write(profiles, output_dir / "reports" / "cluster_profiles")

    report = {
        "task": "customer_segmentation",
        "selection_metric": "silhouette",
        "best_k": best_k,
        "best_silhouette": best_score,
        "k_scores": k_scores,
        "user_count": user_count,
        "features": feature_cols,
    }
    write_json(report, output_dir / "reports" / "segmentation_report.json")
    return report


def train_basket_rules(
    baskets: DataFrame,
    products: DataFrame,
    output_dir: Path,
    min_support: float,
    min_confidence: float,
) -> Dict[str, object]:
    baskets = baskets.filter(F.size("items") >= 2).cache()
    basket_count = baskets.count()
    if basket_count == 0:
        report = {"task": "basket_association_rules", "skipped": True, "reason": "No baskets with 2+ items."}
        write_json(report, output_dir / "reports" / "basket_report.json")
        return report

    fp = FPGrowth(itemsCol="items", minSupport=min_support, minConfidence=min_confidence)
    model = fp.fit(baskets)

    freq_itemsets = model.freqItemsets.orderBy(F.desc("freq"))
    rules = model.associationRules.orderBy(F.desc("lift"), F.desc("confidence"))

    product_map = {
        str(row["product_id"]): row["product_name"]
        for row in products.select("product_id", "product_name").collect()
    }

    def names_for(ids: List[str]) -> List[str]:
        return [product_map.get(str(item), str(item)) for item in ids]

    names_udf = F.udf(names_for, "array<string>")
    rules_named = (
        rules.withColumn("antecedent_names", names_udf("antecedent"))
        .withColumn("consequent_names", names_udf("consequent"))
        .orderBy(F.desc("lift"), F.desc("confidence"))
    )

    model_path = output_dir / "models" / "basket_fpgrowth"
    model.write().overwrite().save(str(model_path))
    safe_write(freq_itemsets, output_dir / "features" / "basket_freq_itemsets")
    safe_write(rules_named, output_dir / "features" / "basket_association_rules")

    top_rules = [
        row.asDict(recursive=True)
        for row in rules_named.select(
            "antecedent",
            "consequent",
            "antecedent_names",
            "consequent_names",
            "confidence",
            "lift",
            "support",
        )
        .limit(20)
        .collect()
    ]
    report = {
        "task": "basket_association_rules",
        "model": "FPGrowth",
        "basket_count": basket_count,
        "min_support": min_support,
        "min_confidence": min_confidence,
        "freq_itemset_count": freq_itemsets.count(),
        "rule_count": rules.count(),
        "top_rules": top_rules,
    }
    write_json(report, output_dir / "reports" / "basket_report.json")
    return report


def write_json(payload: Dict[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)


def load_feature_config(path: str | None) -> Dict[str, object]:
    if not path:
        return {}
    with Path(path).expanduser().resolve().open("r", encoding="utf-8") as fh:
        return json.load(fh)


def selected_tasks(raw: str) -> List[str]:
    tasks = [task.strip().lower() for task in raw.split(",") if task.strip()]
    if "all" in tasks:
        return ["reorder", "segmentation", "basket"]
    valid = {"reorder", "segmentation", "basket"}
    invalid = sorted(set(tasks) - valid)
    if invalid:
        raise ValueError(f"Unknown tasks: {', '.join(invalid)}")
    return tasks


def main() -> None:
    args = parse_args()
    repo_dir = Path.cwd()
    data_dir = (repo_dir / args.data_dir).resolve()
    output_dir = (repo_dir / args.output_dir).resolve()
    feature_dir = output_dir / "features"

    if args.overwrite and output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    spark = create_spark(args.master)
    print(f"Spark version: {spark.version}")
    print(f"Data dir     : {data_dir}")
    print(f"Output dir   : {output_dir}")

    try:
        dfs = read_csvs(spark, data_dir, args.sample_fraction, args.seed)
        features = build_feature_tables(dfs, feature_dir)

        summaries = {}
        tasks = selected_tasks(args.tasks)
        model_names = [name.strip().lower() for name in args.models.split(",") if name.strip()]
        feature_config = load_feature_config(args.feature_config)
        reorder_numeric_features = feature_config.get("reorder_numeric_features", NUMERIC_REORDER_FEATURES)
        segmentation_features = feature_config.get("segmentation_features", RFV_FEATURES)

        if "reorder" in tasks:
            summaries["reorder"] = train_reorder(
                features["train_dataset"],
                output_dir,
                model_names,
                args.seed,
                reorder_numeric_features,
            )
        if "segmentation" in tasks:
            summaries["segmentation"] = train_segmentation(
                features["rfv_features"],
                output_dir,
                args.k_min,
                args.k_max,
                args.seed,
                segmentation_features,
            )
        if "basket" in tasks:
            summaries["basket"] = train_basket_rules(
                features["baskets"],
                dfs["products"],
                output_dir,
                args.min_support,
                args.min_confidence,
            )

        write_json(summaries, output_dir / "reports" / "summary.json")
        print(json.dumps(summaries, indent=2, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
