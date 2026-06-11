#!/usr/bin/env python3
"""
Scikit-learn feature research lab for Instacart.

This script prototypes feature engineering and model selection on a user sample
before the winning feature recipe is ported to Spark MLlib for full-data runs.

It intentionally reads large order-product CSVs in chunks after sampling users,
so local pandas/scikit-learn work stays bounded.
"""

from __future__ import annotations

import argparse
import json
import shutil
import time
from itertools import combinations
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
    silhouette_score,
)
from sklearn.model_selection import GroupShuffleSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


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
    "u_total_items",
    "u_distinct_products",
    "u_avg_basket_size",
    "u_reorder_rate",
    "u_avg_days_since_prior",
    "u_std_days_since_prior",
    "u_organic_ratio",
    "u_preferred_dow",
    "u_preferred_hour",
    "u_unique_aisles",
    "u_unique_departments",
    "u_produce_ratio",
    "u_dairy_ratio",
    "p_total_orders",
    "p_reorder_rate",
    "p_unique_users",
    "p_avg_add_to_cart_order",
    "p_is_organic",
    "p_aisle_id",
]

CATEGORICAL_REORDER_FEATURES = ["p_department_id"]

SEGMENTATION_FEATURES = [
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
    parser = argparse.ArgumentParser(description="Prototype Instacart features with scikit-learn.")
    parser.add_argument("--data-dir", default="../dataset")
    parser.add_argument("--output-dir", default="local_outputs/sklearn_research")
    parser.add_argument("--sample-frac", type=float, default=0.03)
    parser.add_argument("--max-users", type=int, default=30000)
    parser.add_argument("--chunksize", type=int, default=1_000_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--top-features", type=int, default=18)
    parser.add_argument("--k-min", type=int, default=2)
    parser.add_argument("--k-max", type=int, default=8)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def validate_data_dir(data_dir: Path) -> Dict[str, Path]:
    missing = [name for name in REQUIRED_FILES.values() if not (data_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"Missing files in {data_dir}: {missing}")
    return {key: data_dir / value for key, value in REQUIRED_FILES.items()}


def read_filtered_csv(path: Path, key_col: str, keep_values: Iterable[int], chunksize: int) -> pd.DataFrame:
    keep = set(int(v) for v in keep_values)
    chunks = []
    for chunk in pd.read_csv(path, chunksize=chunksize):
        filtered = chunk[chunk[key_col].isin(keep)]
        if not filtered.empty:
            chunks.append(filtered)
    if not chunks:
        return pd.read_csv(path, nrows=0)
    return pd.concat(chunks, ignore_index=True)


def load_sample(paths: Dict[str, Path], sample_frac: float, max_users: int, chunksize: int, seed: int) -> Dict[str, pd.DataFrame]:
    orders = pd.read_csv(paths["orders"])
    users = orders["user_id"].drop_duplicates()
    sampled_users = users.sample(frac=sample_frac, random_state=seed)
    if len(sampled_users) > max_users:
        sampled_users = sampled_users.sample(n=max_users, random_state=seed)

    orders = orders[orders["user_id"].isin(sampled_users)].copy()
    prior_order_ids = orders.loc[orders["eval_set"].eq("prior"), "order_id"].unique()
    train_order_ids = orders.loc[orders["eval_set"].eq("train"), "order_id"].unique()

    prior = read_filtered_csv(paths["prior"], "order_id", prior_order_ids, chunksize)
    train = read_filtered_csv(paths["train"], "order_id", train_order_ids, chunksize)

    return {
        "orders": orders,
        "prior": prior,
        "train": train,
        "products": pd.read_csv(paths["products"]),
        "aisles": pd.read_csv(paths["aisles"]),
        "departments": pd.read_csv(paths["departments"]),
    }


def preferred_mode(df: pd.DataFrame, value_col: str, output_col: str) -> pd.DataFrame:
    counts = df.groupby(["user_id", value_col], as_index=False).size()
    counts = counts.sort_values(["user_id", "size", value_col], ascending=[True, False, True])
    return counts.drop_duplicates("user_id")[["user_id", value_col]].rename(columns={value_col: output_col})


def build_features(dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    orders = dfs["orders"].copy()
    prior = dfs["prior"].copy()
    train = dfs["train"].copy()
    products = dfs["products"].copy()
    aisles = dfs["aisles"].copy()
    departments = dfs["departments"].copy()

    products_full = products.merge(aisles, on="aisle_id", how="left").merge(
        departments, on="department_id", how="left"
    )
    products_full["is_organic"] = products_full["product_name"].str.lower().str.contains("organic").astype(int)

    orders_prior = orders[orders["eval_set"].eq("prior")].copy()
    prior_full = (
        prior.merge(
            orders_prior[
                [
                    "order_id",
                    "user_id",
                    "order_number",
                    "days_since_prior_order",
                    "order_dow",
                    "order_hour_of_day",
                ]
            ],
            on="order_id",
            how="inner",
        )
        .merge(
            products_full[
                [
                    "product_id",
                    "product_name",
                    "aisle_id",
                    "department_id",
                    "aisle",
                    "department",
                    "is_organic",
                ]
            ],
            on="product_id",
            how="inner",
        )
    )
    prior_full["days_since_prior_order"] = prior_full["days_since_prior_order"].fillna(0.0)

    user_features = prior_full.groupby("user_id").agg(
        u_total_orders=("order_id", "nunique"),
        u_total_items=("product_id", "size"),
        u_distinct_products=("product_id", "nunique"),
        u_reorder_sum=("reordered", "sum"),
        u_avg_days_since_prior=("days_since_prior_order", "mean"),
        u_std_days_since_prior=("days_since_prior_order", "std"),
        u_organic_sum=("is_organic", "sum"),
        u_unique_aisles=("aisle_id", "nunique"),
        u_unique_departments=("department_id", "nunique"),
        u_produce_items=("department", lambda s: (s == "produce").sum()),
        u_dairy_items=("department", lambda s: (s == "dairy eggs").sum()),
    ).reset_index()
    user_features["u_avg_basket_size"] = user_features["u_total_items"] / user_features["u_total_orders"]
    user_features["u_reorder_rate"] = user_features["u_reorder_sum"] / user_features["u_total_items"]
    user_features["u_organic_ratio"] = user_features["u_organic_sum"] / user_features["u_total_items"]
    user_features["u_produce_ratio"] = user_features["u_produce_items"] / user_features["u_total_items"]
    user_features["u_dairy_ratio"] = user_features["u_dairy_items"] / user_features["u_total_items"]
    user_features = user_features.drop(columns=["u_reorder_sum", "u_organic_sum", "u_produce_items", "u_dairy_items"])
    user_features["u_std_days_since_prior"] = user_features["u_std_days_since_prior"].fillna(0.0)
    user_features = user_features.merge(preferred_mode(prior_full, "order_dow", "u_preferred_dow"), on="user_id", how="left")
    user_features = user_features.merge(
        preferred_mode(prior_full, "order_hour_of_day", "u_preferred_hour"), on="user_id", how="left"
    )

    product_features = prior_full.groupby("product_id").agg(
        p_total_orders=("order_id", "size"),
        p_reorder_sum=("reordered", "sum"),
        p_unique_users=("user_id", "nunique"),
        p_avg_add_to_cart_order=("add_to_cart_order", "mean"),
        p_is_organic=("is_organic", "first"),
        p_department_id=("department_id", "first"),
        p_aisle_id=("aisle_id", "first"),
    ).reset_index()
    product_features["p_reorder_rate"] = product_features["p_reorder_sum"] / product_features["p_total_orders"]
    product_features = product_features.drop(columns=["p_reorder_sum"])

    up_features = prior_full.groupby(["user_id", "product_id"]).agg(
        up_order_count=("order_id", "size"),
        up_avg_position=("add_to_cart_order", "mean"),
        up_first_order_number=("order_number", "min"),
        up_last_order_number=("order_number", "max"),
    ).reset_index()
    up_features = up_features.merge(user_features[["user_id", "u_total_orders"]], on="user_id", how="inner")
    up_features["up_reorder_rate"] = up_features["up_order_count"] / up_features["u_total_orders"]
    up_features["up_orders_since_last"] = up_features["u_total_orders"] - up_features["up_last_order_number"]
    up_features["up_order_rate_since_first"] = up_features["up_order_count"] / (
        up_features["u_total_orders"] - up_features["up_first_order_number"] + 1
    )
    up_features = up_features.drop(columns=["u_total_orders"])

    orders_train = orders[orders["eval_set"].eq("train")][["order_id", "user_id"]]
    train_positive = train.merge(orders_train, on="order_id", how="inner")[["user_id", "product_id", "reordered"]]
    train_user_ids = orders_train[["user_id"]].drop_duplicates()

    candidates = up_features.merge(train_user_ids, on="user_id", how="inner")
    train_dataset = (
        candidates.merge(train_positive, on=["user_id", "product_id"], how="left")
        .assign(reordered=lambda x: x["reordered"].fillna(0).astype(int))
        .merge(user_features, on="user_id", how="left")
        .merge(product_features, on="product_id", how="left")
    )

    last_orders = orders_prior.sort_values(["user_id", "order_number"]).drop_duplicates("user_id", keep="last")
    recency = last_orders[["user_id", "days_since_prior_order"]].rename(columns={"days_since_prior_order": "recency"})
    rfv = user_features.merge(recency, on="user_id", how="left")
    rfv["recency"] = rfv["recency"].fillna(0.0)
    rfv["frequency"] = rfv["u_total_orders"]
    rfv["volume"] = rfv["u_avg_basket_size"]

    baskets = prior.groupby("order_id")["product_id"].apply(lambda s: sorted(set(s.astype(str)))).reset_index(name="items")

    return {
        "prior_full": prior_full,
        "train_dataset": train_dataset,
        "rfv": rfv,
        "baskets": baskets,
        "products": products,
    }


def build_model_pipeline(model_name: str, numeric_features: List[str], categorical_features: List[str], seed: int) -> Pipeline:
    if model_name == "logreg":
        numeric_transformer = StandardScaler()
        estimator = LogisticRegression(max_iter=300, class_weight="balanced", n_jobs=1, random_state=seed)
    elif model_name == "rf":
        numeric_transformer = "passthrough"
        estimator = RandomForestClassifier(
            n_estimators=180,
            max_depth=12,
            min_samples_leaf=20,
            class_weight="balanced_subsample",
            n_jobs=1,
            random_state=seed,
        )
    elif model_name == "hist_gbdt":
        numeric_transformer = "passthrough"
        estimator = HistGradientBoostingClassifier(
            learning_rate=0.08,
            max_iter=180,
            max_leaf_nodes=31,
            l2_regularization=0.05,
            class_weight="balanced",
            random_state=seed,
        )
    else:
        raise ValueError(f"Unknown model: {model_name}")

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric_features),
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features),
        ],
        remainder="drop",
    )
    return Pipeline([("prep", preprocessor), ("model", estimator)])


def evaluate_reorder(train_dataset: pd.DataFrame, output_dir: Path, top_features: int, seed: int) -> Dict[str, object]:
    data = train_dataset.copy()
    data[NUMERIC_REORDER_FEATURES] = data[NUMERIC_REORDER_FEATURES].fillna(0.0)
    data[CATEGORICAL_REORDER_FEATURES] = data[CATEGORICAL_REORDER_FEATURES].fillna(-1).astype(int)

    feature_cols = NUMERIC_REORDER_FEATURES + CATEGORICAL_REORDER_FEATURES
    X = data[feature_cols]
    y = data["reordered"].astype(int)
    groups = data["user_id"]

    splitter = GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=seed)
    train_idx, test_idx = next(splitter.split(X, y, groups=groups))
    X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
    y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
    test_users = groups.iloc[test_idx].reset_index(drop=True)

    candidates = [
        {"trial": "logreg_C0.3", "family": "logreg", "params": {"model__C": 0.3}},
        {"trial": "logreg_C1.0", "family": "logreg", "params": {"model__C": 1.0}},
        {"trial": "logreg_C3.0", "family": "logreg", "params": {"model__C": 3.0}},
        {
            "trial": "rf_depth10_leaf20",
            "family": "rf",
            "params": {"model__n_estimators": 120, "model__max_depth": 10, "model__min_samples_leaf": 20},
        },
        {
            "trial": "rf_depth14_leaf30",
            "family": "rf",
            "params": {"model__n_estimators": 160, "model__max_depth": 14, "model__min_samples_leaf": 30},
        },
        {
            "trial": "hgb_lr0.05_iter160_leaf31",
            "family": "hist_gbdt",
            "params": {"model__learning_rate": 0.05, "model__max_iter": 160, "model__max_leaf_nodes": 31},
        },
        {
            "trial": "hgb_lr0.08_iter180_leaf31",
            "family": "hist_gbdt",
            "params": {"model__learning_rate": 0.08, "model__max_iter": 180, "model__max_leaf_nodes": 31},
        },
        {
            "trial": "hgb_lr0.10_iter220_leaf31",
            "family": "hist_gbdt",
            "params": {"model__learning_rate": 0.10, "model__max_iter": 220, "model__max_leaf_nodes": 31},
        },
        {
            "trial": "hgb_lr0.08_iter220_leaf63",
            "family": "hist_gbdt",
            "params": {"model__learning_rate": 0.08, "model__max_iter": 220, "model__max_leaf_nodes": 63},
        },
    ]

    results = []
    fitted = {}
    for candidate in candidates:
        start = time.time()
        pipe = build_model_pipeline(candidate["family"], NUMERIC_REORDER_FEATURES, CATEGORICAL_REORDER_FEATURES, seed)
        pipe.set_params(**candidate["params"])
        pipe.fit(X_train, y_train)
        proba = pipe.predict_proba(X_test)[:, 1]
        pred = (proba >= 0.5).astype(int)
        top20_recall = recall_at_k(test_users, y_test.reset_index(drop=True), proba, k=20)
        record = {
            "trial": candidate["trial"],
            "model": candidate["family"],
            "params": candidate["params"],
            "average_precision": float(average_precision_score(y_test, proba)),
            "roc_auc": float(roc_auc_score(y_test, proba)) if y_test.nunique() == 2 else None,
            "precision_pos": float(precision_score(y_test, pred, zero_division=0)),
            "recall_pos": float(recall_score(y_test, pred, zero_division=0)),
            "f1_pos": float(f1_score(y_test, pred, zero_division=0)),
            "recall_at_20": float(top20_recall),
            "elapsed_seconds": time.time() - start,
        }
        fitted[candidate["trial"]] = pipe
        results.append(record)

    best = max(results, key=lambda r: (r["average_precision"], r["recall_at_20"]))
    best_pipe = fitted[best["trial"]]
    perm = permutation_importance(
        best_pipe,
        X_test,
        y_test,
        scoring="average_precision",
        n_repeats=5,
        random_state=seed,
        n_jobs=1,
        max_samples=min(5000, len(X_test)),
    )
    importances = sorted(
        [
            {
                "feature": feature,
                "importance_mean": float(mean),
                "importance_std": float(std),
            }
            for feature, mean, std in zip(feature_cols, perm.importances_mean, perm.importances_std)
        ],
        key=lambda row: row["importance_mean"],
        reverse=True,
    )

    selected = [
        row["feature"]
        for row in importances
        if row["importance_mean"] > 0 and row["feature"] in NUMERIC_REORDER_FEATURES
    ][:top_features]
    if len(selected) < 8:
        fallback = [
            "up_order_count",
            "up_reorder_rate",
            "up_orders_since_last",
            "up_order_rate_since_first",
            "up_avg_position",
            "u_total_orders",
            "u_avg_basket_size",
            "u_reorder_rate",
            "p_total_orders",
            "p_reorder_rate",
        ]
        selected = list(dict.fromkeys(selected + fallback))[:top_features]

    report = {
        "task": "reorder_feature_research",
        "split": "GroupShuffleSplit by user_id",
        "selection_metric": "average_precision",
        "rows": int(len(data)),
        "positive_rate": float(y.mean()),
        "best_model": best,
        "all_models": sorted(results, key=lambda r: r["average_precision"], reverse=True),
        "permutation_importance": importances,
        "selected_numeric_features_for_mllib": selected,
        "categorical_features_for_mllib": CATEGORICAL_REORDER_FEATURES,
    }
    write_json(report, output_dir / "reorder_feature_research.json")
    return report


def recall_at_k(users: pd.Series, y_true: pd.Series, proba: np.ndarray, k: int) -> float:
    eval_df = pd.DataFrame({"user_id": users, "y": y_true, "proba": proba})
    recalls = []
    for _, grp in eval_df.groupby("user_id"):
        positives = grp["y"].sum()
        if positives == 0:
            continue
        top = grp.sort_values("proba", ascending=False).head(k)
        recalls.append(top["y"].sum() / positives)
    return float(np.mean(recalls)) if recalls else 0.0


def evaluate_segmentation(rfv: pd.DataFrame, output_dir: Path, k_min: int, k_max: int, seed: int) -> Dict[str, object]:
    data = rfv.copy()
    data[SEGMENTATION_FEATURES] = data[SEGMENTATION_FEATURES].fillna(0.0)
    X = StandardScaler().fit_transform(data[SEGMENTATION_FEATURES])
    max_k = min(k_max, len(data) - 1)
    scores = []
    best_labels = None
    best_score = -1.0
    best_k = None
    for k in range(max(2, k_min), max_k + 1):
        labels = KMeans(n_clusters=k, n_init="auto", random_state=seed).fit_predict(X)
        score = float(silhouette_score(X, labels)) if len(set(labels)) > 1 else -1.0
        scores.append({"k": k, "silhouette": score})
        if score > best_score:
            best_score = score
            best_k = k
            best_labels = labels

    profiled = data.copy()
    profiled["cluster"] = best_labels
    profiles = profiled.groupby("cluster")[SEGMENTATION_FEATURES].mean().round(4)
    profiles["user_count"] = profiled.groupby("cluster").size()
    profiles.to_csv(output_dir / "segmentation_cluster_profiles.csv")

    report = {
        "task": "segmentation_feature_research",
        "selection_metric": "silhouette plus business interpretability",
        "features": SEGMENTATION_FEATURES,
        "best_k": int(best_k),
        "best_silhouette": float(best_score),
        "k_scores": scores,
    }
    write_json(report, output_dir / "segmentation_feature_research.json")
    return report


def evaluate_basket_pairs(
    baskets: pd.DataFrame,
    products: pd.DataFrame,
    output_dir: Path,
    min_support_count: int = 5,
) -> Dict[str, object]:
    basket_items = baskets["items"].tolist()
    basket_count = len(basket_items)
    item_counts: Dict[str, int] = {}
    pair_counts: Dict[Tuple[str, str], int] = {}
    for items in basket_items:
        unique_items = sorted(set(items))
        for item in unique_items:
            item_counts[item] = item_counts.get(item, 0) + 1
        for left, right in combinations(unique_items, 2):
            pair_counts[(left, right)] = pair_counts.get((left, right), 0) + 1

    product_map = products.assign(product_id=lambda df: df["product_id"].astype(str)).set_index("product_id")[
        "product_name"
    ].to_dict()
    rows = []
    for (left, right), pair_count in pair_counts.items():
        if pair_count < min_support_count:
            continue
        support = pair_count / basket_count
        left_support = item_counts[left] / basket_count
        right_support = item_counts[right] / basket_count
        rows.append(
            {
                "left_id": left,
                "right_id": right,
                "left_name": product_map.get(left, left),
                "right_name": product_map.get(right, right),
                "pair_count": pair_count,
                "support": support,
                "confidence_left_to_right": pair_count / item_counts[left],
                "confidence_right_to_left": pair_count / item_counts[right],
                "lift": support / (left_support * right_support),
            }
        )
    pair_df = pd.DataFrame(rows)
    if not pair_df.empty:
        pair_df = pair_df.sort_values(["lift", "support"], ascending=False)
    pair_df.head(200).to_csv(output_dir / "basket_pair_baseline_top200.csv", index=False)

    report = {
        "task": "basket_pair_research",
        "basket_count": basket_count,
        "min_support_count": min_support_count,
        "top_pairs": pair_df.head(20).to_dict(orient="records"),
        "mllib_recommendation": {
            "algorithm": "FPGrowth",
            "sort_rules_by": ["lift", "confidence", "support"],
            "starting_grid": {
                "minSupport": [0.001, 0.003, 0.005, 0.01],
                "minConfidence": [0.1, 0.2, 0.3],
            },
        },
    }
    write_json(report, output_dir / "basket_pair_research.json")
    return report


def write_json(payload: Dict[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)


def main() -> None:
    args = parse_args()
    data_dir = Path(args.data_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    if args.overwrite and output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    paths = validate_data_dir(data_dir)
    dfs = load_sample(paths, args.sample_frac, args.max_users, args.chunksize, args.seed)
    features = build_features(dfs)

    reports = {
        "reorder": evaluate_reorder(features["train_dataset"], output_dir, args.top_features, args.seed),
        "segmentation": evaluate_segmentation(features["rfv"], output_dir, args.k_min, args.k_max, args.seed),
        "basket": evaluate_basket_pairs(features["baskets"], features["products"], output_dir),
    }
    selected_config = {
        "source": "sklearn_feature_research.py",
        "reorder_numeric_features": reports["reorder"]["selected_numeric_features_for_mllib"],
        "reorder_categorical_features": reports["reorder"]["categorical_features_for_mllib"],
        "segmentation_features": reports["segmentation"]["features"],
        "basket": reports["basket"]["mllib_recommendation"],
    }
    write_json(selected_config, output_dir / "selected_features_for_mllib.json")
    write_json(reports, output_dir / "summary.json")
    print(json.dumps(reports, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
