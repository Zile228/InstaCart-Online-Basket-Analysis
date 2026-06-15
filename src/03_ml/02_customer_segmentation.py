#!/usr/bin/env python3
"""
Task 02: Customer segmentation.

This runner delegates to local_train_mllib.py with the same Colab feature
config and seed. It only selects the segmentation task.
"""

from __future__ import annotations

import sys
from pathlib import Path

from local_train_mllib import main as run_local_mllib


TASK = "segmentation"
PROJECT_DIR = Path(__file__).resolve().parents[2]
CONTAINER_DATA_DIR = Path("/home/nhom05/data")
DEFAULT_DATA_DIR = CONTAINER_DATA_DIR if CONTAINER_DATA_DIR.exists() else PROJECT_DIR / "data"
DEFAULT_OUTPUT_DIR = PROJECT_DIR / "local_outputs" / "02_customer_segmentation_seed42"
DEFAULT_FEATURE_CONFIG = Path(__file__).with_name("selected_features_for_mllib.generated.json")


def has_option(args: list[str], *names: str) -> bool:
    return any(arg in names or any(arg.startswith(f"{name}=") for name in names) for arg in args)


def add_default_args(args: list[str]) -> list[str]:
    final_args = list(args)
    defaults = {
        "--data-dir": str(DEFAULT_DATA_DIR),
        "--tasks": TASK,
        "--output-dir": str(DEFAULT_OUTPUT_DIR),
        "--seed": "42",
        "--sample-fraction": "1.0",
        "--k-min": "2",
        "--k-max": "8",
        "--driver-memory": "5g",
        "--shuffle-partitions": "32",
        "--default-parallelism": "16",
    }
    for option, value in defaults.items():
        if not has_option(final_args, option):
            final_args.extend([option, value])
    if DEFAULT_FEATURE_CONFIG.exists() and not has_option(final_args, "--feature-config"):
        final_args.extend(["--feature-config", str(DEFAULT_FEATURE_CONFIG)])
    if not has_option(final_args, "--overwrite"):
        final_args.append("--overwrite")
    return final_args


def main() -> None:
    sys.argv = [sys.argv[0], *add_default_args(sys.argv[1:])]
    run_local_mllib()


if __name__ == "__main__":
    main()
