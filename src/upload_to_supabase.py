#!/usr/bin/env python3
# ============================================================
#  upload_to_supabase.py
#  Upload CSV exports lên Supabase PostgreSQL
#
#  Chạy SAU KHI đã chạy xong các notebook feature engineering
#  và SQL analysis (các file CSV đã có trong src/02_sql/exports/)
#
#  Chuẩn bị:
#    1. Tạo các bảng trong Supabase (SQL bên dưới)
#    2. Điền SUPABASE_URL và SUPABASE_KEY vào .env
#    3. pip install supabase pandas python-dotenv
#    4. python upload_to_supabase.py
# ============================================================

import os
import sys
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Supabase config ───────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")  # dùng service_role key

if not SUPABASE_URL or not SUPABASE_KEY:
    print("""
ERROR: Supabase credentials not set.
Tạo file .env với nội dung:
  SUPABASE_URL=https://xxxx.supabase.co
  SUPABASE_SERVICE_KEY=eyJhbGc...
""")
    sys.exit(1)

try:
    from supabase import create_client
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print(f"✓ Connected to Supabase: {SUPABASE_URL}")
except ImportError:
    print("ERROR: pip install supabase")
    sys.exit(1)

# ── Paths ─────────────────────────────────────────────────────
SCRIPT_DIR  = Path(__file__).parent
EXPORTS_DIR = SCRIPT_DIR.parent / "02_sql" / "exports"

# ──────────────────────────────────────────────────────────────
# SQL để tạo bảng trong Supabase — chạy trong SQL Editor trước
# ──────────────────────────────────────────────────────────────
CREATE_TABLES_SQL = """
-- Chạy trong Supabase SQL Editor → New Query

-- 1. Heatmap: mật độ đơn theo giờ × ngày
CREATE TABLE IF NOT EXISTS hourly_heatmap (
    order_dow      INT,
    order_hour     INT,
    order_count    BIGINT,
    PRIMARY KEY (order_dow, order_hour)
);

-- 2. Top products
CREATE TABLE IF NOT EXISTS top_products (
    product_id     INT PRIMARY KEY,
    product_name   TEXT,
    department     TEXT,
    aisle          TEXT,
    total_orders   BIGINT,
    reorder_count  BIGINT,
    reorder_rate   FLOAT
);

-- 3. Department stats
CREATE TABLE IF NOT EXISTS department_stats (
    department_id      INT PRIMARY KEY,
    department         TEXT,
    total_orders       BIGINT,
    unique_products    BIGINT,
    reorder_rate       FLOAT,
    avg_cart_position  FLOAT
);

-- 4. Customer segments (từ KMeans notebook)
CREATE TABLE IF NOT EXISTS customer_segments (
    user_id            INT PRIMARY KEY,
    segment_id         INT,
    segment_name       TEXT,
    recency            FLOAT,
    frequency          FLOAT,
    volume             FLOAT
);

-- 5. Association rules (từ FP-Growth notebook)
CREATE TABLE IF NOT EXISTS association_rules (
    id             BIGSERIAL PRIMARY KEY,
    if_buy         TEXT,
    then_buy       TEXT,
    confidence     FLOAT,
    lift           FLOAT,
    support        FLOAT
);

-- 6. Streaming orders (ghi real-time bởi streaming_job.py)
CREATE TABLE IF NOT EXISTS streaming_orders (
    id                   BIGSERIAL PRIMARY KEY,
    order_id             INT,
    user_id              INT,
    product_id           INT,
    product_name         TEXT,
    department           TEXT,
    reordered            INT,
    predicted_reorder    INT,
    reorder_probability  FLOAT,
    event_timestamp      TIMESTAMPTZ,
    processed_at         TIMESTAMPTZ DEFAULT NOW()
);

-- Index để query nhanh
CREATE INDEX IF NOT EXISTS idx_streaming_processed_at ON streaming_orders (processed_at DESC);
CREATE INDEX IF NOT EXISTS idx_streaming_user_id      ON streaming_orders (user_id);
CREATE INDEX IF NOT EXISTS idx_segments_segment       ON customer_segments (segment_id);
"""
print("\n" + "─" * 55)
print("NOTE: Nếu chưa tạo bảng, chạy SQL này trong Supabase:")
print(CREATE_TABLES_SQL[:200] + "... (xem file để lấy full SQL)")
print("─" * 55 + "\n")


# ── Upload function ───────────────────────────────────────────
def upload_df(df: pd.DataFrame, table: str, batch_size: int = 500):
    """Upload DataFrame lên Supabase table theo batch."""
    rows      = df.to_dict(orient="records")
    total     = len(rows)
    uploaded  = 0

    print(f"\nUploading {total:,} rows → {table}...")

    # Xóa dữ liệu cũ trước (upsert hoặc truncate)
    try:
        supabase.table(table).delete().neq("id", -1).execute()   # delete all
    except Exception:
        pass  # bảng có thể không có cột 'id' (như hourly_heatmap)

    for i in range(0, total, batch_size):
        batch = rows[i : i + batch_size]
        try:
            supabase.table(table).insert(batch).execute()
            uploaded += len(batch)
            pct = uploaded / total * 100
            print(f"  [{pct:5.1f}%] {uploaded:,}/{total:,} rows", end="\r")
        except Exception as e:
            print(f"\n  ✗ Error at batch {i//batch_size}: {e}")
            print(f"    Sample row: {batch[0]}")
            raise

    print(f"  ✓ {uploaded:,} rows uploaded to '{table}'          ")


# ── 1. hourly_heatmap ─────────────────────────────────────────
heatmap_file = EXPORTS_DIR / "hourly_heatmap.csv"
if heatmap_file.exists():
    df = pd.read_csv(heatmap_file)
    # Rename columns cho khớp schema
    df = df.rename(columns={
        "order_count": "order_count",
        "order_hour_of_day": "order_hour"
    })
    upload_df(df[["order_dow", "order_hour", "order_count"]], "hourly_heatmap")
else:
    print(f"  ⚠ {heatmap_file} not found — skipping")

# ── 2. top_products ───────────────────────────────────────────
products_file = EXPORTS_DIR / "top_products.csv"
if products_file.exists():
    df = pd.read_csv(products_file)
    # Đảm bảo cột đúng
    cols = ["product_id", "product_name", "department", "aisle",
            "total_orders", "reorder_count", "reorder_rate"]
    available = [c for c in cols if c in df.columns]
    upload_df(df[available], "top_products")
else:
    print(f"  ⚠ {products_file} not found — skipping")

# ── 3. department_stats ───────────────────────────────────────
dept_file = EXPORTS_DIR / "department_stats.csv"
if dept_file.exists():
    df = pd.read_csv(dept_file)
    cols = ["department_id", "department", "total_orders",
            "unique_products", "reorder_rate", "avg_cart_position"]
    available = [c for c in cols if c in df.columns]
    upload_df(df[available], "department_stats")
else:
    print(f"  ⚠ {dept_file} not found — skipping")

# ── 4. customer_segments ─────────────────────────────────────
segments_file = EXPORTS_DIR / "user_segments.csv"
if segments_file.exists():
    df = pd.read_csv(segments_file)
    cols = ["user_id", "segment_id", "segment_name", "recency", "frequency", "volume"]
    available = [c for c in cols if c in df.columns]
    upload_df(df[available], "customer_segments")
else:
    print(f"  ⚠ {segments_file} not found — skipping")

# ── 5. association_rules ──────────────────────────────────────
rules_file = EXPORTS_DIR / "association_rules.csv"
if rules_file.exists():
    df = pd.read_csv(rules_file)
    cols = ["if_buy", "then_buy", "confidence", "lift", "support"]
    available = [c for c in cols if c in df.columns]
    upload_df(df[available].head(500), "association_rules")
else:
    print(f"  ⚠ {rules_file} not found — skipping")

# ── Summary ───────────────────────────────────────────────────
print(f"\n{'═'*55}")
print(f"  Supabase upload complete ✓")
print(f"  Dashboard: {SUPABASE_URL}/project/default/editor")
print(f"{'═'*55}")
