# ============================================================
#  streaming_job.py
#  Spark Structured Streaming — Kafka Consumer
#  PySpark 4.1.1 · Kafka 4.1.2 (KRaft)
#
#  Đọc event từ Kafka topic "instacart-orders",
#  join với predictions từ HDFS,
#  ghi kết quả ra 2 sink:
#    1. HDFS (Parquet, append mode, checkpoint)
#    2. Supabase PostgreSQL (via JDBC, forEach sink)
#
#  Chạy bằng: bash submit_streaming.sh
#  (KHÔNG chạy trực tiếp bằng spark-submit vì cần --packages)
# ============================================================

import os
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, FloatType, TimestampType
)

# ── Config từ environment ─────────────────────────────────────
HDFS           = os.getenv("HDFS_NAMENODE",         "hdfs://namenode:9000")
SPARK_URL      = os.getenv("SPARK_MASTER",           "spark://spark-master:7077")
KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS","kafka:29092")  # internal Docker listener
KAFKA_TOPIC    = os.getenv("KAFKA_TOPIC",            "instacart-orders")
SUPABASE_JDBC  = os.getenv("SUPABASE_JDBC_URL",      "")            # jdbc:postgresql://...
SUPABASE_USER  = os.getenv("SUPABASE_DB_USER",       "postgres")
SUPABASE_PASS  = os.getenv("SUPABASE_DB_PASSWORD",   "")

FEAT           = f"{HDFS}/instacart/features"
STREAMING_OUT  = f"{HDFS}/instacart/streaming"
CHECKPOINT     = f"{HDFS}/instacart/checkpoints/streaming_job"

# ── SparkSession ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Instacart-StructuredStreaming") \
    .master(SPARK_URL) \
    .config("spark.hadoop.fs.defaultFS", HDFS) \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(f"Spark Streaming started — version {spark.version}")
print(f"Reading from Kafka: {KAFKA_SERVERS} / {KAFKA_TOPIC}")

# ── Schema cho JSON payload từ Kafka ─────────────────────────
ORDER_SCHEMA = StructType([
    StructField("order_id",         IntegerType(), True),
    StructField("user_id",          IntegerType(), True),
    StructField("product_id",       IntegerType(), True),
    StructField("product_name",     StringType(),  True),
    StructField("department",       StringType(),  True),
    StructField("reordered",        IntegerType(), True),
    StructField("event_timestamp",  StringType(),  True),
])

# ── Load static lookup: predictions từ GBT model ─────────────
# File này được tạo bởi 01_reorder_classifier.py (CELL 9)
# Schema: user_id, product_id, reorder_probability, predicted_reorder
print("Loading reorder predictions lookup table...")
try:
    predictions_lookup = spark.read.parquet(f"{FEAT}/reorder_predictions.parquet") \
        .select(
            F.col("user_id"),
            F.col("product_id"),
            F.col("reorder_probability"),
            F.col("prediction").cast("int").alias("predicted_reorder")
        )
    predictions_lookup.cache()
    pred_count = predictions_lookup.count()
    print(f"  ✓ Loaded {pred_count:,} predictions")
except Exception as e:
    print(f"  ⚠ Could not load predictions: {e}")
    print("  → Running without prediction enrichment")
    # Tạo empty DataFrame nếu chưa có predictions
    predictions_lookup = spark.createDataFrame([], StructType([
        StructField("user_id",            IntegerType(), True),
        StructField("product_id",         IntegerType(), True),
        StructField("reorder_probability",FloatType(),   True),
        StructField("predicted_reorder",  IntegerType(), True),
    ]))

# ── Source: đọc từ Kafka ──────────────────────────────────────
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 1000) \
    .load()

# ── Parse JSON payload ────────────────────────────────────────
parsed_stream = raw_stream \
    .select(
        F.from_json(
            F.col("value").cast("string"),
            ORDER_SCHEMA
        ).alias("data"),
        F.col("timestamp").alias("kafka_timestamp")
    ) \
    .select(
        F.col("data.order_id"),
        F.col("data.user_id"),
        F.col("data.product_id"),
        F.col("data.product_name"),
        F.col("data.department"),
        F.col("data.reordered"),
        F.to_timestamp(F.col("data.event_timestamp")).alias("event_timestamp"),
        F.col("kafka_timestamp")
    ) \
    .filter(F.col("order_id").isNotNull())   # bỏ malformed messages

# ── Enrich: join với GBT predictions ─────────────────────────
# Left join: event có prediction thì thêm probability,
# không có thì để null (user/product mới)
enriched_stream = parsed_stream \
    .join(
        predictions_lookup,
        on=["user_id", "product_id"],
        how="left"
    ) \
    .withColumn("processed_at", F.current_timestamp()) \
    .withColumn(
        # Default probability nếu không có prediction
        "reorder_probability",
        F.coalesce(F.col("reorder_probability"), F.lit(0.0))
    ) \
    .withColumn(
        "predicted_reorder",
        F.coalesce(F.col("predicted_reorder"), F.lit(-1))  # -1 = unknown
    )

# ── SINK 1: HDFS Parquet (append mode) ───────────────────────
# Lưu raw enriched stream vào HDFS, partition by date
hdfs_query = enriched_stream \
    .withColumn("date", F.to_date("event_timestamp")) \
    .writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", STREAMING_OUT) \
    .option("checkpointLocation", f"{CHECKPOINT}/hdfs") \
    .partitionBy("date") \
    .trigger(processingTime="10 seconds") \
    .start()

print(f"  ✓ HDFS sink started → {STREAMING_OUT}")

# ── SINK 2: Supabase via JDBC (forEach) ──────────────────────
# Ghi vào bảng streaming_orders trên Supabase PostgreSQL
# Schema SQL (chạy trước trong Supabase SQL editor):
#   CREATE TABLE streaming_orders (
#     id BIGSERIAL PRIMARY KEY,
#     order_id INT, user_id INT, product_id INT,
#     product_name TEXT, department TEXT,
#     reordered INT, predicted_reorder INT,
#     reorder_probability FLOAT,
#     event_timestamp TIMESTAMPTZ,
#     processed_at TIMESTAMPTZ DEFAULT NOW()
#   );

def write_to_supabase(batch_df, batch_id):
    """Ghi một micro-batch vào Supabase PostgreSQL."""
    if SUPABASE_JDBC == "":
        # Nếu chưa config Supabase, print ra console để debug
        count = batch_df.count()
        if count > 0:
            print(f"  [Batch {batch_id}] {count} rows — Supabase not configured, printing sample:")
            batch_df.select(
                "order_id", "user_id", "product_name",
                "reorder_probability", "processed_at"
            ).show(5, truncate=30)
        return

    try:
        (batch_df
         .select(
             "order_id", "user_id", "product_id",
             "product_name", "department",
             "reordered", "predicted_reorder",
             "reorder_probability",
             "event_timestamp", "processed_at"
         )
         .write
         .format("jdbc")
         .option("url", SUPABASE_JDBC)
         .option("dbtable", "streaming_orders")
         .option("user", SUPABASE_USER)
         .option("password", SUPABASE_PASS)
         .option("driver", "org.postgresql.Driver")
         .option("batchsize", 500)
         .mode("append")
         .save()
        )
        print(f"  [Batch {batch_id}] ✓ {batch_df.count()} rows written to Supabase")
    except Exception as e:
        print(f"  [Batch {batch_id}] ✗ Supabase write error: {e}")

supabase_query = enriched_stream \
    .writeStream \
    .foreachBatch(write_to_supabase) \
    .outputMode("append") \
    .option("checkpointLocation", f"{CHECKPOINT}/supabase") \
    .trigger(processingTime="10 seconds") \
    .start()

print(f"  ✓ Supabase sink started")

# ── Debug sink: in ra console (dùng để test) ─────────────────
console_query = parsed_stream \
    .select("order_id", "user_id", "product_name", "department", "event_timestamp") \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("numRows", 5) \
    .trigger(processingTime="10 seconds") \
    .start()

print(f"  ✓ Console sink started (debug)")

# ── Await termination ─────────────────────────────────────────
print(f"\n{'═'*55}")
print(f"  Streaming job running — Ctrl+C to stop")
print(f"  Kafka topic : {KAFKA_TOPIC}")
print(f"  HDFS output : {STREAMING_OUT}")
print(f"  Trigger     : every 10 seconds")
print(f"{'═'*55}\n")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\nStopping all streaming queries...")
    for q in spark.streams.active:
        q.stop()
    print("All queries stopped ✓")
finally:
    spark.stop()
    print("SparkSession stopped.")
