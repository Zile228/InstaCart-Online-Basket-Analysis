#!/usr/bin/env python3
# ============================================================
#  order_producer.py
#  Kafka Producer — Giả lập luồng đơn hàng real-time
#  Kafka 4.1.2 (KRaft, no Zookeeper)
#
#  Đọc dữ liệu từ orders.csv + order_products__train.csv,
#  gửi từng event lên topic "instacart-orders" với tốc độ
#  có thể điều chỉnh qua EVENTS_PER_SECOND.
#
#  Chạy:
#    # Từ host (Windows/Mac):
#    python order_producer.py
#
#    # Từ trong Docker container jupyter:
#    docker exec -it jupyter python3 /home/nhom05/work/04_streaming/order_producer.py
#
#  Yêu cầu: pip install confluent-kafka pandas python-dotenv
# ============================================================

import os
import json
import time
import random
import pandas as pd
from datetime import datetime, timezone
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────
# Từ host Windows: localhost:9092
# Trong Docker network: kafka:29092
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME      = os.getenv("KAFKA_TOPIC", "instacart-orders")
DATA_DIR        = os.getenv("DATA_DIR", "../data")           # path đến thư mục data
EVENTS_PER_SEC  = float(os.getenv("EVENTS_PER_SECOND", "2")) # tốc độ gửi event

print(f"""
╔══════════════════════════════════════════════╗
║  Instacart Kafka Producer                    ║
║  Bootstrap : {KAFKA_BOOTSTRAP:<30} ║
║  Topic     : {TOPIC_NAME:<30} ║
║  Speed     : {EVENTS_PER_SEC} events/second               ║
╚══════════════════════════════════════════════╝
""")

# ── Tạo Kafka topic nếu chưa có ──────────────────────────────
def ensure_topic(bootstrap: str, topic: str, num_partitions: int = 3):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics
    if topic not in existing:
        print(f"Creating topic '{topic}'...")
        new_topic = NewTopic(topic, num_partitions=num_partitions, replication_factor=1)
        fs = admin.create_topics([new_topic])
        for t, f in fs.items():
            try:
                f.result()
                print(f"  ✓ Topic '{t}' created")
            except Exception as e:
                print(f"  Topic error: {e}")
    else:
        print(f"  Topic '{topic}' already exists ✓")

# ── Load data ─────────────────────────────────────────────────
def load_data(data_dir: str):
    """Load và join CSV files, trả về DataFrame sẵn sàng để stream."""
    print("\nLoading CSV data...")

    orders_path   = os.path.join(data_dir, "orders.csv")
    train_path    = os.path.join(data_dir, "order_products__train.csv")
    products_path = os.path.join(data_dir, "products.csv")
    depts_path    = os.path.join(data_dir, "departments.csv")

    for p in [orders_path, train_path, products_path, depts_path]:
        if not os.path.exists(p):
            raise FileNotFoundError(
                f"File not found: {p}\n"
                f"Đảm bảo CSV files nằm trong thư mục: {data_dir}"
            )

    orders   = pd.read_csv(orders_path)
    train    = pd.read_csv(train_path)
    products = pd.read_csv(products_path)
    depts    = pd.read_csv(depts_path)

    # Chỉ lấy đơn hàng của tập train (đơn cuối cùng mỗi user)
    orders_train = orders[orders["eval_set"] == "train"][["order_id", "user_id", "order_hour_of_day", "order_dow"]]

    # Join tất cả
    merged = train \
        .merge(orders_train, on="order_id") \
        .merge(products[["product_id", "product_name", "department_id"]], on="product_id") \
        .merge(depts, on="department_id")

    print(f"  ✓ Loaded {len(merged):,} order-product events")
    print(f"  ✓ {orders_train['user_id'].nunique():,} unique users")
    print(f"  ✓ {merged['product_id'].nunique():,} unique products")
    return merged

# ── Delivery report callback ──────────────────────────────────
def delivery_report(err, msg):
    if err is not None:
        print(f"  ✗ Delivery failed for {msg.key()}: {err}")

# ── Main producer loop ────────────────────────────────────────
def produce_events(df: pd.DataFrame, producer: Producer, topic: str, rate: float):
    """
    Shuffle dataframe và gửi từng row như 1 event lên Kafka.
    Mỗi event là 1 JSON message với schema:
    {
        "order_id": int,
        "user_id": int,
        "product_id": int,
        "product_name": str,
        "department": str,
        "reordered": int,   (0 hoặc 1)
        "event_timestamp": str  (ISO 8601)
    }
    """
    delay = 1.0 / rate
    df_shuffled = df.sample(frac=1, random_state=42).reset_index(drop=True)

    print(f"\nStarting to produce {len(df_shuffled):,} events at {rate}/s...")
    print("Press Ctrl+C to stop.\n")

    sent   = 0
    errors = 0

    try:
        for _, row in df_shuffled.iterrows():
            event = {
                "order_id"       : int(row["order_id"]),
                "user_id"        : int(row["user_id"]),
                "product_id"     : int(row["product_id"]),
                "product_name"   : str(row["product_name"]),
                "department"     : str(row["department"]),
                "reordered"      : int(row["reordered"]),
                "event_timestamp": datetime.now(timezone.utc).isoformat(),
            }

            # Dùng user_id làm Kafka key → cùng user sẽ vào cùng partition
            producer.produce(
                topic=topic,
                key=str(event["user_id"]),
                value=json.dumps(event),
                callback=delivery_report,
            )
            producer.poll(0)   # non-blocking flush buffer
            sent += 1

            if sent % 100 == 0:
                producer.flush()  # đảm bảo buffer không đầy
                ts = datetime.now().strftime("%H:%M:%S")
                print(f"  [{ts}] Sent {sent:,} events | Last: user={event['user_id']}, "
                      f"product={event['product_name'][:30]}")

            time.sleep(delay)

    except KeyboardInterrupt:
        print(f"\n⚡ Interrupted by user after {sent:,} events")
    finally:
        producer.flush()
        print(f"\n{'═'*50}")
        print(f"  Producer finished: {sent:,} events sent, {errors} errors")
        print(f"  Topic: {topic} @ {KAFKA_BOOTSTRAP}")
        print(f"{'═'*50}")


# ── Entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    # 1. Đảm bảo topic tồn tại
    ensure_topic(KAFKA_BOOTSTRAP, TOPIC_NAME)

    # 2. Load data
    df = load_data(DATA_DIR)

    # 3. Khởi tạo Producer
    producer_config = {
        "bootstrap.servers"         : KAFKA_BOOTSTRAP,
        "acks"                      : "all",         # đợi tất cả replicas confirm
        "compression.type"          : "lz4",         # nén để tăng throughput
        "linger.ms"                 : 5,             # gom batch nhỏ
        "batch.size"                : 16384,
        "queue.buffering.max.messages": 100000,
    }

    producer = Producer(producer_config)
    print(f"\nProducer initialized ✓")

    # 4. Produce!
    produce_events(df, producer, TOPIC_NAME, EVENTS_PER_SEC)
