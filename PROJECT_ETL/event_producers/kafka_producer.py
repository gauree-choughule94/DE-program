# ✅ A Kafka event producer (using KafkaProducer) that reads from Parquet files and sends to customer_events, product_events, and order_events topics.

# ✅ A Kafka consumer (sending events to PostHog) that subscribes to the same topics.

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
import pyarrow.parquet as pq
from datetime import datetime, timedelta

# ---------------- Configuration ----------------
DATA_BASE_PATH = "/opt/data_lake/gold"
# REPORT_DATE = "2025-06-16"  
REPORT_DATE = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

TOPICS = {
    "customers_summary": "customer_events",
    "products_summary": "product_events",
    "orders_summary": "order_events"
}

# ---------------- Kafka Setup ----------------
admin_client = KafkaAdminClient(
    bootstrap_servers='kafka:9092',
    client_id='admin-client'
)

topics_to_create = [
    NewTopic(name=topic, num_partitions=3, replication_factor=1)
    for topic in TOPICS.values()
]

try:
    admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
    print("Kafka topics created successfully.")
except TopicAlreadyExistsError:
    print("Some or all Kafka topics already exist.")
except Exception as e:
    print(f"Error creating topics: {e}")

producer = KafkaProducer(
    # bootstrap_servers=['localhost:29092'],  
    bootstrap_servers=['kafka:9092'],  # Correct for Docker internal network
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ---------------- Kafka Emit Helpers ----------------
def send_event_to_kafka(topic, event_data):
    try:
        print(f"Sending to Kafka → {topic}: {event_data}")
        producer.send(topic, event_data)
        producer.flush()
    except Exception as e:
        print(f"Error sending to Kafka topic {topic}: {e}")


def emit_customer_events():
    path = f"{DATA_BASE_PATH}/customers_summary/report_date={REPORT_DATE}"
    df = pq.read_table(path).to_pandas()

    if df.empty:
        print("No customer data.")
        return

    for _, row in df.iterrows():
        send_event_to_kafka('customer_events', {
            'event_type': 'new_customer',
            'address': row["address"],
            'total_customers': int(row["total_customers"]),
            'timestamp': datetime.now().isoformat()
        })

def emit_product_events():
    path = f"{DATA_BASE_PATH}/products_summary/report_date={REPORT_DATE}"
    df = pq.read_table(path).to_pandas()
    if df.empty:
        print("No product data.")
        return
    for _, row in df.iterrows():
        send_event_to_kafka('product_events', {
            'event_type': 'new_product_added',
            'category': row["category"],
            'total_products': int(row["total_products"]),
            'max_price': float(row["max_price"]),
            'avg_price': float(row["avg_price"]),
            'timestamp': datetime.now().isoformat()
        })

def emit_order_events():
    path = f"{DATA_BASE_PATH}/orders_summary/report_date={REPORT_DATE}"
    df = pq.read_table(path).to_pandas()
    if df.empty:
        print("No order data.")
        return
    for _, row in df.iterrows():
        send_event_to_kafka('order_events', {
            'event_type': 'order_summary_generated',
            'customer_id': row["customer_id"],
            'total_orders': int(row["total_orders"]),
            'total_spent': float(row["total_spent"]),
            'rank_by_spend': int(row["rank_by_spend"]),
            'timestamp': datetime.now().isoformat()
        })

# ---------------- Run All ----------------
if __name__ == "__main__":
    emit_customer_events()
    emit_product_events()
    emit_order_events()
