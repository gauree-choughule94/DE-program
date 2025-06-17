# from event_consumers.posthog_event_consumer import capture_event as send_posthog_event
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import json
import requests
import pandas as pd
import pyarrow.parquet as pq
import logging

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# PostHog config
POSTHOG_API_KEY = "phc_HqW5rE3RsehAk7gGYV12V4dZKlmcFwAHpdAsJI5flmW"
# POSTHOG_API_URL = "http://posthog:8000/capture/"                 
POSTHOG_API_URL = "https://us.i.posthog.com/capture/"  # ✅ PostHog Cloud endpoint

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def send_posthog_event(event_name, properties):
    if "distinct_id" not in properties:
        raise ValueError(f"'distinct_id' is required in properties for event '{event_name}'")
    
    payload = {
        "distinct_id": properties["distinct_id"],
        "api_key": POSTHOG_API_KEY,
        "event": event_name,
        "properties": properties,
    }
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.post(POSTHOG_API_URL, data=json.dumps(payload), headers=headers)
        response.raise_for_status()
        logger.info(f"Event '{event_name}' sent for user: {properties.get('distinct_id')}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send event '{event_name}': {e}")
        if e.response is not None:
            logger.error(f"Response content: {e.response.text}")

# execution_date = "{{ ds }}"
# input_path = f"/opt/data_lake/gold/customers_summary/report_date={execution_date}"
def emit_customer_events():
    df = pq.read_table("/opt/data_lake/gold/customers_summary/report_date=2025-06-16").to_pandas()
    if df.empty:
        raise ValueError("The customer_DataFrame is empty. No data to emit.")
    logger.info(f"Customer DataFrame columns: {df.columns.tolist()}")
    logger.info(f"Customer sample rows:\n{df.head()}")
    for _, row in df.iterrows():
        send_posthog_event("new_customer", {
            "distinct_id": row["address"],  # Using address as distinct identifier since no customer_id exists
            "address": row["address"],
            "total_customers": int(row["total_customers"]),
            # "report_date": str(row["report_date"]),
        })

def emit_product_events():
    df = pq.read_table("/opt/data_lake/gold/products_summary/report_date=2025-06-16").to_pandas()
    if df.empty:
        raise ValueError("The product_DataFrame is empty. No data to emit.")
    logger.info(f"Product DataFrame columns: {df.columns.tolist()}")
    logger.info(f"Product sample rows:\n{df.head()}")
    for _, row in df.iterrows():
        send_posthog_event("new_product_added", {
            "distinct_id": f"category_{row['category']}",   # not valid => Event 'new_product_added' sent for user: None 
            "category": row["category"],
            "total_products": int(row["total_products"]),
            "max_price": float(row["max_price"]),
            # "report_date": str(row["report_date"]),
            "avg_price": float(row["avg_price"])
        })

def emit_order_events():
    df = pq.read_table("/opt/data_lake/gold/orders_summary/report_date=2025-06-16").to_pandas()
    if df.empty:
        raise ValueError("The order_DataFrame is empty. No data to emit.")
    logger.info(f"Order DataFrame columns: {df.columns.tolist()}")
    logger.info(f"Order sample rows:\n{df.head()}")
    for _, row in df.iterrows():
        send_posthog_event("order_summary_generated", {
            "distinct_id": row["customer_id"],
            "customer_id": row["customer_id"],
            "total_orders": int(row["total_orders"]),
            "total_spent": float(row["total_spent"]),
            "rank_by_spend": int(row["rank_by_spend"]),
            # "report_date": str(row["report_date"]),
        })

with DAG(
    dag_id='posthog_event_tracking_from_gold',
    default_args=default_args,
    description='Track customer/product/order analytics from gold layer to PostHog',
    schedule_interval="@daily",
    catchup=False,
    tags=["posthog", "gold", "analytics"],
) as dag:

    emit_customers = PythonOperator(
        task_id='emit_customers',
        python_callable=emit_customer_events
    )

    emit_products = PythonOperator(
        task_id='emit_products',
        python_callable=emit_product_events
    )

    emit_orders = PythonOperator(
        task_id='emit_orders',
        python_callable=emit_order_events
    )

    emit_customers >> emit_products >> emit_orders
