from kafka import KafkaConsumer
import json, requests

consumer = KafkaConsumer(
    'product_events',
    'order_events',
    'customer_events',
    bootstrap_servers=['kafka:9092'],
    group_id='posthog_consumer',
    auto_offset_reset='earliest',
    enable_auto_commit=True,  # Don't auto commit
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else {},
)

POSTHOG_API_KEY = 'phc_HqW5rE3RsehAk7gGYV12V4dZKlmcFwAHpdAsJI5flmW'
POSTHOG_INSTANCE_URL = 'https://us.i.posthog.com'

def send_to_posthog_capture(event_data):
    event_type = event_data.get("event_type", "unknown_event")

    # Assign meaningful distinct_id based on event type
    if event_type == "new_customer":
        event_data["distinct_id"] = event_data.get("address", "unknown_customer")

    elif event_type == "new_product_added":
        event_data["distinct_id"] = str(event_data.get("max_price", "unknown_product"))

    elif event_type == "order_summary_generated":
        event_data["distinct_id"] = str(event_data.get("customer_id", "unknown_order"))

    else:
        # Default for any unknown event type
        event_data["distinct_id"] = "system"

    distinct_id = event_data.get("distinct_id")
    if not distinct_id:
        print(f"Skipping event, missing distinct_id: {event_data}")
        return
    
    payload = {
        "api_key": POSTHOG_API_KEY,
        "event": event_type,
        "properties": {
            "distinct_id": distinct_id,
            **event_data
        },
    }

    try:
        response = requests.post(f"{POSTHOG_INSTANCE_URL}/capture/", json=payload)
        response.raise_for_status()
        print(f"Sent: {event_type} for {distinct_id}")
    except requests.RequestException as e:
        print(f" Failed: {event_type} for {distinct_id}, Error: {e}, Response: {e.response.text if e.response else 'N/A'}")
        raise

while True:
    try:
        message_batch = consumer.poll(timeout_ms=1000)
        for topic_partition, messages in message_batch.items():
            for message in messages:
                print(f"Received: {message.value} from {message.topic}")
                try:
                    send_to_posthog_capture(message.value)
                    # Acknowledge this message
                    consumer.commit({topic_partition: message.offset + 1})
                    print("Offset committed.")
                except Exception as e:
                    print(f" Processing failed for offset {message.offset}: {e}")
    except Exception as err:
        print(f"Consumer error: {err}")
