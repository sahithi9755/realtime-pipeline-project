import json
import time
import random
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',  # From your PC → Docker Kafka
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

pages = ["/home", "/products", "/cart", "/checkout"]
event_types = ["page_view", "click", "session_start", "session_end"]

print("Starting producer... Press Ctrl+C to stop.")

while True:
    event = {
        # Proper UTC timestamp for Spark
        "event_time": (datetime.now(timezone.utc) - timedelta(seconds=random.randint(0, 180))).isoformat(),
        "user_id": f"user_{random.randint(1,20)}",
        "page_url": random.choice(pages),
        "event_type": random.choice(event_types)
    }

    producer.send("user_activity", event)
    producer.flush()  # ensures message is sent immediately

    print("Sent:", event)
    time.sleep(1)
