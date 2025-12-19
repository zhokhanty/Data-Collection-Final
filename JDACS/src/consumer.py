import json
from kafka import KafkaConsumer

from src.clean_transform import clean_event
from src.store_db import init_db, insert_event

consumer = KafkaConsumer(
    "raw_events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

def process_batch():
    init_db()
    for msg in consumer:
        cleaned = clean_event(msg.value)
        if cleaned:
            insert_event(cleaned)

if __name__ == "__main__":
    process_batch()
