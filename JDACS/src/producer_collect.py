import json
import time
import requests
from kafka import KafkaProducer

API_URL = (
    "https://www.gdacs.org/gdacsapi/api/Events/"
    "geteventlist/SEARCH?eventlist=EQ;TC;FL"
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def collect():
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    data = response.json()

    events = data.get("features", [])
    for event in events:
        producer.send("raw_events", event)

    producer.flush()

if __name__ == "__main__":
    while True:
        collect()
        time.sleep(180)
