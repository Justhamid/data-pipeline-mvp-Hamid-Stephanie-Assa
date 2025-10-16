from kafka import KafkaConsumer
import json, time

c = KafkaConsumer(
    "test-topic-kraft",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    consumer_timeout_ms=10000,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for msg in c:
    print("received:", msg.value)
