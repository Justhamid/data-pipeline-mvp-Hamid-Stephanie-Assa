from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:
    message = {"artist": "Daft Punk", "track": "Get Lucky"}
    producer.send("deezer_tracks", message)
    print("✅ Message envoyé à Kafka:", message)
    time.sleep(5)
