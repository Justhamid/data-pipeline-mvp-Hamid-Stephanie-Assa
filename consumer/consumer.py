from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'deezer_tracks',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("🎧 En attente de messages...")
for message in consumer:
    print("📩 Reçu:", message.value)
