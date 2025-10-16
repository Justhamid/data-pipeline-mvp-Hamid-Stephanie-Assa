from kafka import KafkaConsumer
import json

# ---------------------------
# CONFIG KAFKA
# ---------------------------
KAFKA_SERVER = "kafka:9092"
TOPICS = ["artists", "albums", "tracks"]

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset='earliest',  # lire depuis le début
    group_id='debug_consumer',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("👀 En attente des messages sur les topics :", TOPICS)

for message in consumer:
    topic = message.topic
    data = message.value
    print(f"\n📌 Topic: {topic}")
    for k, v in data.items():
        print(f"  {k}: {v}")
