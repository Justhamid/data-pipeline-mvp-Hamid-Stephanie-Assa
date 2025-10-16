from kafka import KafkaConsumer
import json

# Configuration Kafka
KAFKA_TOPIC = "artists"
KAFKA_SERVER = "kafka:9092"  # parce qu'on est dans le container app

# Initialisation du consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset='earliest',  # lire depuis le début du topic
    group_id='consumer-test',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print(f"En attente des messages sur le topic '{KAFKA_TOPIC}'…")

for message in consumer:
    print("Reçu :", message.value)
