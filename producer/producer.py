from kafka import KafkaProducer
import json, time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5
)

producer.send("test-topic-kraft", value={"msg": "hello-kraft"})
producer.flush()
print("sent")
