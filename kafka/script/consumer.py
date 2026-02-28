from kafka import KafkaConsumer
import json

TOPIC = "temperature_raw"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",   # biar baca dari awal kalau belum ada commit
    enable_auto_commit=True,
    group_id="heat-consumer-test",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print(f"Listening on topic: {TOPIC} ...")

for msg in consumer:
    print("KEY:", msg.key.decode("utf-8") if msg.key else None)
    print("VALUE:", msg.value)
    print("-" * 40)