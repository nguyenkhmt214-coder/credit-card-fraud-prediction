# kafka_consumer_app.py
# =========================
# Consumer đọc 4 topic và in ra màn hình
# - Dùng confluent-kafka
# - Subcribe nhiều topic cùng lúc
# - In payload JSON + topic + key
# =========================

from confluent_kafka import Consumer
import json

# =========================
# Kafka Consumer config
# =========================
consumer_conf = {
    # DÙNG EXTERNAL PORT (HOST)
    "bootstrap.servers": "localhost:29092,localhost:29094",

    "group.id": "fraud-sim-consumer-group",
    "auto.offset.reset": "latest",
}

consumer = Consumer(consumer_conf)

# đăng ký 4 topic
topics = ["user_profile", "card_account", "merchant_profile", "card_txn_auth"]
consumer.subscribe(topics)

print("Consumer started. Listening...")

# =========================
# Loop nhận message
# =========================
while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        continue

    if msg.error():
        print("Consumer error:", msg.error())
        continue

    try:
        payload = json.loads(msg.value().decode("utf-8"))
    except:
        payload = msg.value()

    print("\n============================")
    print(f"TOPIC      : {msg.topic()}")
    print(f"PARTITION  : {msg.partition()}")
    print(f"KEY        : {msg.key().decode('utf-8') if msg.key() else None}")
    print("VALUE:")
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    print("============================")
