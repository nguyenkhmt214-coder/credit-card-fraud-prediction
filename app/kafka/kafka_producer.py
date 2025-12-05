# kafka_producer_app.py
# =========================
# App producer Kafka:
# - Import fraud_data_gen
# - Tạo Kafka producer
# - Gửi mỗi event vào topic tương ứng
# =========================

from confluent_kafka import Producer
import json
import time

from ..utils.generator import generate_bundle

# =========================
# Kafka Producer config
# =========================

producer_conf = {
    # DÙNG EXTERNAL PORT — ĐÚNG CHO APP CHẠY NGOÀI DOCKER
    "bootstrap.servers": "localhost:29092,localhost:29093",
    "client.id": "fraud-simulator",
}
producer = Producer(producer_conf)

def send_to_kafka(topic, value, key=None):
    """
    Gửi dữ liệu vào Kafka.
    value: dict → sẽ stringify thành JSON.
    key: partition key (optional)
    """
    payload = json.dumps(value).encode("utf-8")
    producer.produce(
        topic=topic,
        key=(key.encode("utf-8") if key else None),
        value=payload
    )
    producer.flush(0)

# =========================
# Main loop — push dữ liệu
# =========================

if __name__ == "__main__":
    while True:
        bundle = generate_bundle()

        send_to_kafka("user_profile", bundle["user_profile"], key=bundle["user_profile"]["party_id"])
        send_to_kafka("card_account", bundle["card_account"], key=bundle["card_account"]["card_ref"])
        send_to_kafka("merchant_profile", bundle["merchant_profile"], key=bundle["merchant_profile"]["merchant_id"])
        send_to_kafka("card_txn_auth", bundle["card_txn_auth"], key=bundle["card_txn_auth"]["card_ref"])

        print("Sent 4 events → Kafka")
        time.sleep(60)  # mỗi 200ms → 5 giao dịch/giây
