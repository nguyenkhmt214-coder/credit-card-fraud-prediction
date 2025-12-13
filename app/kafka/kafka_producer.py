# kafka_producer_app.py
# =========================
# App producer Kafka with debug logging
# =========================

from confluent_kafka import Producer
import json
import time
import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from utils.generator import generate_bundle


# =========================
# Kafka Producer config
# =========================
producer_conf = {
    "bootstrap.servers": "localhost:29092,localhost:29093",
    "client.id": "fraud-simulator",
}
producer = Producer(producer_conf)


def send_to_kafka(topic, value, key=None):
    """
    Send message to Kafka with debug print.
    """
    payload = json.dumps(value).encode("utf-8")

    print("\n----------------------------------")
    print(f"[PRODUCER] Sending to topic: {topic}")
    print(f"[PRODUCER] Key: {key}")
    print("[PRODUCER] Value JSON:")
    print(json.dumps(value, indent=2))
    print("----------------------------------\n")

    producer.produce(
        topic=topic,
        key=(key.encode("utf-8") if key else None),
        value=payload
    )
    producer.flush(0)


# =========================
# Main loop — push data with faster speeds
# =========================

if __name__ == "__main__":

    # Counters to control send frequency
    tick = 0

    while True:
        bundle = generate_bundle()

        print("\n========== GENERATED BUNDLE ==========")
        print(json.dumps(bundle, indent=2))
        print("======================================\n")

        # ------------------------------------------
        # 1) USER_PROFILE → ~mỗi 0.2 giây
        # ------------------------------------------
        if tick % 1 == 0:
            send_to_kafka(
                "user_profile",
                bundle["user_profile"],
                key=bundle["user_profile"]["party_id"]
            )
            print(">>> Sent → user_profile")

        # ------------------------------------------
        # 2) MERCHANT_PROFILE → ~mỗi 0.6 giây
        # ------------------------------------------
        if tick % 3 == 0:
            send_to_kafka(
                "merchant_profile",
                bundle["merchant_profile"],
                key=bundle["merchant_profile"]["merchant_id"]
            )
            print(">>> Sent → merchant_profile")

        # ------------------------------------------
        # 3) CARD_ACCOUNT → ~mỗi 1.0 giây
        # ------------------------------------------
        if tick % 5 == 0:
            send_to_kafka(
                "card_account",
                bundle["card_account"],
                key=bundle["card_account"]["card_ref"]
            )
            print(">>> Sent → card_account")

        # ------------------------------------------
        # 4) CARD_TXN_AUTH → ~mỗi 1.6 giây (chậm nhất)
        # ------------------------------------------
        if tick % 8 == 0:
            send_to_kafka(
                "card_txn_auth",
                bundle["card_txn_auth"],
                key=bundle["card_txn_auth"]["card_ref"]
            )
            print(">>> Sent → card_txn_auth (slowest)")

        print(f">>> Completed tick {tick}\n")

        tick += 1
        time.sleep(0.2)    # 1 tick ~ 0.2 giây → nhanh hơn


# Tick duration: 0.2s
# | Topic            | Tốc độ gửi       | Giải thích                      |
# | ---------------- | ---------------- | ------------------------------- |
# | user_profile     | ~mỗi 0.2 giây    | nhanh nhất                      |
# | merchant_profile | ~mỗi 0.6 giây    | trung bình                      |
# | card_account     | ~mỗi 1.0 giây    | chậm hơn                        |
# | card_txn_auth    | ~mỗi 1.6 giây    | **chậm nhất**                   |
