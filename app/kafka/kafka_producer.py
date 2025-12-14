# kafka_producer_app.py
# =========================
# App producer Kafka with debug logging & data validation
# Includes enhanced data quality checks and feature engineering display
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


# =========================
# Data Validation Functions
# =========================

def validate_record(record_type: str, data: dict) -> bool:
    """
    Validate required fields untuk mỗi record type
    """
    required_fields = {
        "user_profile": ["party_id", "event_id", "event_ts", "birth_date"],
        "card_account": ["card_ref", "party_id_fk", "event_id"],
        "merchant_profile": ["merchant_id", "event_id", "geo_lat", "geo_lon"],
        "card_txn_auth": ["event_id", "card_ref", "merchant_ref", "amount_minor"],
    }
    
    required = required_fields.get(record_type, [])
    for field in required:
        if field not in data or data[field] is None:
            print(f"❌ [VALIDATION ERROR] {record_type}: Missing required field '{field}'")
            return False
    
    return True

def print_feature_engineering_summary(bundle: dict):
    """
    Hiển thị các engineered features cho Spark
    """
    print("\n" + "="*70)
    print("🔧 FEATURE ENGINEERING SUMMARY (cho Spark Transform)")
    print("="*70)
    
    user = bundle["user_profile"]
    card = bundle["card_account"]
    merch = bundle["merchant_profile"]
    txn = bundle["card_txn_auth"]
    
    print(f"\n📊 USER FEATURES:")
    print(f"  • party_id: {user.get('party_id', 'N/A')}")
    print(f"  • age: {user.get('age', 'N/A')} years (group: {user.get('age_group', 'N/A')})")
    print(f"  • income_level: {user.get('income_level', 'N/A')}")
    print(f"  • home_location: ({user.get('home_latitude', 'N/A')}, {user.get('home_longitude', 'N/A')})")
    
    print(f"\n💳 CARD FEATURES:")
    print(f"  • card_ref: {card.get('card_ref', 'N/A')}")
    print(f"  • days_since_issuance: {card.get('days_since_issuance', 'N/A')} days")
    print(f"  • card_age_category: {card.get('card_age_category', 'N/A')}")
    print(f"  • is_primary_card: {card.get('is_primary_card', 'N/A')}")
    print(f"  • daily_limit: {card.get('daily_limit_minor', 'N/A')} (minor units)")
    
    print(f"\n🏪 MERCHANT FEATURES:")
    print(f"  • merchant_id: {merch.get('merchant_id', 'N/A')}")
    print(f"  • mcc_group: {merch.get('mcc_group', 'N/A')}")
    print(f"  • merchant_risk_score: {merch.get('risk_score_merchant', 'N/A')}")
    print(f"  • merchant_type: {merch.get('merchant_type', 'N/A')}")
    print(f"  • is_high_risk_merchant: {merch.get('is_high_risk', 'N/A')}")
    print(f"  • merchant_location: ({merch.get('geo_lat', 'N/A')}, {merch.get('geo_lon', 'N/A')})")
    
    print(f"\n💰 TRANSACTION FEATURES:")
    print(f"  • event_id: {txn.get('event_id', 'N/A')}")
    print(f"  • amount_minor: {txn.get('amount_minor', 'N/A')} (amount_major: {txn.get('amount_major', 'N/A')} VND)")
    print(f"  • amount_category: {txn.get('amount_category', 'N/A')}")
    print(f"  • hour_of_day: {txn.get('hour_of_day', 'N/A')}:00")
    print(f"  • is_unusual_hour: {txn.get('is_unusual_hour', 'N/A')} ⚠️" if txn.get('is_unusual_hour') == 1 else f"  • is_unusual_hour: {txn.get('is_unusual_hour', 'N/A')}")
    print(f"  • distance_from_home_km: {txn.get('distance_from_home_km', 'N/A')} km")
    print(f"  • exceeds_daily_limit: {txn.get('exceeds_daily_limit', 'N/A')}")
    print(f"  • amount_deviation_ratio: {txn.get('amount_deviation_ratio', 'N/A')}")
    print(f"  • txn_fraud_probability: {txn.get('txn_fraud_probability', 'N/A')} 🚨")
    print(f"  • fraud_flag: {txn.get('fraud_flag', 'N/A')}")
    print(f"  • channel_code: {txn.get('channel_code', 'N/A')}")
    
    print("\n" + "="*70 + "\n")


def send_to_kafka(topic, value, key=None):
    """
    Send message to Kafka with debug print.
    Validates data before sending.
    """
    # Validate before sending
    if not validate_record(topic, value):
        print(f"⚠️  Skipping invalid record for topic: {topic}")
        return False
    
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
    return True


# =========================
# Main loop — push data with faster speeds
# =========================

if __name__ == "__main__":

    # Counters to control send frequency
    tick = 0
    
    print("\n🚀 Starting Kafka Producer with Feature Engineering...")
    print("Sending enriched data for Spark transformations\n")

    while True:
        bundle = generate_bundle()

        print("\n========== GENERATED BUNDLE ==========")
        print(json.dumps(bundle, indent=2))
        print("======================================\n")
        
        # Display feature engineering summary
        print_feature_engineering_summary(bundle)

        # ------------------------------------------
        # 1) USER_PROFILE → ~mỗi 0.2 giây
        # ------------------------------------------
        if tick % 1 == 0:
            if send_to_kafka(
                "user_profile",
                bundle["user_profile"],
                key=bundle["user_profile"]["party_id"]
            ):
                print("✅ Sent → user_profile")

        # ------------------------------------------
        # 2) MERCHANT_PROFILE → ~mỗi 0.6 giây
        # ------------------------------------------
        if tick % 3 == 0:
            if send_to_kafka(
                "merchant_profile",
                bundle["merchant_profile"],
                key=bundle["merchant_profile"]["merchant_id"]
            ):
                print("✅ Sent → merchant_profile")

        # ------------------------------------------
        # 3) CARD_ACCOUNT → ~mỗi 1.0 giây
        # ------------------------------------------
        if tick % 5 == 0:
            if send_to_kafka(
                "card_account",
                bundle["card_account"],
                key=bundle["card_account"]["card_ref"]
            ):
                print("✅ Sent → card_account")

        # ------------------------------------------
        # 4) CARD_TXN_AUTH → ~mỗi 1.6 giây (chậm nhất)
        # ------------------------------------------
        if tick % 8 == 0:
            if send_to_kafka(
                "card_txn_auth",
                bundle["card_txn_auth"],
                key=bundle["card_txn_auth"]["card_ref"]
            ):
                print("✅ Sent → card_txn_auth (slowest)")

        print(f">>> Completed tick {tick}\n")

        tick += 1
        time.sleep(0.2)    # 1 tick ~ 0.2 giây → nhanh hơn


# Tick duration: 0.2s
# | Topic            | Tốc độ gửi       | Giải thích                      |
# | ---------------- | ---------------- | ------------------------------- |
# | user_profile     | ~mỗi 0.2 giây    | nhanh nhất                      |
# | merchant_profile | ~mỗi 0.6 giây    | trung bình                      |
# | card_account     | ~mỗi 1.0 giây    | chậm hơn                        |
# | card_txn_auth    | ~mỗi 1.6 giây    | **chậm nhất** ⭐ (chứa features) |
