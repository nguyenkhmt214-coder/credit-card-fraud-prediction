# fraud_data_gen.py
# =========================
# Bộ hàm sinh data cho 4 topic: user_profile, card_account, merchant_profile, card_txn_auth
# MỤC TIÊU:
# - KHÔNG có field nào trùng với flat table ban đầu
# - Các field phải "dơ", đổi tên, chia nhỏ, để downstream Spark phải JOIN + FEATURE ENGINEERING
# - Code giữ nguyên toàn bộ comment giải thích để dễ maintain
# =========================

import uuid
import random
from datetime import datetime, timezone

# =========================
# Static "dimension" data
# (giống như location_dim / city_dim)
# =========================

CITY_MASTER = [
    {
        "location_id": "VN-HCM-01",
        "city_name": "Ho Chi Minh City",
        "country_code": "VN",
        "region_code": "HCM",
        "postal_code": "700000",
        "base_latitude": 10.7769,
        "base_longitude": 106.7009,
        "population_size": 9000000,
    },
    {
        "location_id": "VN-HN-01",
        "city_name": "Ha Noi",
        "country_code": "VN",
        "region_code": "HN",
        "postal_code": "100000",
        "base_latitude": 21.0278,
        "base_longitude": 105.8342,
        "population_size": 8000000,
    },
    {
        "location_id": "VN-DN-01",
        "city_name": "Da Nang",
        "country_code": "VN",
        "region_code": "DN",
        "postal_code": "550000",
        "base_latitude": 16.0544,
        "base_longitude": 108.2022,
        "population_size": 1200000,
    },
]

MERCHANT_CATEGORIES = ["ELEC", "FOOD", "TRVL", "FASH", "GROC", "SERV"]
JOBS = ["software_engineer","data_scientist","accountant","teacher","doctor","sales_rep"]
GENDERS = ["M", "F"]
CURRENCIES = ["VND"]

# =========================
# Helper functions
# =========================

def random_city():
    """Chọn một city_dim record bất kỳ (giả lập dimension table)."""
    return random.choice(CITY_MASTER)

def random_birthdate():
    """Sinh ngày sinh đơn giản trong khoảng 1970-2000."""
    year = random.randint(1970, 2000)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return datetime(year, month, day, tzinfo=timezone.utc)

def random_vietnamese_name():
    """
    Sinh tên kiểu Việt rất đơn giản.
    Thực tế có thể dùng list lớn hơn hoặc Faker.
    """
    first_names = ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Vo", "Dang"]
    middle_names = ["Van", "Thi", "Thanh", "Duc", "Minh", "Ngoc"]
    last_names = ["Anh", "Binh", "Hoa", "Tuan", "Linh", "Hung", "Trang"]

    family_name = random.choice(first_names)
    given_name = f"{random.choice(middle_names)} {random.choice(last_names)}"
    full_name = f"{family_name} {given_name}"
    return family_name, given_name, full_name

def random_amount_minor():
    """
    Sinh số tiền theo distribution giống code JS:
    - nhiều giao dịch nhỏ, ít giao dịch lớn
    Trả về đơn vị 'minor' (VND * 100) -> integer.
    """
    r = random.random()
    if r < 0.7:
        amt = random.random() * 50
    elif r < 0.95:
        amt = 50 + random.random() * 450
    else:
        amt = 500 + random.random() * 4500
    return int(round(amt * 100))

# =========================
# Topic 1: user_profile
# =========================

def gen_user_profile_event():
    """
    Giả lập record cho topic: user_profile
    Các field tương ứng từ flat table cũ được CHIA NHỎ / ĐỔI TÊN:
    - không có first, last → dùng given_name, family_name, full_name
    - dob → birth_date + birth_year
    - city, state, zip, lat, long, city_pop → JOIN qua CITY_MASTER bằng location_id_home
    - cc_num hoàn toàn không tồn tại → thay bằng party_id
    """
    now = datetime.now(timezone.utc)
    city_dim = random_city()

    family, given, full = random_vietnamese_name()
    birth_dt = random_birthdate()

    party_id = f"party_{100000 + random.randint(0, 899999)}"

    event = {
        "event_id": str(uuid.uuid4()),
        "event_ts": now.isoformat(),

        # business key thay cho cc_num
        "party_id": party_id,

        # tên tách nhỏ 
        "given_name": given,
        "family_name": family,
        "full_name": full,

        "gender_code": random.choice(GENDERS),

        # dob chia nhỏ
        "birth_date": birth_dt.date().isoformat(),
        "birth_year": birth_dt.year,

        "occupation_title": random.choice(JOBS),

        # địa chỉ mapping ngược từ city dim
        "address_line": "Some random street",
        "region_code": city_dim["region_code"],
        "home_postal_hint": city_dim["postal_code"],
        "location_id_home": city_dim["location_id"],
    }

    return event


# =========================
# Topic 2: card_account
# =========================

def gen_card_account_event(party_id, location_id_home):
    """
    Giả lập topic card_account
    - Không chứa cc_num
    - Dùng card_ref + card_pan_hash làm key JOIN sang transaction
    """
    now = datetime.now(timezone.utc)

    # số PAN giả → hash
    pan_num = str(4000000000000000 + random.randint(0, 999999999999999))
    card_ref = f"card_{uuid.uuid4().hex[:12]}"

    event = {
        "event_id": str(uuid.uuid4()),
        "event_ts": now.isoformat(),

        "party_id_fk": party_id,            # join sang user_profile
        "card_ref": card_ref,               # join sang txn
        "card_pan_hash": f"h_{hash(pan_num) & 0xffffffff:x}",
        "card_pan_last4": pan_num[-4:],     # giống cc_num cuối 4 số

        "billing_location_id": location_id_home,

        "product_type": random.choice(["CREDIT","DEBIT"]),
        "brand": random.choice(["VISA","MASTERCARD","JCB"]),
    }
    return event


# =========================
# Topic 3: merchant_profile
# =========================

def gen_merchant_profile_event():
    """
    Merchant profile:
    - không dùng merchant, merch_lat, merch_long
    - đổi thành merchant_id, merchant_display_name, geo_lat/geo_lon
    """
    now = datetime.now(timezone.utc)
    city_dim = random_city()

    merchant_id = f"m_{1000 + random.randint(0,9999)}"

    # location lệch nhẹ quanh city center
    geo_lat = city_dim["base_latitude"] + (random.random() - 0.5) * 0.02
    geo_lon = city_dim["base_longitude"] + (random.random() - 0.5) * 0.02

    event = {
        "event_id": str(uuid.uuid4()),
        "event_ts": now.isoformat(),

        "merchant_id": merchant_id,
        "merchant_display_name": f"Merchant #{random.randint(1,9999)}",

        "mcc_group": random.choice(MERCHANT_CATEGORIES),

        # lat/long mới
        "geo_lat": geo_lat,
        "geo_lon": geo_lon,

        "location_id_operate": city_dim["location_id"],
    }
    return event


# =========================
# Topic 4: card_txn_auth
# =========================

def gen_card_txn_auth_event(card_account, merchant_profile):
    """
    Main transaction stream:
    - Không có amt float → amount_minor + amount_sign
    - Không có city/lat/long → JOIN merchant_profile + user_profile
    - Không có trans_num → auth_code
    """
    now = datetime.now(timezone.utc)

    amount_minor = random_amount_minor()
    amount_sign = random.choice([1, 1, 1, 1, -1])  # đa số dương

    fraud_flag = 1 if random.random() < 0.005 else 0
    auth_code = f"AUTH-{random.randint(100000, 99999999)}"

    event = {
        "event_id": str(uuid.uuid4()),
        "event_ts": now.isoformat(),
        "event_epoch_sec": int(now.timestamp()),

        # JOIN keys:
        "card_ref": card_account["card_ref"],
        "card_pan_hash": card_account["card_pan_hash"],
        "merchant_ref": merchant_profile["merchant_id"],

        # amount kiểu mới
        "amount_minor": amount_minor,
        "amount_sign": amount_sign,
        "currency": "VND",

        "channel_code": random.choice(["POS","ECOM","MOBILE"]),
        "entry_mode": random.choice(["CHIP","MAGSTRIPE","CONTACTLESS","MANUAL"]),

        "auth_code": auth_code,

        "fraud_flag": fraud_flag,
        "risk_score_online": round(random.uniform(0, 1), 3),
    }

    return event


# =========================
# Orchestration: tạo 1 bundle đầy đủ 4 topic
# =========================

def generate_bundle():
    """
    Sinh 1 bộ event:
    1 user_profile
    1 card_account (liên kết user)
    1 merchant_profile
    1 card_txn_auth (liên kết card + merchant)

    Mục đích:
    - Producer sẽ gọi từng event và gửi vào đúng Kafka topic
    """
    user_ev = gen_user_profile_event()
    card_ev = gen_card_account_event(
        party_id=user_ev["party_id"],
        location_id_home=user_ev["location_id_home"]
    )
    merchant_ev = gen_merchant_profile_event()
    txn_ev = gen_card_txn_auth_event(card_ev, merchant_ev)

    return {
        "user_profile": user_ev,
        "card_account": card_ev,
        "merchant_profile": merchant_ev,
        "card_txn_auth": txn_ev,
    }
