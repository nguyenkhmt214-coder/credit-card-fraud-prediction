import redis
import pandas as pd
import random
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# ============================================================
# CONFIG
# ============================================================
KAFKA_SERVERS = "kafka-1:9092,kafka-2:9092"
MINIO_ENDPOINT = "http://minio:9000"
REDIS_HOST = "redis"
TS_RETENTION_MS = "600000"

# ============================================================
# 🔥 SPARK SESSION (dùng lại config cũ, NOT TOUCH)
# ============================================================
spark = (
    SparkSession.builder
        .appName("All_Streams")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")

        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# METRICS UTILITIES (Grafana)
# ============================================================
_ts_ready = False

def ensure_timeseries(r: redis.Redis):
    """Create Redis time series keys used by Grafana if they do not exist."""
    series = [
        "ts:txn:count",
        "ts:txn:fraud",
        "ts:txn:pred",
        "ts:txn:amount_minor",
    ]
    for key in series:
        try:
            r.execute_command("TS.CREATE", key, "RETENTION", TS_RETENTION_MS)
        except Exception:
            # Ignore if the series already exists
            pass

# ============================================================
# USER STREAM
# ============================================================

CITY_MASTER = {
    "VN-HCM-01": {"city": "Ho Chi Minh City", "lat": 10.7769, "long": 106.7009, "pop": 9000000},
    "VN-HN-01":  {"city": "Ha Noi", "lat": 21.0278, "long": 105.8342, "pop": 8000000},
    "VN-DN-01":  {"city": "Da Nang", "lat": 16.0544, "long": 108.2022, "pop": 1200000},
}

user_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_ts", StringType()),
    StructField("party_id", StringType()),
    StructField("given_name", StringType()),
    StructField("family_name", StringType()),
    StructField("full_name", StringType()),
    StructField("gender_code", StringType()),
    StructField("birth_date", StringType()),
    StructField("birth_year", IntegerType()),
    StructField("occupation_title", StringType()),
    StructField("address_line", StringType()),
    StructField("region_code", StringType()),
    StructField("home_postal_hint", StringType()),
    StructField("location_id_home", StringType()),
])

user_df = (
    spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "user_profile")
        .option("startingOffsets", "latest")
        .load()
        .select(from_json(col("value").cast("string"), user_schema).alias("data"))
        .select("data.*")
)

def write_user(batch_df, batch_id):
    pdf = batch_df.toPandas()
    r = redis.Redis(host=REDIS_HOST, decode_responses=True)
    pipe = r.pipeline()

    for _, row in pdf.iterrows():
        pid = row["party_id"]
        if not pid:
            continue

        city = CITY_MASTER.get(row["location_id_home"], {})

        pipe.hset(f"u:{pid}", mapping={
            "first": row["given_name"],
            "last": row["family_name"],
            "gender": row["gender_code"],
            "street": row["address_line"],
            "city": city.get("city", "Unknown"),
            "state": row["region_code"],
            "zip": row["home_postal_hint"],
            "lat": city.get("lat", 0.0),
            "long": city.get("long", 0.0),
            "city_pop": city.get("pop", 0),
            "job": row["occupation_title"],
            "dob": row["birth_date"]
        })

    pipe.execute()
    print(f"[USER] Batch {batch_id} → {len(pdf)} rows")

user_q = (
    user_df.writeStream
        .foreachBatch(write_user)
        .outputMode("append")
        .option("checkpointLocation", "s3a://fraud/checkpoints/user_redis/")
        .start()
)

# ============================================================
# MERCHANT STREAM
# ============================================================

merch_schema = StructType([
    StructField("event_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("merchant_display_name", StringType()),
    StructField("mcc_group", StringType()),
    StructField("geo_lat", DoubleType()),
    StructField("geo_lon", DoubleType()),
])

merch_df = (
    spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "merchant_profile")
        .option("startingOffsets", "latest")
        .load()
        .select(from_json(col("value").cast("string"), merch_schema).alias("data"))
        .select("data.*")
)

def write_merchant(batch_df, batch_id):
    pdf = batch_df.toPandas()
    r = redis.Redis(host=REDIS_HOST, decode_responses=True)
    pipe = r.pipeline()

    for _, row in pdf.iterrows():
        mid = row["merchant_id"]
        if not mid:
            continue

        pipe.hset(f"m:{mid}", mapping={
            "merchant": row["merchant_display_name"],
            "category": row["mcc_group"],
            "merch_lat": row["geo_lat"],
            "merch_long": row["geo_lon"],
        })

    pipe.execute()
    print(f"[MERCHANT] Batch {batch_id} → {len(pdf)} rows")

merch_q = (
    merch_df.writeStream
        .foreachBatch(write_merchant)
        .outputMode("append")
        .option("checkpointLocation", "s3a://fraud/checkpoints/merchant_redis/")
        .start()
)

# ============================================================
# CARD STREAM
# ============================================================

card_schema = StructType([
    StructField("event_id", StringType()),
    StructField("party_id_fk", StringType()),
    StructField("card_ref", StringType()),
    StructField("card_pan_last4", StringType()),
])

card_df = (
    spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "card_account")
        .option("startingOffsets", "latest")
        .load()
        .select(from_json(col("value").cast("string"), card_schema).alias("data"))
        .select("data.*")
)

def write_card(batch_df, batch_id):
    pdf = batch_df.toPandas()
    r = redis.Redis(host=REDIS_HOST, decode_responses=True)
    pipe = r.pipeline()

    for _, row in pdf.iterrows():
        cid = row["card_ref"]
        if not cid:
            continue

        pipe.hset(f"c:{cid}", mapping={
            "party_id": row["party_id_fk"],
            "cc_last4": row["card_pan_last4"],
        })

    pipe.execute()
    print(f"[CARD] Batch {batch_id} → {len(pdf)} rows")

card_q = (
    card_df.writeStream
        .foreachBatch(write_card)
        .outputMode("append")
        .option("checkpointLocation", "s3a://fraud/checkpoints/card_redis/")
        .start()
)

# ============================================================
# TRANSACTION STREAM
# ============================================================

txn_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_ts", StringType()),
    StructField("event_epoch_sec", IntegerType()),
    StructField("card_ref", StringType()),
    StructField("merchant_ref", StringType()),
    StructField("amount_minor", IntegerType()),
    StructField("auth_code", StringType()),
    StructField("fraud_flag", IntegerType()),
])

txn_df = (
    spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "card_txn_auth")
        .option("startingOffsets", "latest")
        .load()
        .select(from_json(col("value").cast("string"), txn_schema).alias("data"))
        .select("data.*")
)

def process_and_join(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    pdf = batch_df.toPandas()
    r = redis.Redis(host=REDIS_HOST, decode_responses=True)

    global _ts_ready
    if not _ts_ready:
        ensure_timeseries(r)
        _ts_ready = True

    ts_now = int(time.time() * 1000)
    batch_txn_count = 0
    batch_fraud_count = 0
    batch_pred_fraud = 0
    batch_amount_minor = 0

    for _, row in pdf.iterrows():
        card_info = r.hgetall(f"c:{row['card_ref']}")
        if not card_info:
            continue

        user = r.hgetall(f"u:{card_info['party_id']}")
        merch = r.hgetall(f"m:{row['merchant_ref']}")

        simulated_fraud = 1 if random.random() < 0.1 else 0
        predicted_fraud = max(0, simulated_fraud + random.randint(-1, 1))

        batch_txn_count += 1
        batch_fraud_count += simulated_fraud
        batch_pred_fraud += predicted_fraud
        batch_amount_minor += int(row["amount_minor"])

        # Metrics for Grafana
        r.incr("total_transactions")
        if simulated_fraud == 1:
            r.incr("total_fraud_transactions")
        r.set("latest_transaction_amount", float(row["amount_minor"]) / 100.0)

    # Write aggregated metrics to Redis time series for Grafana dashboards
    try:
        r.execute_command("TS.ADD", "ts:txn:count", ts_now, batch_txn_count)
        r.execute_command("TS.ADD", "ts:txn:fraud", ts_now, batch_fraud_count)
        r.execute_command("TS.ADD", "ts:txn:pred", ts_now, batch_pred_fraud)
        r.execute_command("TS.ADD", "ts:txn:amount_minor", ts_now, batch_amount_minor)
    except Exception as e:
        print(f"[TXN] Failed to write Grafana metrics: {e}")

    print(f"[TXN] Batch {batch_id} → {len(pdf)} rows")

txn_q = (
    txn_df.writeStream
        .foreachBatch(process_and_join)
        .outputMode("append")
        .option("checkpointLocation", "s3a://fraud/checkpoints/txn_process/")
        .start()
)

# ============================================================
# WAIT FOR ALL STREAMS
# ============================================================
txn_q.awaitTermination()
