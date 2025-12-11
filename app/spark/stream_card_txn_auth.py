import redis
import pandas as pd
from datetime import datetime
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# --- CONFIG ---
KAFKA_SERVERS = "kafka-1:9092,kafka-2:9092"
MINIO_ENDPOINT = "http://minio:9000"
REDIS_HOST = "redis"

# Schema Transaction đầu vào (từ generator)
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("event_epoch_sec", IntegerType(), True),
    StructField("card_ref", StringType(), True),
    StructField("merchant_ref", StringType(), True),
    StructField("amount_minor", IntegerType(), True),
    StructField("auth_code", StringType(), True),
    StructField("fraud_flag", IntegerType(), True),
])

spark = (SparkSession.builder.appName("Fraud_Detection_Engine")

    .config("spark.executor.cores", "1")
    .config("spark.executor.instances", "1")
    
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

df = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", "card_txn_auth").option("startingOffsets", "latest").load()
    .select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*"))

def process_and_join(batch_df, batch_id):
    if batch_df.isEmpty(): return
    
    # 1. Lấy Transaction Data
    txns = batch_df.toPandas()
    
    # Lưu ý: Nếu chạy trong Docker Compose cùng mạng thì host="redis", 
    # Nếu chạy Spark bên ngoài thì phải đổi thành IP (ví dụ '172.17.0.2')
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    
    final_rows = []
    
    # 2. Loop & Lookup (Join trong Code)
    for idx, row in txns.iterrows():
        # --- LOOKUP REDIS ---
        card_info = r.hgetall(f"c:{row['card_ref']}")
        if not card_info: continue 
        
        party_id = card_info.get("party_id")
        user_info = r.hgetall(f"u:{party_id}")
        merch_info = r.hgetall(f"m:{row['merchant_ref']}")
        
        trans_date = row['event_ts'] 
        
        # --- [MỚI THÊM VÀO 1] RANDOM FRAUD ĐỂ TEST GRAFANA ---
        # Random đại: 10% là Fraud (1), 90% là sạch (0)
        # Mục đích: Để thấy biểu đồ Grafana nhảy lên xuống cho vui mắt
        simulated_fraud = 1 if random.random() < 0.1 else 0
        
        # --- Construct row ---
        clean_row = {
            'Unnamed: 0': idx,
            'trans_date_trans_time': trans_date,
            'cc_num': int(f"400000{card_info.get('cc_last4','0000')}"),
            'merchant': merch_info.get('merchant', 'Unknown'),
            'category': merch_info.get('category', 'misc_net'),
            'amt': float(row['amount_minor']) / 100.0,
            'first': user_info.get('first', ''),
            'last': user_info.get('last', ''),
            'gender': user_info.get('gender', 'M'),
            'street': user_info.get('street', ''),
            'city': user_info.get('city', ''),
            'state': user_info.get('state', ''),
            'zip': int(user_info.get('zip', '0')) if user_info.get('zip') else 0,
            'lat': float(user_info.get('lat', 0.0)),
            'long': float(user_info.get('long', 0.0)),
            'city_pop': int(user_info.get('city_pop', 0)),
            'job': user_info.get('job', ''),
            'dob': user_info.get('dob', ''),
            'trans_num': row['auth_code'],
            'unix_time': row['event_epoch_sec'],
            'merch_lat': float(merch_info.get('merch_lat', 0.0)),
            'merch_long': float(merch_info.get('merch_long', 0.0)),
            
            # Thay vì lấy row['fraud_flag'] thật, ta lấy cái random vừa tạo
            'is_fraud': simulated_fraud 
        }
        
        # --- [MỚI THÊM VÀO 2] GHI METRIC VÀO REDIS CHO GRAFANA ---
        try:
            # 1. Tăng tổng số giao dịch lên 1
            r.incr('total_transactions')
            
            # 2. Nếu là Fraud (giả lập) thì tăng biến fraud lên 1
            if simulated_fraud == 1:
                r.incr('total_fraud_transactions')
                print(f"!!! FRAUD DETECTED !!! Amt: {clean_row['amt']}")
            
            # 3. Lưu giá trị tiền giao dịch mới nhất (để vẽ biểu đồ Line)
            r.set('latest_transaction_amount', clean_row['amt'])
            
        except Exception as e:
            print(f"Error writing to Redis Grafana metrics: {e}")

        final_rows.append(clean_row)
    
    # 3. Tạo DataFrame kết quả cuối cùng
    if final_rows:
        final_df = pd.DataFrame(final_rows)
        print("\n=== FULL DATASET READY FOR ML ===")
        print(final_df[['cc_num', 'amt', 'category', 'is_fraud']].tail(3))

(df.writeStream.foreachBatch(process_and_join).outputMode("append")
 .option("checkpointLocation", f"s3a://fraud/checkpoints/txn_process/").start().awaitTermination())