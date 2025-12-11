import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# --- CONFIG ---
KAFKA_SERVERS = "kafka-1:9092,kafka-2:9092"
MINIO_ENDPOINT = "http://minio:9000"
REDIS_HOST = "redis"

# DATA DIMENSION (Copy từ file generator sang để map chính xác location)
CITY_MASTER = {
    "VN-HCM-01": {"city": "Ho Chi Minh City", "lat": 10.7769, "long": 106.7009, "pop": 9000000},
    "VN-HN-01":  {"city": "Ha Noi", "lat": 21.0278, "long": 105.8342, "pop": 8000000},
    "VN-DN-01":  {"city": "Da Nang", "lat": 16.0544, "long": 108.2022, "pop": 1200000},
}

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("party_id", StringType(), True),
    StructField("given_name", StringType(), True),
    StructField("family_name", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("gender_code", StringType(), True),
    StructField("birth_date", StringType(), True),
    StructField("birth_year", IntegerType(), True),
    StructField("occupation_title", StringType(), True),
    StructField("address_line", StringType(), True),
    StructField("region_code", StringType(), True),
    StructField("home_postal_hint", StringType(), True),
    StructField("location_id_home", StringType(), True),
])

spark = (SparkSession.builder.appName("User_To_Redis")

    .config("spark.executor.cores", "1")
    .config("spark.executor.instances", "1")
    .config("spark.executor.memory", "512m")
    .config("spark.executor.memoryOverhead", "128m")


    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

df = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", "user_profile").option("startingOffsets", "latest").load()
    .select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*"))

def write_user_redis(batch_df, batch_id):
    pdf = batch_df.toPandas()
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    pipe = r.pipeline()
    
    for _, row in pdf.iterrows():
        pid = row['party_id']
        if not pid: continue
        
        # Lấy thông tin thành phố từ dictionary
        city_info = CITY_MASTER.get(row['location_id_home'], {})
        
        # MAPPING TÊN CỘT CHUẨN (DATASET GỐC)
        user_data = {
            "first": str(row['given_name']),
            "last": str(row['family_name']),
            "gender": str(row['gender_code']),
            "street": str(row['address_line']),
            "city": city_info.get("city", "Unknown"),
            "state": str(row['region_code']),
            "zip": str(row['home_postal_hint']),
            "lat": str(city_info.get("lat", 0.0)),
            "long": str(city_info.get("long", 0.0)),
            "city_pop": str(city_info.get("pop", 0)),
            "job": str(row['occupation_title']),
            "dob": str(row['birth_date'])
        }
        pipe.hset(f"u:{pid}", mapping=user_data) # Key ngắn gọn: u:{id}
    
    pipe.execute()
    print(f"Batch {batch_id}: Updated {len(pdf)} users.")

(df.writeStream.foreachBatch(write_user_redis).outputMode("append")
 .option("checkpointLocation", f"s3a://fraud/checkpoints/user_redis/").start().awaitTermination())


 