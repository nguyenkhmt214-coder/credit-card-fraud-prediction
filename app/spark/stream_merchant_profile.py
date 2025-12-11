import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# --- CONFIG ---
KAFKA_SERVERS = "kafka-1:9092,kafka-2:9092"
MINIO_ENDPOINT = "http://minio:9000"
REDIS_HOST = "redis"

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_display_name", StringType(), True),
    StructField("mcc_group", StringType(), True),
    StructField("geo_lat", DoubleType(), True),
    StructField("geo_lon", DoubleType(), True),
])

spark = (SparkSession.builder.appName("Merchant_To_Redis")

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
    .option("subscribe", "merchant_profile").option("startingOffsets", "latest").load()
    .select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*"))

def write_merchant_redis(batch_df, batch_id):
    pdf = batch_df.toPandas()
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    pipe = r.pipeline()
    
    for _, row in pdf.iterrows():
        mid = row['merchant_id']
        if not mid: continue
        
        # MAPPING TÊN CỘT CHUẨN
        merch_data = {
            "merchant": str(row['merchant_display_name']),
            "category": str(row['mcc_group']),
            "merch_lat": str(row['geo_lat']),
            "merch_long": str(row['geo_lon'])
        }
        pipe.hset(f"m:{mid}", mapping=merch_data) # Key: m:{id}
        
    pipe.execute()
    print(f"Batch {batch_id}: Updated {len(pdf)} merchants.")

(df.writeStream.foreachBatch(write_merchant_redis).outputMode("append")
 .option("checkpointLocation", f"s3a://fraud/checkpoints/merchant_redis/").start().awaitTermination())