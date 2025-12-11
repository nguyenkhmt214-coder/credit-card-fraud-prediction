import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

# --- CONFIG ---
KAFKA_SERVERS = "kafka-1:9092,kafka-2:9092"
MINIO_ENDPOINT = "http://minio:9000"
REDIS_HOST = "redis"

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("party_id_fk", StringType(), True),
    StructField("card_ref", StringType(), True),
    StructField("card_pan_last4", StringType(), True),
])

spark = (SparkSession.builder.appName("Card_To_Redis")
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
    .option("subscribe", "card_account").option("startingOffsets", "latest").load()
    .select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*"))

def write_card_redis(batch_df, batch_id):
    pdf = batch_df.toPandas()
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    pipe = r.pipeline()
    
    for _, row in pdf.iterrows():
        cid = row['card_ref']
        if not cid: continue
        
        # Lưu mapping để lát Transaction tra cứu
        # Key: c:{card_ref} -> Value: party_id
        pipe.hset(f"c:{cid}", mapping={
            "party_id": str(row['party_id_fk']),
            "cc_last4": str(row['card_pan_last4']) # Giả lập cc_num
        })
        
    pipe.execute()
    print(f"Batch {batch_id}: Updated {len(pdf)} cards.")

(df.writeStream.foreachBatch(write_card_redis).outputMode("append")
 .option("checkpointLocation", f"s3a://fraud/checkpoints/card_redis/").start().awaitTermination())