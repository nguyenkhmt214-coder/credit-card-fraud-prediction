"""
Spark Structured Streaming Job:
- Đọc Kafka topic "user_profile"
- Parse JSON
- Ghi raw parquet vào MinIO (S3-compatible)
- Ghi bảng cleaned vào DuckDB
"""

import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# ============================================================
# CONFIG — gom tất cả path và thông số để bạn sửa cho dễ
# ============================================================

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS = "admin"
MINIO_SECRET = "password123"
MINIO_BUCKET = "fraud"

KAFKA_SERVERS = "kafka-1:9092,kafka-2:9092"

# DuckDB file lưu trong container Spark:
# docker-compose mount: ./app → /opt/workspace/app
DUCKDB_PATH = "/opt/workspace/app/duckdb/user_profile.duckdb"

CHECKPOINT_RAW = f"s3a://{MINIO_BUCKET}/checkpoints/user_profile_raw/"
RAW_OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/user_profile_raw/"

CHECKPOINT_DUCK = f"s3a://{MINIO_BUCKET}/checkpoints/user_profile_duckdb/"


# ============================================================
# 1. Tạo SparkSession có plugin S3A để ghi Minio
# ============================================================

spark = (
    SparkSession.builder
    .appName("stream_user_profile_to_minio_duckdb")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. Khai báo schema user_profile
# ============================================================

user_schema = StructType([
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


# ============================================================
# 3. Đọc Kafka stream (value là JSON string)
# ============================================================

df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", "user_profile")
    .option("startingOffsets", "latest")
    .load()
)

df_raw = df_kafka.selectExpr("CAST(value AS STRING) AS json_str")

df = (
    df_raw
    .select(from_json(col("json_str"), user_schema).alias("data"))
    .select("data.*")
)

df = df.withColumn("event_timestamp", to_timestamp(col("event_ts")))


# ============================================================
# 4. Ghi RAW PARQUET vào MinIO
# ============================================================

raw_query = (
    df.writeStream
    .format("parquet")
    .option("checkpointLocation", CHECKPOINT_RAW)
    .option("path", RAW_OUTPUT_PATH)
    .outputMode("append")
    .start()
)


# ============================================================
# 5. Ghi vào DuckDB qua foreachBatch
# ============================================================

def write_to_duckdb(batch_df, batch_id):
    """
    Mỗi micro-batch Spark gọi hàm này.
    Lưu xuống DuckDB theo dạng append.
    """
    pdf = batch_df.toPandas()

    con = duckdb.connect(DUCKDB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS user_profile (
            event_id VARCHAR,
            event_ts VARCHAR,
            party_id VARCHAR,
            given_name VARCHAR,
            family_name VARCHAR,
            full_name VARCHAR,
            gender_code VARCHAR,
            birth_date VARCHAR,
            birth_year INTEGER,
            occupation_title VARCHAR,
            address_line VARCHAR,
            region_code VARCHAR,
            home_postal_hint VARCHAR,
            location_id_home VARCHAR,
            event_timestamp TIMESTAMP
        );
    """)
    con.append("user_profile", pdf)
    con.close()


duckdb_query = (
    df.writeStream
    .foreachBatch(write_to_duckdb)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_DUCK)
    .start()
)


# ============================================================
# 6. Run forever
# ============================================================
raw_query.awaitTermination()
duckdb_query.awaitTermination()
