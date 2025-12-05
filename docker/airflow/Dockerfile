FROM apache/airflow:2.7.1

USER root

# Cài Java để chạy Spark Client
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    apt-get clean

USER airflow

# --- ĐOẠN SỬA QUAN TRỌNG Ở ĐÂY ---
# 1. Gỡ bỏ provider Google để tránh lỗi protobuf (vì bạn không dùng GCP)
# 2. Gỡ bỏ openlineage (thường gây lỗi tương thích)
RUN pip uninstall -y apache-airflow-providers-google apache-airflow-providers-openlineage || true

# 3. Cài các thư viện cần thiết
# Lưu ý: Cài thêm pyspark để Airflow có thể chạy SparkSubmitOperator local nếu cần
RUN pip install --user pandas apache-airflow-providers-apache-spark pyspark==3.5.1