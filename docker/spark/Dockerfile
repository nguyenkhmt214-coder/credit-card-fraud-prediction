FROM apache/spark:3.5.1

# Install dependencies for DuckDB .deb
RUN apt-get update && apt-get install -y wget

# Install precompiled DuckDB CLI (Linux x64)
RUN wget https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-amd64.zip \
    && unzip duckdb_cli-linux-amd64.zip \
    && mv duckdb /usr/local/bin/duckdb \
    && rm duckdb_cli-linux-amd64.zip

# Install DuckDB Python package WITHOUT compiling
RUN pip install --no-cache-dir duckdb==1.0.0 --prefer-binary

RUN pip install pandas pyarrow
