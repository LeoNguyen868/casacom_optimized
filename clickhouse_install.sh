docker run -d --name clickhouse-server \
  -p 8123:8123 \
  -p 9000:9000 \
  -e CLICKHOUSE_PASSWORD=default \
  -v ./clickhouse-data:/var/lib/clickhouse \
  -v ./clickhouse-logs:/var/log/clickhouse-server \
  --ulimit nofile=262144:262144 \
  clickhouse/clickhouse-server:latest