#!/bin/bash

# Remove existing container if it exists
docker rm -f clickhouse-server 2>/dev/null || true

# Run ClickHouse server
docker run -d \
  --name clickhouse-server \
  -p 8123:8123 \
  -p 9001:9000 \
  -e CLICKHOUSE_PASSWORD=default \
  -v "$(pwd)/clickhouse-data:/var/lib/clickhouse" \
  -v "$(pwd)/clickhouse-logs:/var/log/clickhouse-server" \
  --ulimit nofile=262144:262144 \
  clickhouse/clickhouse-server:latest