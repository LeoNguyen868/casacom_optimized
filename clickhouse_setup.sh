#!/bin/bash
set -e

echo "Starting ClickHouse C++ UDF Setup..."

# 1. Compile C++ Tool (Static binary for Docker compatibility)
echo "Compiling scoring_tool..."
mkdir -p clickhouse_udf/bin
g++ -static -O3 -o clickhouse_udf/bin/scoring_tool \
    clickhouse_udf/src/main.cpp \
    clickhouse_udf/src/scoring.cpp

if [ $? -ne 0 ]; then
    echo "Compilation failed!"
    exit 1
fi
echo "Compilation successful."

# 2. Deploy to Docker Container
CONTAINER_NAME="clickhouse-server"

echo "Deploying to container: $CONTAINER_NAME"

# Ensure user_scripts directory exists
docker exec $CONTAINER_NAME mkdir -p /var/lib/clickhouse/user_scripts

# Copy binary
docker cp clickhouse_udf/bin/scoring_tool $CONTAINER_NAME:/var/lib/clickhouse/user_scripts/
docker exec $CONTAINER_NAME chmod +x /var/lib/clickhouse/user_scripts/scoring_tool

# Copy configuration
docker cp evidence_function.xml $CONTAINER_NAME:/etc/clickhouse-server/evidence_function.xml

# 3. Reload ClickHouse
echo "Reloading ClickHouse functions..."
docker exec $CONTAINER_NAME clickhouse-client -q "SYSTEM RELOAD FUNCTIONS"

echo "Setup complete! UDFs are ready to use."