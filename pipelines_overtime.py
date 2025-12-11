import clickhouse_connect
import duckdb
import pandas as pd
import time
from tqdm import tqdm
# Get client connection
client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='default')

# Check existing tables in ClickHouse
print("Checking existing tables in ClickHouse...")
tables = client.query("SHOW TABLES").result_rows
print("\nCurrent tables:")
for table in tables:
    print(f"  - {table[0]}")

# Reset and initialize ClickHouse schema
print("Resetting ClickHouse schema...")

# Execute the ClickHouse schema script (includes DROP and CREATE statements)
with open('clickhouse_schema.sql', 'r') as f:
    schema_sql = f.read()

# Split by semicolons and execute each statement
statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
for i, stmt in enumerate(statements, 1):
    try:
        client.command(stmt)
        # Only print progress for CREATE statements to reduce noise
        if 'CREATE' in stmt.upper():
            print(f"Statement {i}/{len(statements)} executed successfully")
    except Exception as e:
        print(f"Statement {i}/{len(statements)} failed: {str(e)}")
        print(f"Statement: {stmt[:100]}...")

print("\nSchema reset complete.")

# Show all tables in ClickHouse
tables = client.query("SHOW TABLES").result_rows
print("\nClickHouse Tables:")
for table in tables:
    print(f"  - {table[0]}")

data=duckdb.query("""
SELECT maid, latitude, longitude, timestamp, flux
FROM read_parquet('/home/hieu/Work/new_casacom/data/raw/2025-08-05/**/*.parquet')
""").df()
total_maids=data['maid'].nunique()
print(f"Total maids with records: {total_maids}")

# Clean tables
print("Cleaning tables...")
client.command("TRUNCATE TABLE IF EXISTS raw_maid_pings")

print("Preparing dataframe before import...")

data['tz'] = 7.0 # Add default timezone offset

start_time = time.perf_counter()
batch_size = 100000
with tqdm(total=len(data), unit="rows", desc="Inserting data") as pbar:
    for i in range(0, len(data), batch_size):
        batch_df = data.iloc[i:i + batch_size]
        client.insert_df('raw_maid_pings', batch_df)
        pbar.update(len(batch_df))
end_time = time.perf_counter()

insert_elapsed_ms = (end_time - start_time) * 1000
per_maid_insert_ms = insert_elapsed_ms / total_maids if total_maids else 0

print(f"Insert time: {insert_elapsed_ms:.2f} ms ({per_maid_insert_ms:.4f} ms per maid)")
client.command("OPTIMIZE TABLE raw_maid_pings FINAL")

print("Querying view_aggregated_data for timing...")
query_start=time.perf_counter()
result=client.query("SELECT * FROM view_aggregated_data").result_rows
query_elapsed_ms=(time.perf_counter()-query_start)*1000
per_maid_query_ms=query_elapsed_ms/total_maids if total_maids else 0
print(f"view_aggregated_data query time: {query_elapsed_ms:.2f} ms ({per_maid_query_ms:.4f} ms per maid)")

res=pd.DataFrame(result)