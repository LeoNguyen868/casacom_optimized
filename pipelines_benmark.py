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
# if 'geohash' in data.columns:
#     data = data.drop(columns=['geohash'])
# Parse timestamps with timezone info and convert to UTC (naive)
data['timestamp'] = pd.to_datetime(data['timestamp'], utc=True)\
    .dt.tz_convert('UTC')\
    .dt.tz_localize(None)

print("Importing data from dataframe in batches...")
batch_size=10000
batch_ms_per_maid=[]

# Sort data by maid for efficient sliding window
data_sorted = data.sort_values('maid').reset_index(drop=True)

# Get maid boundaries for sliding window
maid_changes = data_sorted['maid'].ne(data_sorted['maid'].shift()).cumsum()
maid_boundaries = data_sorted.groupby(maid_changes).apply(lambda x: (x.index[0], x.index[-1] + 1)).tolist()

batch_idx = 0
i = 0
with tqdm(total=total_maids, desc="Importing batches") as pbar:
    while i < len(maid_boundaries):
        batch_idx += 1
        # Collect batch_size maids using sliding window
        batch_end = min(i + batch_size, len(maid_boundaries))
        start_row = maid_boundaries[i][0]
        end_row = maid_boundaries[batch_end - 1][1]
        
        batch_df = data_sorted.iloc[start_row:end_row]
        num_maids = batch_end - i
        
        if batch_df.empty:
            tqdm.write(f"Batch {batch_idx} skipped (no rows).")
            i = batch_end
            pbar.update(num_maids)
            continue
            
        start_time = time.perf_counter()
        try:
            client.insert_df('raw_maid_pings', batch_df)
        except Exception as e:
            tqdm.write(f"Batch {batch_idx} insert failed: {str(e)}")
            i = batch_end
            pbar.update(num_maids)
            continue
            
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        ms_per_maid = elapsed_ms / num_maids
        batch_ms_per_maid.append(ms_per_maid)
        tqdm.write(f"Batch {batch_idx}: {num_maids} maids, {len(batch_df)} rows, {ms_per_maid:.2f} ms per maid")
        
        i = batch_end
        pbar.update(num_maids)

client.command("OPTIMIZE TABLE raw_maid_pings FINAL")

print("Querying view_aggregated_data for timing...")
query_start=time.perf_counter()
result=client.query("SELECT * FROM view_aggregated_data").result_rows
query_elapsed_ms=(time.perf_counter()-query_start)*1000
per_maid_query_ms=query_elapsed_ms/total_maids if total_maids else 0
print(f"view_aggregated_data query time: {query_elapsed_ms:.2f} ms ("{per_maid_query_ms:.4f} ms per maid)")

res=pd.DataFrame(result)