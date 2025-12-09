import duckdb
import os

def generate_test_data():
    output_file = 'tests/data/test_maids.csv'
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    print("Querying DuckDB for test data (limiting to 100 MAIDs)...")
    
    query = """
    WITH selected_maids AS (
        SELECT DISTINCT maid 
        FROM read_parquet('/home/hieu/Work/new_casacom/data/raw/2025-08-05/**/*.parquet')
        LIMIT 100
    )
    SELECT t.maid, t.latitude, t.longitude, t.timestamp, t.flux
    FROM read_parquet('/home/hieu/Work/new_casacom/data/raw/2025-08-05/**/*.parquet') t
    JOIN selected_maids sm ON t.maid = sm.maid
    ORDER BY t.maid, t.timestamp
    """
    
    # Execute query and save to CSV
    # converting to df first to easily save as csv compatible with our processing
    df = duckdb.query(query).df()
    
    print(f"Extracted {len(df)} rows for {df['maid'].nunique()} MAIDs.")
    
    df.to_csv(output_file, index=False)
    print(f"Saved test data to {output_file}")

if __name__ == "__main__":
    generate_test_data()
