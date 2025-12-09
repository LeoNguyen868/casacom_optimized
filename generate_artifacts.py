
import csv
import sys
import json
import datetime
from pathlib import Path
from collections import defaultdict

# Adjust paths to import modules
ROOT = Path(__file__).resolve().parent
sys.path.append(str(ROOT))

try:
    from scripts.evidence_pipeline_new import build_columnar_store, derive_columnar
except ImportError as e:
    print(f"Error importing pipelines: {e}")
    sys.exit(1)

# JSON Encoder for datetimes and sets
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        if isinstance(obj, set):
            return list(obj)
        return super().default(obj)

import pygeohash as pgh

def load_data(filepath):
    data_by_maid = defaultdict(list)
    with open(filepath, 'r') as f:
        filtered_lines = (line for line in f if not line.startswith('#'))
        reader = csv.DictReader(filtered_lines)
        for row in reader:
            maid = row['maid']
            if 'geohash' not in row and 'latitude' in row and 'longitude' in row:
                try:
                    lat = float(row['latitude'])
                    lon = float(row['longitude'])
                    row['geohash'] = pgh.encode(lat, lon, precision=7)
                except (ValueError, TypeError):
                    continue
            
            data_by_maid[maid].append(row)
    return data_by_maid

def run_new_pipeline_full(rows, maid):
    store = build_columnar_store(rows, maid=maid)
    derived = derive_columnar(store)
    return store, derived

def main():
    import time
    input_file = ROOT / "multi_maids_sample.csv"
    if not input_file.exists():
        print(f"Input file not found: {input_file}")
        return

    print("Loading data...")
    t0_load = time.time()
    data_by_maid = load_data(input_file)
    t1_load = time.time()
    
    if not data_by_maid:
        print("No data found in multi_maids_sample.csv")
        return

    maid_count = len(data_by_maid)
    print(f"Found {maid_count} MAIDs. Load time: {t1_load - t0_load:.2f}s")

    multi_stored = {}
    multi_aggregated = {}

    print(f"Starting Python Processing for {maid_count} MAIDs...")
    t0_proc = time.time()
    
    # Process only first 100 if there are more, or all if less
    # User asked for "100 maids", assuming file has them.
    
    processed_count = 0
    for maid, rows in data_by_maid.items():
        # print(f"Processing MAID: {maid} ({len(rows)} rows)") # Reduce noise
        new_store, new_derived = run_new_pipeline_full(rows, maid)
        multi_stored[maid] = new_store
        multi_aggregated[maid] = new_derived
        processed_count += 1
        if processed_count % 10 == 0:
            print(f"  Processed {processed_count}/{maid_count}...")
            
    t1_proc = time.time()
    total_time = t1_proc - t0_proc
    avg_time = total_time / processed_count if processed_count else 0
    
    print(f"\nPython Processing Complete.")
    print(f"Total Time: {total_time:.4f}s")
    print(f"Average Time per MAID: {avg_time:.4f}s")

    with open("stored_data_100.json", "w") as f:
        json.dump(multi_stored, f, indent=2, cls=CustomJSONEncoder)
    print("Saved stored_data_100.json")
    
    with open("aggregated_data_100.json", "w") as f:
        json.dump(multi_aggregated, f, indent=2, cls=CustomJSONEncoder)
    print("Saved aggregated_data_100.json")
    
    print("\nArtifact generation complete.")

if __name__ == "__main__":
    main()
