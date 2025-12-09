import subprocess
import pandas as pd
import json
import logging
import io
import sys
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def run_query(query):
    cmd = ['docker', 'exec', 'clickhouse-server', 'clickhouse-client', '--format', 'JSON', '--query', query]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logging.error(f"Query failed: {result.stderr}")
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return result.stdout

def run_command_no_output(query):
    cmd = ['docker', 'exec', 'clickhouse-server', 'clickhouse-client', '--query', query]
    subprocess.run(cmd, check=True)

def verification_main():
    logging.info("Starting Verification (Per-Geohash Schema)...")

    # 1. Clean Data
    logging.info("Cleaning tables...")
    run_command_no_output("TRUNCATE TABLE IF EXISTS raw_maid_pings")
    run_command_no_output("TRUNCATE TABLE IF EXISTS maid_geohash_state")
    # 2. Insert Data
    logging.info("Importing CSV (multi_maids_sample.csv) ...")
    
    # Measure Data Prep + Insert Time
    t0_ingest = time.time()
    
    # Reading CSV in chunks to avoid memory spikes if needed, but 28MB is fine for pandas
    df = pd.read_csv('multi_maids_sample.csv')
    if 'geohash' in df.columns:
        df = df.drop(columns=['geohash'])
        
    # Convert to standard format for CH (CSV import expects 'YYYY-MM-DD HH:MM:SS' or similar)
    # Important: Python pipeline uses UTC. We must convert input timestamps to UTC.
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_convert('UTC').dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    # df['timestamp'] = df['timestamp'].astype(str).str.split('+').str[0] -- OLD BUGGY WAY
    
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    cmd_insert = [
        'docker', 'exec', '-i', 'clickhouse-server',
        'clickhouse-client',
        '--query', 'INSERT INTO raw_maid_pings (maid,timestamp,latitude,longitude,flux) FORMAT CSVWithNames'
    ]
    
    proc = subprocess.Popen(cmd_insert, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate(input=csv_buffer.getvalue())
    if proc.returncode != 0:
        logging.error(f"Insert Failed: {err.decode()}")
        return
        
    run_command_no_output("OPTIMIZE TABLE raw_maid_pings FINAL")
    t1_ingest = time.time()
    total_ingest_time = t1_ingest - t0_ingest
    logging.info(f"Insert Successful. Total Ingest Time: {total_ingest_time:.4f}s")

    # 3. Load Expected JSONs (100 Maids)
    with open('aggregated_data_100.json', 'r') as f:
        multi_agg_json = json.load(f)

    logging.info(f"Loaded expectations for {len(multi_agg_json)} MAIDs.")

    # 4. Verification Loop
    total_maids = 0
    passed_maids = 0
    failed_maids = 0
    
    t0_query = time.time()
    query_durations = []

    for maid, agg_json in multi_agg_json.items():
        total_maids += 1
        
        t_q_start = time.time()
        # Verify View
        view_data = run_query(f"SELECT * FROM view_aggregated_data WHERE maid = '{maid}'")
        t_q_end = time.time()
        query_durations.append(t_q_end - t_q_start)
        
        if not view_data or not view_data.get('data'):
            logging.error(f"  ❌ No data in View for {maid[:10]}...")
            failed_maids += 1
            continue

        v_row = view_data['data'][0]
        
        ch_geohashes = v_row['geohash']
        ch_home_scores = v_row['home_score']
        ch_home_scores = v_row['home_score']
        ch_work_scores = v_row['work_score']
        ch_leisure_scores = v_row.get('leisure_score', [0]*len(ch_geohashes))
        ch_pingsink_scores = v_row.get('pingsink_score', [0]*len(ch_geohashes))
        
        ch_pings = v_row.get('pings_array', [0]*len(ch_geohashes))
        ch_std_gh = v_row.get('std_geohash_m', [0.0]*len(ch_geohashes)) # Need to ensure this column exists in View
        
        # Map CH results
        ch_map = {}
        for i, gh in enumerate(ch_geohashes):
            ch_map[gh] = {
                'home': ch_home_scores[i],
                'work': ch_work_scores[i],
                'leisure': ch_leisure_scores[i],
                'pingsink': ch_pingsink_scores[i],
                'pings': ch_pings[i],
                'std_m': ch_std_gh[i]
            }
            
        # Map JSON results
        # ...
        json_geohashes = agg_json['geohash']
        json_home_scores = agg_json['home_score']
        json_work_scores = agg_json['work_score']
        json_leisure_scores = agg_json.get('leisure_score', [0]*len(json_geohashes))
        json_pingsink_scores = agg_json.get('pingsink_score', [0]*len(json_geohashes))
        json_pings = agg_json['pings']
        json_std_gh = agg_json['std_geohash_m'] # Ensure this exists in JSON
        
        json_map = {}
        for i, gh in enumerate(json_geohashes):
            json_map[gh] = {
                'home': json_home_scores[i],
                'work': json_work_scores[i],
                'leisure': json_leisure_scores[i],
                'pingsink': json_pingsink_scores[i],
                'pings': json_pings[i],
                'std_m': json_std_gh[i]
            }
            
        # Compare
        local_matches = 0
        local_mismatches = 0
        
        for gh, scores in json_map.items():
            if gh not in ch_map:
                # logging.warning(f"  ⚠️ Geohash {gh} missing in CH view")
                local_mismatches += 1
                continue
                
            ch_s = ch_map[gh]
            diff_home = abs(scores['home'] - ch_s['home'])
            diff_work = abs(scores['work'] - ch_s['work'])
            diff_leisure = abs(scores['leisure'] - ch_s['leisure'])
            diff_pingsink = abs(scores['pingsink'] - ch_s['pingsink'])
            
            if diff_home < 0.1 and diff_work < 0.1 and diff_leisure < 0.1 and diff_pingsink < 0.1:
                 local_matches += 1
            else:
                 # logging.warning(f"  ❌ Score Mismatch {gh}")
                 if failed_maids == 0: # Only print for first failed MAID
                     logging.warning(f"MISMATCH GH={gh}:")
                     logging.warning(f"  Pings: JSON={scores['pings']} CH={ch_s['pings']}")
                     logging.warning(f"  StdM : JSON={scores['std_m']:.2f} CH={ch_s['std_m']:.2f}")
                     # Extract intermediate metrics if available (need to update extraction logic above first if not)
                     # For now, just logging scores.
                     logging.warning(f"  JSON: H={scores['home']:.3f} W={scores['work']:.3f} L={scores['leisure']:.3f} P={scores['pingsink']:.3f}")
                     logging.warning(f"  CH  : H={ch_s['home']:.3f} W={ch_s['work']:.3f} L={ch_s['leisure']:.3f} P={ch_s['pingsink']:.3f}")
                     logging.warning(f"  Diff: H={diff_home:.3f} W={diff_work:.3f} L={diff_leisure:.3f} P={diff_pingsink:.3f}")
                 local_mismatches += 1
        
        if local_mismatches == 0:
            pass
            passed_maids += 1
        else:
            logging.warning(f"  ⚠️ MAID {maid[:10]}... Failed: {local_mismatches} mismatches.")
            failed_maids += 1
            
    t1_query = time.time()
    total_query_time = t1_query - t0_query
    avg_query_time = sum(query_durations) / len(query_durations) if query_durations else 0

    logging.info("="*40)
    logging.info(f"Verification Summary: {passed_maids}/{total_maids} MAIDs Passed.")
    logging.info(f"Performance Stats:")
    logging.info(f"  Total Ingest Time (incl prep): {total_ingest_time:.4f}s")
    logging.info(f"  Total Query Loop Time:         {total_query_time:.4f}s")
    logging.info(f"  Avg Query Latency (Client):    {avg_query_time:.4f}s")
    logging.info("="*40)

if __name__ == "__main__":
    verification_main()
