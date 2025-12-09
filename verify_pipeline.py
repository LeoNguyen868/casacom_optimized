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
    logging.info("Starting Verification (Batch Mode)...")

    # 1. Clean Data
    logging.info("Cleaning tables...")
    run_command_no_output("TRUNCATE TABLE IF EXISTS raw_maid_pings")
    run_command_no_output("TRUNCATE TABLE IF EXISTS maid_geohash_state")
    
    # 2. Insert Data
    logging.info("Importing CSV (multi_maids_sample.csv) ...")
    
    t0_ingest = time.time()
    
    try:
        df = pd.read_csv('multi_maids_sample.csv')
    except FileNotFoundError:
        logging.error("multi_maids_sample.csv not found!")
        return

    if 'geohash' in df.columns:
        df = df.drop(columns=['geohash'])
        
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_convert('UTC').dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    
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

    # 3. Load Expected JSONs
    with open('aggregated_data_100.json', 'r') as f:
        multi_agg_json = json.load(f)
    logging.info(f"Loaded expectations for {len(multi_agg_json)} MAIDs.")

    # 4. Batch Query
    logging.info("Running Batch Query on view_aggregated_data...")
    t0_query = time.time()
    # Fetch ALL data ordered by maid
    all_data = run_query("SELECT * FROM view_aggregated_data ORDER BY maid")
    t1_query = time.time()
    
    if not all_data or 'data' not in all_data:
        logging.error("No data returned from ClickHouse!")
        return

    # Index CH data by MAID
    ch_data_map = {row['maid']: row for row in all_data['data']}
    
    total_maids = 0
    passed_maids = 0
    failed_maids = 0
    
    # Error Accumulators
    errors = {
        'home': [], 'work': [], 'leisure': [], 'pingsink': [], 'std_m': []
    }
    
    logging.info(f"Comparing {len(multi_agg_json)} MAIDs...")

    for maid, agg_json in multi_agg_json.items():
        total_maids += 1
        
        if maid not in ch_data_map:
            logging.error(f"  ❌ MAID {maid[:10]} missing in ClickHouse results.")
            failed_maids += 1
            continue

        v_row = ch_data_map[maid]
        
        # CH Data
        ch_geohashes = v_row['geohash']
        ch_pings = v_row.get('pings_array', [0]*len(ch_geohashes))
        
        ch_map = {}
        for i, gh in enumerate(ch_geohashes):
            ch_map[gh] = {
                'home': v_row['home_score'][i],
                'work': v_row['work_score'][i],
                'leisure': v_row.get('leisure_score', [0]*len(ch_geohashes))[i],
                'pingsink': v_row.get('pingsink_score', [0]*len(ch_geohashes))[i],
                'std_m': v_row.get('std_geohash_m', [0.0]*len(ch_geohashes))[i],
                # Debug metrics
                'active_day_ratio': v_row['active_day_ratio'][i],
                'continuity': v_row['continuity_factor'][i],
                'unique_days': v_row['unique_days'][i],
                ## 'span_days': v_row['span_days_raw'][i],  <-- Need to expose this in view if I want to check it. 
                # Calculating span manually from ts? No, trust view? 
                # Wait, I didn't expose span_days_raw in the final view. 
                # Let's check active_days_last_30d
                'active_days_30d': v_row['active_days_last_30d'][i],
                'night_ratio': v_row['night_days_ratio'][i] # Check one ratio
            }

        # JSON Data
        json_geohashes = agg_json['geohash']
        json_map = {}
        for i, gh in enumerate(json_geohashes):
            json_map[gh] = {
                'home': agg_json['home_score'][i],
                'work': agg_json['work_score'][i],
                'leisure': agg_json.get('leisure_score', [0]*len(json_geohashes))[i],
                'pingsink': agg_json.get('pingsink_score', [0]*len(json_geohashes))[i],
                'std_m': agg_json['std_geohash_m'][i],
                'active_day_ratio': agg_json['active_day_ratio'][i] if 'active_day_ratio' in agg_json else 0,
                # Python aggregation script doesn't output continuity directly in level_1/2?
                # Check generate_artifacts / evidence_pipeline_new.
                # It does output 'active_days_last_30d', 'unique_days', 'span_days'
                'unique_days': agg_json['unique_days'][i],
                'active_days_30d': agg_json['active_days_last_30d'][i],
                'night_ratio': agg_json['night_days_ratio'][i]
            }

        local_mismatches = 0
        
        for gh, j_scores in json_map.items():
            if gh not in ch_map:
                continue
            
            c_scores = ch_map[gh]
            
            diff_home = abs(j_scores['home'] - c_scores['home'])
            diff_work = abs(j_scores['work'] - c_scores['work'])
            diff_leisure = abs(j_scores['leisure'] - c_scores['leisure'])
            diff_pingsink = abs(j_scores['pingsink'] - c_scores['pingsink'])
            diff_std = abs(j_scores['std_m'] - c_scores['std_m'])
            
            errors['home'].append(diff_home)
            errors['work'].append(diff_work)
            errors['leisure'].append(diff_leisure)
            errors['pingsink'].append(diff_pingsink)
            errors['std_m'].append(diff_std)
            
            if max(diff_home, diff_work, diff_leisure, diff_pingsink) > 0.05: # Stricter threshold 0.05
                local_mismatches += 1
                if failed_maids < 5: # Limit detailed logs
                    logging.warning(f"  Mismatch {maid[:8]} GH={gh}:")
                    logging.warning(f"    Home: J={j_scores['home']:.3f} C={c_scores['home']:.3f} D={diff_home:.3f}")
                    logging.warning(f"    Work: J={j_scores['work']:.3f} C={c_scores['work']:.3f} D={diff_work:.3f}")
                    
                    # Detailed Debug
                    logging.warning(f"    -- Components --")
                    logging.warning(f"    UniqueD: J={j_scores['unique_days']} C={c_scores['unique_days']}")
                    logging.warning(f"    ActDy30: J={j_scores['active_days_30d']} C={c_scores['active_days_30d']}")
                    logging.warning(f"    ActRatio: J={j_scores['active_day_ratio']:.3f} C={c_scores['active_day_ratio']:.3f}")
                    if 'continuity' in c_scores:
                        logging.warning(f"    CH Cont.: {c_scores['continuity']:.3f}")
                    logging.warning(f"    NightRa: J={j_scores['night_ratio']:.3f} C={c_scores['night_ratio']:.3f}")
        
        if local_mismatches == 0:
            passed_maids += 1
        else:
            failed_maids += 1

    logging.info("="*40)
    logging.info(f"Summary: {passed_maids}/{total_maids} Passed (Threshold 0.05)")
    logging.info(f"Batch Query Time: {t1_query - t0_query:.4f}s")
    
    def print_stats(name, err_list):
        if not err_list: return
        avg = sum(err_list)/len(err_list)
        mx = max(err_list)
        logging.info(f"  {name:10} | MAE: {avg:.4f} | Max: {mx:.4f}")

    logging.info("Error Stats (MAE / Max):")
    print_stats("Home", errors['home'])
    print_stats("Work", errors['work'])
    print_stats("Leisure", errors['leisure'])
    print_stats("Pingsink", errors['pingsink'])
    print_stats("Std(m)", errors['std_m'])
    logging.info("="*40)

if __name__ == "__main__":
    verification_main()
