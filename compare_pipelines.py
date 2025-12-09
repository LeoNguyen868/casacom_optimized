
import csv
import sys
import json
import math
import pygeohash as pgh
from pathlib import Path
from collections import defaultdict
from datetime import datetime

# Adjust paths to import modules
ROOT = Path(__file__).resolve().parent
sys.path.append(str(ROOT))

# Import pipelines
try:
    from evidence_old import EvidenceStore
    from scripts.evidence_pipeline_new import build_columnar_store, derive_columnar
except ImportError as e:
    print(f"Error importing pipelines: {e}")
    sys.exit(1)

def run_old_pipeline(rows, maid):
    store = EvidenceStore(maid=maid)
    
    # Prepare data structure for proper update() call
    # update(self, new_data, geohashes, flux_data)
    # new_data: {gh7: [ts, ...]}
    # geohashes: {gh7: [gh12, ...]}
    # flux_data: {gh7: [flux, ...]}
    
    new_data = defaultdict(list)
    geohashes_p12 = defaultdict(list)
    flux_data = defaultdict(list)
    
    for row in rows:
        gh = row['geohash']
        # Ensure timestamp is string for _to_dt inside the class, or datetime objects
        # The class handles strings.
        ts = row['timestamp']
        lat = float(row['latitude'])
        lon = float(row['longitude'])
        flux = row['flux']
        
        new_data[gh].append(ts)
        
        # Calculate gh12 using pygeohash
        gh12 = pgh.encode(lat, lon, precision=12)
        geohashes_p12[gh].append(gh12)
        
        if flux:
            flux_data[gh].append(flux)
            
    store.update(new_data, geohashes_p12, flux_data)
    
    # Derive scores for each geohash
    results = {}
    for gh in store.store.keys():
        ev = store.derive(gh)
        if ev:
            scores = store.overall_score(ev)
            # Combine relevant info for comparison
            stats = {
                'pings': ev['level_1_primary']['pings'],
                'scores': scores,
                'home_score': scores['home'],
                'work_score': scores['work'],
                'leisure_score': scores['leisure'],
                'pingsink_score': scores['pingsink'],
                'feat': {
                    'night_days_ratio': ev['level_2_secondary']['night_days_ratio'],
                    'night_ratio': ev['level_2_secondary']['night_ratio'],
                    'late_evening_days_ratio': ev['level_2_secondary']['late_evening_days_ratio'],
                    'early_morning_days_ratio': ev['level_2_secondary']['early_morning_days_ratio'],
                    'entropy_hour_norm': ev['level_2_secondary']['entropy_hour_norm'],
                    'active_day_ratio': ev['level_1_primary']['active_day_ratio'],
                    'monthly_stability': ev['level_2_secondary']['monthly_stability'],
                    'active_days_last_30d': ev['level_2_secondary']['active_days_last_30d'],
                },
                'meta': ev.get('meta', {})
            }
            results[gh] = stats
            
    return results

def run_new_pipeline(rows, maid):
    # build_columnar_store expects iterable of dicts with keys matching column names
    # Our CSV rows are already dicts, need to ensure type conversion if not handled by build function
    # The build function does str(), float() conversions internally.
    
    store = build_columnar_store(rows, maid=maid)
    derived = derive_columnar(store)
    
    results = {}
    # derived is columnar, iterate through lists
    if not derived['geohash']:
        return results
        
    num_ghs = len(derived['geohash'])
    for i in range(num_ghs):
        gh = derived['geohash'][i]
        stats = {
            'pings': derived['pings'][i],
            'scores': {
                'home': derived['home_score'][i],
                'work': derived['work_score'][i],
                'leisure': derived['leisure_score'][i],
                'pingsink': derived['pingsink_score'][i],
            },
            'home_score': derived['home_score'][i],
            'work_score': derived['work_score'][i],
            'leisure_score': derived['leisure_score'][i],
            'pingsink_score': derived['pingsink_score'][i],
            'feat': {
                'night_days_ratio': derived['night_days_ratio'][i],
                'night_ratio': derived['night_ratio'][i],
                'late_evening_days_ratio': derived['late_evening_days_ratio'][i],
                'early_morning_days_ratio': derived['early_morning_days_ratio'][i],
                'entropy_hour_norm': derived['entropy_hour_norm'][i],
                'active_day_ratio': derived['active_day_ratio'][i],
                'monthly_stability': derived['monthly_stability'][i],
                'active_days_last_30d': derived['active_days_last_30d'][i],
            },
            'std_geohash_m': derived['std_geohash_m'][i],
        }
        results[gh] = stats
        
    return results

def compare_results(old_res, new_res, maid):
    print(f"\nComparing results for MAID: {maid}")
    all_ghs = set(old_res.keys()) | set(new_res.keys())
    
    for gh in sorted(all_ghs):
        old_stats = old_res.get(gh)
        new_stats = new_res.get(gh)
        
        if not old_stats:
            print(f"  Geohash {gh}: Missing in OLD pipeline")
            continue
        if not new_stats:
            print(f"  Geohash {gh}: Missing in NEW pipeline")
            continue
            
        print(f"  Geohash {gh}:")
        
        # Compare Pings
        p_old = old_stats['pings']
        p_new = new_stats['pings']
        if p_old != p_new:
             print(f"    Pings MISMATCH: Old={p_old}, New={p_new}")
        else:
             print(f"    Pings: {p_old} (Match)")
        
        # Compare intermediate features
        feats_diff = False
        if 'feat' in old_stats and 'feat' in new_stats:
            for k in old_stats['feat']:
                v_old = old_stats['feat'][k]
                v_new = new_stats['feat'][k]
                diff = abs(v_old - v_new)
                if diff > 1e-6:
                    print(f"    Feat {k} MISMATCH: Old={v_old:.6f}, New={v_new:.6f}, Diff={diff:.6f}")
                    feats_diff = True
        
        if feats_diff:
            print("    -> Feature mismatches detected, likely causing score differences.")
            
        # Debug std_m specifically for pingsink mismatch
        std_old = old_stats.get('meta', {}).get('std_geohash_m')
        std_new = new_stats.get('std_geohash_m')
        
        if std_old is not None and std_new is not None:
             if abs(std_old - std_new) > 1e-6:
                 print(f"    Meta std_geohash_m MISMATCH: Old={std_old:.9f}, New={std_new:.9f}")
             
        # Compare Scores
        for score_type in ['home_score', 'work_score', 'leisure_score', 'pingsink_score']:
            s_old = old_stats[score_type]
            s_new = new_stats[score_type]
            diff = abs(s_old - s_new)
            if diff > 1e-6:
                print(f"    {score_type} MISMATCH: Old={s_old:.6f}, New={s_new:.6f}, Diff={diff:.6f}")
            else:
                pass

import argparse

def main():
    parser = argparse.ArgumentParser(description="Compare evidence pipelines.")
    parser.add_argument("--input", default="sample_data_input.csv", help="Input CSV file path")
    args = parser.parse_args()
    
    input_file = ROOT / args.input
    if not input_file.exists():
        print(f"Input file not found: {input_file}")
        return

    # Load data grouped by MAID
    data_by_maid = defaultdict(list)
    with open(input_file, 'r') as f:
        # filter out comment lines starting with #
        filtered_lines = (line for line in f if not line.startswith('#'))
        reader = csv.DictReader(filtered_lines)
        for row in reader:
            maid = row['maid']
            data_by_maid[maid].append(row)
            
    for maid, rows in data_by_maid.items():
        print(f"Processing MAID: {maid} ({len(rows)} rows)")
        
        try:
            old_results = run_old_pipeline(rows, maid)
            new_results = run_new_pipeline(rows, maid)
            
            compare_results(old_results, new_results, maid)
        except Exception as e:
            print(f"Error processing MAID {maid}: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()
