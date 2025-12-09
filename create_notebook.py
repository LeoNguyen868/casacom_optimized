
import json
import nbformat as nbf

nb = nbf.v4.new_notebook()

# HEADER
text_header = """# Pipeline Verification Report
This notebook provides an interactive and visual verification of the ClickHouse pipeline against the Python baseline.
It performs the following steps:
1. **Ingest Data**: Loads `multi_maids_sample.csv` into ClickHouse.
2. **Execute View**: Queries the `view_aggregated_data` for each MAID.
3. **Compare**: Validates ClickHouse results against reference JSONs (`aggregated_data_100.json`).
4. **Visualize**: Plots score correlations and error distributions.
"""
nb['cells'].append(nbf.v4.new_markdown_cell(text_header))

# IMPORTS
code_imports = """
import pandas as pd
import json
import subprocess
import io
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Configure plotting
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
"""
nb['cells'].append(nbf.v4.new_code_cell(code_imports))

# HELPER FUNCTIONS
code_helpers = """
def run_query(query):
    cmd = ['docker', 'exec', 'clickhouse-server', 'clickhouse-client', '--format', 'JSON', '--query', query]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Query Error: {result.stderr}")
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return result.stdout

def run_command_no_output(query):
    cmd = ['docker', 'exec', 'clickhouse-server', 'clickhouse-client', '--query', query]
    subprocess.run(cmd, check=True)
"""
nb['cells'].append(nbf.v4.new_code_cell(code_helpers))

# DATA INGESTION
text_ingest = "## 1. Data Ingestion\nReloads data from CSV to ensure a fresh state."
nb['cells'].append(nbf.v4.new_markdown_cell(text_ingest))

code_ingest = """
# Clean and Clean
print("Cleaning tables...")
run_command_no_output("TRUNCATE TABLE IF EXISTS raw_maid_pings")

print("Importing multi_maids_sample.csv...")
df = pd.read_csv('multi_maids_sample.csv')
if 'geohash' in df.columns:
    df = df.drop(columns=['geohash'])
df['timestamp'] = df['timestamp'].astype(str).str.split('+').str[0]

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

if proc.returncode == 0:
    print("✅ Insert Successful.")
    run_command_no_output("OPTIMIZE TABLE raw_maid_pings FINAL")
else:
    print(f"❌ Insert Failed: {err.decode()}")
"""
nb['cells'].append(nbf.v4.new_code_cell(code_ingest))

# VERIFICATION LOGIC
text_verify = "## 2. Verification Loop\nIterates through all MAIDs and compares scores."
nb['cells'].append(nbf.v4.new_markdown_cell(text_verify))

code_verify = """
# Load Expectations
with open('aggregated_data_100.json', 'r') as f:
    multi_agg_json = json.load(f)

print(f"Verifying {len(multi_agg_json)} MAIDs...")

results = []

for maid, agg_json in multi_agg_json.items():
    # Query ClickHouse
    view_data = run_query(f"SELECT * FROM view_aggregated_data WHERE maid = '{maid}'")
    
    if not view_data or not view_data.get('data'):
        results.append({'maid': maid, 'status': 'MISSING', 'error': 'No Data'})
        continue
        
    v_row = view_data['data'][0]
    
    # Extract lists
    ch_geohashes = v_row['geohash']
    ch_home = v_row['home_score']
    ch_work = v_row['work_score']
    
    ch_map = {gh: {'h': h, 'w': w} for gh, h, w in zip(ch_geohashes, ch_home, ch_work)}
    
    # Extract JSON Reference
    ref_geohashes = agg_json['geohash']
    ref_home = agg_json['home_score']
    ref_work = agg_json['work_score']
    
    ref_map = {gh: {'h': h, 'w': w} for gh, h, w in zip(ref_geohashes, ref_home, ref_work)}
    
    # Compare per geohash
    for gh, ref_scores in ref_map.items():
        if gh not in ch_map:
            continue # Should log missing geohash
            
        ch_scores = ch_map[gh]
        
        diff_h = abs(ref_scores['h'] - ch_scores['h'])
        diff_w = abs(ref_scores['w'] - ch_scores['w'])
        
        status = 'PASS' if diff_h < 0.1 and diff_w < 0.1 else 'FAIL'
        
        results.append({
            'maid': maid,
            'geohash': gh,
            'ref_home': ref_scores['h'],
            'ch_home': ch_scores['h'],
            'diff_home': diff_h,
            'ref_work': ref_scores['w'],
            'ch_work': ch_scores['w'],
            'diff_work': diff_w,
            'status': status
        })

df_res = pd.DataFrame(results)
print(f"Processed {len(df_res)} geohash comparisons.")
"""
nb['cells'].append(nbf.v4.new_code_cell(code_verify))

# ANALYSIS
text_analysis = "## 3. Analysis & Visualization"
nb['cells'].append(nbf.v4.new_markdown_cell(text_analysis))

code_analysis = """
# Summary
print("--- PASS/FAIL Summary ---")
print(df_res['status'].value_counts())

# Failures
failures = df_res[df_res['status'] == 'FAIL']
if not failures.empty:
    print("\\n--- Top Failures ---")
    display(failures.sort_values(by='diff_home', ascending=False).head(5))
else:
    print("\\n✅ No significant failures found!")

# Visualization: Scatter Plot of Home Scores
plt.figure(figsize=(8, 8))
sns.scatterplot(data=df_res, x='ref_home', y='ch_home', hue='status', alpha=0.6)
plt.plot([0, 1], [0, 1], 'r--', label='Perfect Match')
plt.title('Correlation: Reference vs ClickHouse (Home Score)')
plt.xlabel('Python (Reference)')
plt.ylabel('ClickHouse (SQL)')
plt.legend()
plt.show()

# Visualization: Scatter Plot of Work Scores
plt.figure(figsize=(8, 8))
sns.scatterplot(data=df_res, x='ref_work', y='ch_work', hue='status', alpha=0.6)
plt.plot([0, 1], [0, 1], 'r--', label='Perfect Match')
plt.title('Correlation: Reference vs ClickHouse (Work Score)')
plt.xlabel('Python (Reference)')
plt.ylabel('ClickHouse (SQL)')
plt.legend()
plt.show()

# Distribution of Differences
plt.figure(figsize=(10, 4))
sns.histplot(df_res['diff_home'], bins=50, kde=True, color='blue', label='Home Diff')
sns.histplot(df_res['diff_work'], bins=50, kde=True, color='orange', label='Work Diff')
plt.title('Distribution of Score Deviations (Abs Diff)')
plt.xlabel('Absolute Difference')
plt.legend()
plt.show()
"""
nb['cells'].append(nbf.v4.new_code_cell(code_analysis))

# SAVE
with open('verify_pipeline.ipynb', 'w') as f:
    nbf.write(nb, f)

print("Notebook generated: verify_pipeline.ipynb")
