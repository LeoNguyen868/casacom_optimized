import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

def generate_dense_data(num_maids=5, min_pings=1200, max_pings=2000):
    data = []
    
    # Base location (Hanoi roughly)
    base_lat = 21.0285
    base_lon = 105.8542
    
    for i in range(num_maids):
        maid_id = f"dense_maid_{i:03d}"
        n_pings = random.randint(min_pings, max_pings)
        
        # Define "Home" and "Work" locations
        home_lat = base_lat + random.uniform(-0.01, 0.01)
        home_lon = base_lon + random.uniform(-0.01, 0.01)
        
        work_lat = base_lat + random.uniform(-0.02, 0.02)
        work_lon = base_lon + random.uniform(-0.02, 0.02)
        
        # Start date
        start_date = datetime(2025, 1, 1)
        
        for _ in range(n_pings):
            # Pick a time
            day_offset = random.randint(0, 60) # 2 months
            hour = random.randint(0, 23)
            minute = random.randint(0, 59)
            ts = start_date + timedelta(days=day_offset, hours=hour, minutes=minute)
            
            # Pick location based on time (simple heuristic)
            if 0 <= hour < 6 or hour > 20: # Night/Evening -> Home
                lat = home_lat + np.random.normal(0, 0.0001)
                lon = home_lon + np.random.normal(0, 0.0001)
            elif 9 <= hour <= 17 and ts.weekday() < 5: # Work hours -> Work
                lat = work_lat + np.random.normal(0, 0.0001)
                lon = work_lon + np.random.normal(0, 0.0001)
            else: # Random/Leisure
                lat = base_lat + random.uniform(-0.05, 0.05)
                lon = base_lon + random.uniform(-0.05, 0.05)
                
            data.append({
                'maid': maid_id,
                'latitude': lat,
                'longitude': lon,
                'timestamp': ts.strftime('%Y-%m-%d %H:%M:%S+07:00'), # Local time
                'flux': random.choice(['A', 'B'])
            })
            
    df = pd.DataFrame(data)
    print(f"Generated {len(df)} pings for {num_maids} MAIDs.")
    print(df.groupby('maid').size())
    
    output_path = 'dense_maids.csv'
    df.to_csv(output_path, index=False)
    print(f"Saved to {output_path}")

if __name__ == "__main__":
    generate_dense_data()
