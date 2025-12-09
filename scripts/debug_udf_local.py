import struct
import subprocess
import os

def debug_udf():
    # pings=100 (u64), std=10.0 (f64), time=3600.0 (f64), total=1000 (u64)
    data = struct.pack('<QddQ', 100, 10.0, 3600.0, 1000)
    
    input_file = 'test_input.bin'
    with open(input_file, 'wb') as f:
        f.write(data)
        
    print(f"Created {input_file} with {len(data)} bytes")
    
    # Run tool
    cmd = ['./clickhouse_udf/bin/scoring_tool']
    print(f"Running {' '.join(cmd)}...")
    
    process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate(input=data)
    
    print(f"Return code: {process.returncode}")
    if stderr:
        print(f"Stderr: {stderr.decode()}")
        
    if stdout:
        print(f"Stdout length: {len(stdout)} bytes")
        if len(stdout) == 8:
            result = struct.unpack('<d', stdout)[0]
            print(f"Result: {result}")
        else:
            print("Stdout hex:", stdout.hex())
    else:
        print("No stdout")

if __name__ == "__main__":
    debug_udf()
