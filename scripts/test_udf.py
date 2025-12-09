import clickhouse_connect
import time

def test_udf():
    client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='default')
    
    print("Reloading functions...")
    try:
        client.command("SYSTEM RELOAD FUNCTIONS")
        client.command("SYSTEM RELOAD CONFIG")
    except Exception as e:
        print(f"Warning during reload: {e}")

    # Test case
    # Pings: 100
    # Std: 10.0 -> geo_stability = 0.7 * exp(-10/20) = 0.7 * exp(-0.5) = 0.7 * 0.6065 = 0.4245
    # TimeDiff: 3600s -> 60m -> temp_density = 0.1 * exp(-60/60) = 0.1 * 0.3678 = 0.0368
    # Pings factor: 1 - exp(-100/50) = 1 - 0.1353 = 0.8647 -> contrib = 0.2 * 0.8647 = 0.1729
    # Base = 0.4245 + 0.0368 + 0.1729 = 0.6342
    # Multiplier: pings/total = 100/1000 = 0.1. sqrt(0.1) = 0.316. 
    # Mult = 0.8 + 0.2 * 0.316 = 0.8 + 0.0632 = 0.8632
    # Final = 0.6342 * 0.8632 = 0.5474
    
    print("Running test query...")
    try:
        # Explicitly cast arguments to match UDF definition (RowBinary is strict)
        query = """
        SELECT score_pingsink_cpp(
            toUInt64(100), 
            toFloat64(10.0), 
            toFloat64(3600.0), 
            toUInt64(1000)
        )
        """
        result = client.query(query).result_rows
        print(f"Result Pingsink: {result[0][0]}")
        
        # Test Spatial
        print("Testing Spatial Stats...")
        # Lats: [10.0, 10.0, 10.0] -> Mean 10.0, Std 0
        # Lats: [10.0, 11.0] -> Mean 10.5. Dist(10, 10.5) approx 55km.
        query_spatial = """
        SELECT calculate_spatial_stats_cpp(
            [toFloat64(10.0), toFloat64(11.0)],
            [toFloat64(100.0), toFloat64(100.0)]
        )
        """
        result_spatial = client.query(query_spatial).result_rows
        print(f"Result Spatial: {result_spatial[0][0]}")
        
    except Exception as e:
        print(f"Query failed: {e}")

if __name__ == "__main__":
    test_udf()
