# ClickHouse C++ UDF Implementation

This project implements high-precision scoring functions and spatial statistics for ClickHouse using C++ Executable UDFs.

## Components

1.  **C++ Tool (`scoring_tool`)**:
    *   Located in `clickhouse_udf/`.
    *   Implements `pingsink`, `home`, `work`, `leisure`, and `spatial` modes.
    *   Uses `RowBinary` format for efficient, lossless data exchange.
2.  **Configuration (`evidence_function.xml`)**:
    *   Defines the UDF signatures in ClickHouse.
    *   Maps SQL functions (e.g., `score_pingsink_cpp`) to the C++ executable.

## Deployment

To compile and deploy the UDFs to your local ClickHouse Docker container, run:

```bash
./clickhouse_setup.sh
```

This script will:
1.  Compile the C++ code into a static binary (`clickhouse_udf/bin/scoring_tool`).
2.  Copy the binary to the ClickHouse container (`/var/lib/clickhouse/user_scripts/`).
3.  Copy the XML configuration to the ClickHouse container.
4.  Reload ClickHouse functions to apply changes.

## Available Functions

| Function | Description |
| :--- | :--- |
| `score_pingsink_cpp(...)` | Calculates pingsink score |
| `score_home_cpp(...)` | Calculates home score |
| `score_work_cpp(...)` | Calculates work score |
| `score_leisure_cpp(...)` | Calculates leisure score |
| `calculate_spatial_stats_cpp(lats, lons)` | Returns `(mean_lat, mean_lon, std_m)` |

## Verification

To verify the installation, run the python test script:

```bash
python3 scripts/test_udf.py
```
