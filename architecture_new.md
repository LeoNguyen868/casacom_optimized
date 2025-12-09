# ClickHouse ingestion and feature store (raw → stored → aggregated)

This doc captures the DDL and materialized views for the new flow described in `new_migartion.md`. The design follows the MergeTree guidance in `ClickHouse/en/engines/table-engines/mergetree-family/mergetree.md`, `aggregatingmergetree.md`, and the `sumMap` aggregate function in `ClickHouse/en/sql-reference/aggregate-functions/reference/summap.md`.

## Tables

### 1) Raw ingress: `raw_maid_pings`

Purpose: lossless landing zone with short TTL. All downstream state is derived from here.

```sql
CREATE TABLE IF NOT EXISTS raw_maid_pings (
    maid String CODEC(ZSTD(3)),
    event_time DateTime64(3, 'UTC') CODEC(Delta(4), ZSTD(1)),
    latitude Float64 CODEC(Gorilla, ZSTD(1)),
    longitude Float64 CODEC(Gorilla, ZSTD(1)),
    flux LowCardinality(String),
    geohash String MATERIALIZED geohashEncode(longitude, latitude, 7),
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toDate(event_time)
ORDER BY (maid, event_time)
TTL event_time + INTERVAL 7 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
```

Notes:
- Daily partitioning keeps TTL drops cheap while preserving locality for time-range scans.
- The materialized `geohash` reduces CPU cost in MVs.
- Add optional data skipping index if needed later, e.g. `INDEX maid_bf maid TYPE bloom_filter(0.01) GRANULARITY 4`.

### 2) Aggregation state: `maid_state_agg`

Purpose: replace `stored_data_new.json` with incremental aggregate states.

```sql
CREATE TABLE IF NOT EXISTS maid_state_agg (
    maid String,

    total_pings SimpleAggregateFunction(sum, UInt64),
    first_seen SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    last_seen SimpleAggregateFunction(max, DateTime64(3, 'UTC')),

    -- Time history for gap/entropy; keep as array state (consider sampling if bots appear)
    time_points_state AggregateFunction(groupArray, DateTime64(3, 'UTC')),
    day_bitmap_state AggregateFunction(groupBitmap, UInt32),

    -- Geohash ↦ ping counts
    geohash_map_state AggregateFunction(sumMap, Array(String), Array(UInt64)),

    -- Sums used to derive averages
    sum_lat SimpleAggregateFunction(sum, Float64),
    sum_lon SimpleAggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(last_seen)
ORDER BY maid
TTL last_seen + INTERVAL 400 DAY DELETE
SETTINGS index_granularity = 8192;
```

Notes:
- `groupBitmap` holds active days efficiently; use `toUInt32(toDate(event_time))` when populating.
- If array explosion appears, switch `time_points_state` to `groupArraySample(N)` or add a `histogram` state column for hours.

### 3) Final features: `maid_aggregated`

Purpose: durable feature table ready for analytics/serving. Values are materialized (no AggregateFunction types).

```sql
CREATE TABLE IF NOT EXISTS maid_aggregated (
    maid String,

    total_pings UInt64,
    unique_geohash UInt32,
    geohash Array(String),
    pings Array(UInt64),

    first_seen DateTime64(3, 'UTC'),
    last_seen DateTime64(3, 'UTC'),
    span_days UInt16,
    unique_days UInt32,
    active_day_ratio Float64,

    gap_bins_0d UInt64,
    gap_bins_1_3d UInt64,
    gap_bins_4_7d UInt64,
    gap_bins_8_30d UInt64,
    gap_bins_gt_30d UInt64,

    night_ratio Float64,
    weekend_ratio Float64,
    entropy_hour_norm Float64,
    monthly_stability Float64,

    mean_lat Float64,
    mean_lon Float64,

    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(last_seen)
ORDER BY maid
TTL last_seen + INTERVAL 730 DAY DELETE
SETTINGS index_granularity = 8192;
```

Notes:
- `ReplacingMergeTree` keeps the latest recomputation per `maid`; reruns of the second MV can safely overwrite.
- TTL is longer than state to preserve derived features even if raw/state expire.

## Materialized Views

### MV 1: raw → state (`mv_raw_to_state`)

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_raw_to_state
TO maid_state_agg AS
SELECT
    maid,
    count() AS total_pings,
    min(event_time) AS first_seen,
    max(event_time) AS last_seen,
    groupArrayState(event_time) AS time_points_state,
    groupBitmapState(toUInt32(toDate(event_time))) AS day_bitmap_state,
    sumMapState([geohash], [toUInt64(1)]) AS geohash_map_state,
    sum(latitude) AS sum_lat,
    sum(longitude) AS sum_lon
FROM raw_maid_pings
GROUP BY maid;
```

Rationale:
- Uses `sumMapState` to keep key/value arrays (per `sumMap` docs) so merges stay associative.
- Keeps the MV stateless per insert block; AggregatingMergeTree merges handle global rollup.

### MV 2: state → aggregated (`mv_state_to_agg`)

Compute final metrics from states. The heavy operations (arraySort/arrayDifference) run once per maid instead of per query.

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_state_to_agg
TO maid_aggregated AS
(
SELECT
    maid,
    total_pings,
    geohash_tuple.1 AS geohash,
    geohash_tuple.2 AS pings,
    length(geohash_tuple.1) AS unique_geohash,
    first_seen,
    last_seen,
    span_days,
    unique_days,
    active_day_ratio,
    gap_bins_0d,
    gap_bins_1_3d,
    gap_bins_4_7d,
    gap_bins_8_30d,
    gap_bins_gt_30d,
    night_ratio,
    weekend_ratio,
    entropy_hour_norm,
    monthly_stability,
    if(total_pings = 0, 0, sum_lat / total_pings) AS mean_lat,
    if(total_pings = 0, 0, sum_lon / total_pings) AS mean_lon,
    now() AS updated_at
FROM
(
    SELECT
        maid,
        sumMerge(total_pings) AS total_pings,
        minMerge(first_seen) AS first_seen,
        maxMerge(last_seen) AS last_seen,
        sumMerge(sum_lat) AS sum_lat,
        sumMerge(sum_lon) AS sum_lon,
        sumMapMerge(geohash_map_state) AS geohash_tuple,
        groupArrayMerge(time_points_state) AS ts_all,
        groupBitmapMerge(day_bitmap_state) AS day_bitmap,

        /* Derived metrics in-place */
        arraySort(ts_all) AS ts_sorted,
        arrayDifference(ts_sorted) AS gaps,
        arrayCount(x -> x <= 0, gaps) AS gap_bins_0d,
        arrayCount(x -> x > 0 AND x <= 86400 * 3, gaps) AS gap_bins_1_3d,
        arrayCount(x -> x > 86400 * 3 AND x <= 86400 * 7, gaps) AS gap_bins_4_7d,
        arrayCount(x -> x > 86400 * 7 AND x <= 86400 * 30, gaps) AS gap_bins_8_30d,
        arrayCount(x -> x > 86400 * 30, gaps) AS gap_bins_gt_30d,

        cardinality(day_bitmap) AS unique_days,
        greatest(1, dateDiff('day', toDate(first_seen), toDate(last_seen)) + 1) AS span_days,
        if(span_days = 0, 0, unique_days / span_days) AS active_day_ratio,

        arrayCount(t -> toHour(t) >= 22 OR toHour(t) < 6, ts_sorted) / nullIf(length(ts_sorted), 0) AS night_ratio,
        arrayCount(t -> toDayOfWeek(t) IN (6, 7), ts_sorted) / nullIf(length(ts_sorted), 0) AS weekend_ratio,

        /* Entropy on hour-of-day */
        arrayMap(t -> toHour(t), ts_sorted) AS hours,
        arrayMap(h -> countEqual(hours, h), range(24)) AS hour_counts,
        arraySum(hour_counts) AS hour_total,
        -arraySum(arrayMap(p -> if(p = 0, 0, p * log2(p)), arrayMap(c -> if(hour_total = 0, 0, c / hour_total), hour_counts))) / log2(24) AS entropy_hour_norm,

        /* Month stability: 1 - (stddev / mean), clamped */
        arrayMap(m -> countEqual(arrayMap(t -> toUInt32(toYYYYMM(t)), ts_sorted), m), arrayDistinct(arrayMap(t -> toUInt32(toYYYYMM(t)), ts_sorted))) AS month_counts,
        arrayAvg(month_counts) AS month_avg,
        arrayReduce('stddevPop', month_counts) AS month_std,
        if(month_avg = 0, 1, greatest(0., 1 - month_std / month_avg)) AS monthly_stability
    FROM maid_state_agg
    GROUP BY maid
);
```

Implementation notes:
- Derived metrics are computed inline; if memory spikes appear, switch `ts_all` to a sampled version or precompute hour histograms.
- If MV is too heavy at insert time, switch it to a periodically refreshed `MATERIALIZED VIEW … POPULATE` or to a `CREATE TABLE AS SELECT` scheduled job.

### Optional fast-path (raw → aggregated)

For cheap, latency-sensitive counts (e.g., total pings per day), add a second MV that bypasses state:

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_raw_daily_rollup
TO maid_aggregated_daily AS
SELECT
    maid,
    toDate(event_time) AS event_date,
    count() AS daily_pings,
    uniqExact(geohash) AS daily_unique_geohash,
    now() AS updated_at
FROM raw_maid_pings
GROUP BY maid, event_date;
```

Use a `SummingMergeTree` on `maid_aggregated_daily` for compact daily rollups if needed.

## Operational tips
- Keep `optimize_on_insert = 1` (default) so MV1 pre-aggregates per block; if ingest batches are small, disable it and rely on background merges for better compression.
- Re-cluster only if ORDER BY changes; otherwise rely on background merges.
- To backfill, load historical data into `raw_maid_pings` with `INSERT SELECT` and let the MVs rebuild automatically.

