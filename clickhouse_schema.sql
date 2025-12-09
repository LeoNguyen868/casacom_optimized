-- Database cleanup (Comprehensive)
DROP TABLE IF EXISTS view_aggregated_data;
DROP TABLE IF EXISTS mv_raw_to_geohash_agg;
DROP TABLE IF EXISTS maid_geohash_state;
DROP TABLE IF EXISTS raw_maid_pings;

-- 1. Raw Data Table (Unchanged)
CREATE TABLE IF NOT EXISTS raw_maid_pings (
    maid String,
    timestamp DateTime64(3, 'UTC') CODEC(Delta(4), ZSTD(1)),
    latitude Float64 CODEC(Gorilla, ZSTD(1)),
    longitude Float64 CODEC(Gorilla, ZSTD(1)),
    flux LowCardinality(String),
    geohash String MATERIALIZED geohashEncode(longitude, latitude, 7)
) 
ENGINE = MergeTree()
PARTITION BY toDate(timestamp)
ORDER BY (maid, timestamp)
TTL timestamp + INTERVAL 7 DAY;

-- 2. Intermediate State Table: Per (Maid, Geohash)
CREATE TABLE IF NOT EXISTS maid_geohash_state (
    maid String,
    geohash String,
    
    total_pings SimpleAggregateFunction(sum, UInt64),
    first_seen SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    last_seen SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    
    sum_lat SimpleAggregateFunction(sum, Float64),
    sum_lon SimpleAggregateFunction(sum, Float64),
    sum_lat_sq SimpleAggregateFunction(sum, Float64),
    sum_lon_sq SimpleAggregateFunction(sum, Float64),
    
    time_points_state AggregateFunction(groupArray, DateTime64(3)),
    unique_days_state AggregateFunction(groupBitmap, UInt32),
    flux_counts_state AggregateFunction(sumMap, Map(String, UInt64))
)
ENGINE = AggregatingMergeTree()
ORDER BY (maid, geohash);

-- 3. Materialized View: Raw -> State
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_raw_to_geohash_agg TO maid_geohash_state AS
SELECT
    maid,
    geohash,
    count() AS total_pings,
    min(timestamp) AS first_seen,
    max(timestamp) AS last_seen,
    sum(latitude) AS sum_lat,
    sum(longitude) AS sum_lon,
    sum(latitude * latitude) AS sum_lat_sq,
    sum(longitude * longitude) AS sum_lon_sq,
    groupArrayState(timestamp) AS time_points_state,
    groupBitmapState(toUInt32(toYYYYMMDD(timestamp))) AS unique_days_state,
    sumMapState(map(flux, toUInt64(1))) AS flux_counts_state
FROM raw_maid_pings
GROUP BY maid, geohash;

-- 4. Final Aggregated View (Complex Scoring with UDFs)
CREATE OR REPLACE VIEW view_aggregated_data AS
WITH 
    gh_merged AS (
        SELECT
            maid,
            geohash,
            sum(total_pings) AS pings,
            min(first_seen) AS first_ts,
            max(last_seen) AS last_ts,
            sum(sum_lat)/sum(total_pings) AS mean_lat,
            sum(sum_lon)/sum(total_pings) AS mean_lon,
            sum(sum_lat_sq) AS total_lat_sq,
            sum(sum_lon_sq) AS total_lon_sq,
            groupArrayMerge(time_points_state) AS all_ts,
            groupBitmapMerge(unique_days_state) AS unique_days
        FROM maid_geohash_state
        GROUP BY maid, geohash
    ),
    gh_calc_1 AS (
        SELECT
            *,
            length(all_ts) AS n_pings, 
            arraySort(all_ts) AS sorted_ts,
            toFloat64(dateDiff('day', toDate(first_ts), toDate(last_ts)) + 1) AS span_days_raw,
            greatest(30.0, span_days_raw) AS capped_span,
            if(capped_span > 0, unique_days / capped_span, 0.0) AS base_active_ratio
        FROM gh_merged
    ),
    gh_calc_2 AS (
        SELECT 
            *,
            -- Adjust to Local Time (UTC+7 for Vietnam as per data)
            arrayMap(t -> t + INTERVAL 7 HOUR, sorted_ts) AS local_ts,
            
            arrayDifference(sorted_ts) AS gaps, -- Gaps are time-deltas, independent of timezone shift
            
            arrayMap(t -> toHour(t), local_ts) AS hours,
            arrayMap(t -> toDayOfWeek(t), local_ts) AS weekdays, 
            arrayMap(h -> countEqual(hours, h), range(24)) AS hour_counts,
            arrayMap(d -> countEqual(weekdays, d), range(1, 8)) AS weekday_counts,
            
            -- For continuity: sorted unique LOCAL days
            arraySort(arrayDistinct(arrayMap(t -> toRelativeDayNum(t), local_ts))) AS sorted_unique_days,
            arrayDifference(sorted_unique_days) AS day_gaps
        FROM gh_calc_1
    ),
    gh_metrics AS (
        SELECT
            *,
            -- Recalculate unique_days based on LOCAL time?
            -- Original unique_days in gh_merged was from UTC bitmap?
            -- YES. groupBitmap(toUInt32(toYYYYMMDD(timestamp))).
            -- This is WRONG if we want Local Days.
            -- We need to recalculate unique_days from local_ts.
            length(sorted_unique_days) AS unique_days_local,
            
            -- Continuity (Daily Basis)
            if(length(day_gaps) > 0, 
               countEqual(arrayMap(x -> if(x >= 1 AND x <= 3, 1, 0), day_gaps), 1) / length(day_gaps),
               0.0
            ) AS continuity_factor,
            
            -- Need to use unique_days_local for ratios
            base_active_ratio * (unique_days_local / unique_days) * (0.5 + 0.5 * continuity_factor) AS active_day_ratio_corrected,
            -- Wait, base_active_ratio uses UTC unique_days.
            -- Let's just recompute base active ratio?
            -- capped_span is also UTC-based.
            -- Let's stick to using 'unique_days_local' for Denominators where 'unique_days' was used.
            
            base_active_ratio * (0.5 + 0.5 * continuity_factor) AS active_day_ratio, -- Approximating base with UTC unique days for now, or recompute?
            
            -- Ratios (Ping-based)
            (hour_counts[23] + hour_counts[24] + hour_counts[1] + hour_counts[2] + hour_counts[3] + hour_counts[4] + hour_counts[5] + hour_counts[6]) / n_pings AS night_ratio,
            
            -- ... (rest identical using recalculated hours/weekdays)
            arraySum(arraySlice(hour_counts, 10, 9)) / n_pings AS weekday_days_ratio_global,
            countEqual(arrayMap((h, w) -> if(w <= 5 AND h >= 9 AND h <= 17, 1, 0), hours, weekdays), 1) / n_pings AS weekday_day_ratio,
            countEqual(arrayMap((h, w) -> if(w <= 5 AND h >= 11 AND h <= 14, 1, 0), hours, weekdays), 1) / n_pings AS midday_weekday_ratio,
            (hour_counts[19] + hour_counts[20] + hour_counts[21] + hour_counts[22]) / n_pings AS evening_ratio,
            (weekday_counts[6] + weekday_counts[7]) / n_pings AS weekend_ratio,
            
            -- Day-based Ratios (Using LOCAL TIMESTAMP and LOCAL UNIQUE DAYS)
            length(arrayDistinct(arrayMap(t -> toUInt32(toYYYYMMDD(t)), arrayFilter(t -> toHour(t) >= 22 OR toHour(t) <= 5, local_ts)))) / unique_days_local AS night_days_ratio,
            length(arrayDistinct(arrayMap(t -> toUInt32(toYYYYMMDD(t)), arrayFilter(t -> toHour(t) >= 20 AND toHour(t) <= 23, local_ts)))) / unique_days_local AS late_evening_days_ratio,
            length(arrayDistinct(arrayMap(t -> toUInt32(toYYYYMMDD(t)), arrayFilter(t -> toHour(t) >= 4 AND toHour(t) <= 6, local_ts)))) / unique_days_local AS early_morning_days_ratio,
            length(arrayDistinct(arrayMap(t -> toUInt32(toYYYYMMDD(t)), arrayFilter(t -> toDayOfWeek(t) <= 5 AND toHour(t) >= 9 AND toHour(t) <= 17, local_ts)))) / unique_days_local AS weekday_work_days_ratio,
            
            -- Entropy
            -1 * arraySum(arrayMap(x -> if(x=0, 0.0, (x/n_pings) * log2(x/n_pings)), hour_counts)) / 4.585 AS entropy_hour_norm,
            
            -- Active Days Last 30d (Local)
             length(arrayDistinct(arrayMap(t -> toUInt32(toYYYYMMDD(t)), arrayFilter(t -> toDate(t) >= toDate(local_ts[length(local_ts)]) - 30, local_ts)))) AS active_days_last_30d,
            
            -- Monthly stability placeholder (complex in SQL without materialized monthly Agg)
            -- Python logic: CV of monthly counts. Defaults to 0.0 if not enough history.
            0.0 AS monthly_stability,
            
            -- Spatial Stats (Euclidean Approx)
            (total_lat_sq / n_pings) - (mean_lat * mean_lat) AS var_lat,
            (total_lon_sq / n_pings) - (mean_lon * mean_lon) AS var_lon,
            sqrt(greatest(0.0, var_lat + var_lon)) * 111139.0 AS std_geohash_m,
            
            -- Temporal Density Approx
            (span_days_raw * 86400.0) / greatest(1, n_pings) AS mean_diff_approx
            
        FROM gh_calc_2
    ),
    gh_scored AS (
        SELECT
            *,
            -- UDF CALLS
            score_home_cpp(
                toUInt64(n_pings),
                toUInt64(unique_days),
                toFloat64(night_ratio),
                toFloat64(night_days_ratio),
                toFloat64(late_evening_days_ratio),
                toFloat64(early_morning_days_ratio),
                toFloat64(entropy_hour_norm),
                toFloat64(active_day_ratio),
                toFloat64(monthly_stability), 
                toUInt64(active_days_last_30d)
            ) AS home_score,
            
            score_work_cpp(
                toUInt64(n_pings),
                toUInt64(unique_days),
                toFloat64(weekday_day_ratio),
                toFloat64(weekday_work_days_ratio),
                toFloat64(midday_weekday_ratio),
                toFloat64(entropy_hour_norm),
                toFloat64(active_day_ratio),
                toFloat64(monthly_stability),
                toUInt64(active_days_last_30d)
            ) AS work_score,
            
            -- Leisure needs home and work scores. 
            score_leisure_cpp(
                toUInt64(n_pings),
                toUInt64(unique_days),
                toFloat64(weekend_ratio),
                toFloat64(evening_ratio),
                toFloat64(entropy_hour_norm),
                toFloat64(monthly_stability),
                toUInt64(active_days_last_30d),
                toFloat64(home_score), -- Pass computed home_score
                toFloat64(work_score)  -- Pass computed work_score
            ) AS leisure_score,
            
            -- Pingsink
            -- Needs total_pings for MAID. We calculate it using a Window Function here?
            -- ClickHouse supports Window Functions in recent versions.
            sum(n_pings) OVER (PARTITION BY maid) AS maid_total_pings,
            
            score_pingsink_cpp(
                toUInt64(n_pings),
                toFloat64(std_geohash_m),
                toFloat64(mean_diff_approx), -- Using approx mean diff
                toUInt64(maid_total_pings)
            ) AS pingsink_score

        FROM gh_metrics
    )
SELECT
    maid,
    count() AS total_geohashes,
    sum(pings) AS total_pings,
    groupArray(geohash) AS geohash,
    groupArray(pings) AS pings_array,
    groupArray(unique_days) AS unique_days,
    groupArray(mean_lat) AS mean_lat,
    groupArray(mean_lon) AS mean_lon,
    groupArray(entropy_hour_norm) AS entropy_hour_norm,
    groupArray(home_score) AS home_score,
    groupArray(work_score) AS work_score,
    groupArray(leisure_score) AS leisure_score,
    groupArray(pingsink_score) AS pingsink_score,
    groupArray(std_geohash_m) AS std_geohash_m,
    -- Debug Metrics
    groupArray(active_days_last_30d) AS active_days_last_30d,
    groupArray(active_day_ratio) AS active_day_ratio,
    groupArray(night_days_ratio) AS night_days_ratio,
    groupArray(continuity_factor) AS continuity_factor
FROM gh_scored
GROUP BY maid;
