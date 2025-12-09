-- Database cleanup (Comprehensive)
DROP TABLE IF EXISTS view_aggregated_data;
DROP TABLE IF EXISTS mv_raw_to_agg;      -- Old MV name
DROP TABLE IF EXISTS mv_raw_to_state;    -- Conflicting MV from architecture_new.md experiment
DROP TABLE IF EXISTS maid_state_agg;     -- Old State Table
DROP TABLE IF EXISTS mv_raw_to_geohash_agg;
DROP TABLE IF EXISTS maid_geohash_state;
DROP TABLE IF EXISTS raw_maid_pings;

-- 1. Raw Data Table (Unchanged)
CREATE TABLE IF NOT EXISTS raw_maid_pings (
    maid String,
    timestamp DateTime64(3) CODEC(Delta(4), ZSTD(1)),
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
    first_seen SimpleAggregateFunction(min, DateTime64(3)),
    last_seen SimpleAggregateFunction(max, DateTime64(3)),
    
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

-- 4. Final Aggregated View (Complex Scoring)
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
            arrayDifference(sorted_ts) AS gaps,
            arrayMap(t -> toHour(t), sorted_ts) AS hours,
            arrayMap(t -> toDayOfWeek(t), sorted_ts) AS weekdays, 
            arrayMap(h -> countEqual(hours, h), range(24)) AS hour_counts,
            arrayMap(d -> countEqual(weekdays, d), range(1, 8)) AS weekday_counts
        FROM gh_calc_1
    ),
    gh_metrics AS (
        SELECT
            *,
            -- Continuity
            (countEqual(arrayMap(g -> g > 0 AND g <= 86400, gaps), 1) + countEqual(arrayMap(g -> g > 86400 AND g <= 86400*3, gaps), 1)) / if(length(gaps) > 1, length(gaps)-1, 1) AS continuity_factor,
            base_active_ratio * (0.5 + 0.5 * continuity_factor) AS active_day_ratio,
            
            -- Ratios (Ping-based)
            (hour_counts[23] + hour_counts[24] + hour_counts[1] + hour_counts[2] + hour_counts[3] + hour_counts[4] + hour_counts[5] + hour_counts[6]) / n_pings AS night_ratio,
            arraySum(arraySlice(hour_counts, 10, 9)) / n_pings AS weekday_day_approx_ratio,
            (hour_counts[19] + hour_counts[20] + hour_counts[21] + hour_counts[22]) / n_pings AS evening_ratio,
            (weekday_counts[6] + weekday_counts[7]) / n_pings AS weekend_ratio,
            
            -- Day-based Ratios
            length(arrayDistinct(arrayMap(t -> toUInt32(toYYYYMMDD(t)), arrayFilter(t -> toHour(t) >= 22 OR toHour(t) <= 5, sorted_ts)))) / unique_days AS night_days_ratio,
            length(arrayDistinct(arrayMap(t -> toUInt32(toYYYYMMDD(t)), arrayFilter(t -> toHour(t) >= 20 AND toHour(t) <= 23, sorted_ts)))) / unique_days AS late_evening_days_ratio,
            length(arrayDistinct(arrayMap(t -> toUInt32(toYYYYMMDD(t)), arrayFilter(t -> toHour(t) >= 4 AND toHour(t) <= 6, sorted_ts)))) / unique_days AS early_morning_days_ratio,
            length(arrayDistinct(arrayMap(t -> toUInt32(toYYYYMMDD(t)), arrayFilter(t -> toDayOfWeek(t) <= 5 AND toHour(t) >= 9 AND toHour(t) <= 17, sorted_ts)))) / unique_days AS weekday_work_days_ratio,
            
            -- Entropy
            -1 * arraySum(arrayMap(x -> if(x=0, 0.0, (x/n_pings) * log2(x/n_pings)), hour_counts)) / 4.585 AS entropy_hour_norm
            
        FROM gh_calc_2
    ),
    gh_scored AS (
        SELECT
            *,
            -- Weight Constants
            2.0 AS a,
            (n_pings / 5.0) AS w_visits_exp,
            (unique_days / 3.0) AS w_days_exp,
            (1.0 - exp(-w_visits_exp)) AS w_visits,
            (1.0 - exp(-w_days_exp)) AS w_days,
            
            -- HOME calculation
            (night_ratio * n_pings + 2.0 * 0.333) / (n_pings + 2.0) AS night_shrunk,
             (
                0.375 * night_days_ratio +
                0.10 * night_shrunk +
                0.15 * late_evening_days_ratio + 
                0.10 * early_morning_days_ratio +
                0.075 * (1.0 - entropy_hour_norm) + 
                0.25 * active_day_ratio
             ) * w_visits * w_days AS home_score_raw,
             if(home_score_raw > 1.0, 1.0, home_score_raw) AS home_score,
             
             -- WORK calculation
             (weekday_day_approx_ratio * n_pings + 2.0 * 0.267) / (n_pings + 2.0) AS work_shrunk,
             (
                0.425 * weekday_work_days_ratio + 
                0.15 * work_shrunk + 
                0.075 * (1.0 - entropy_hour_norm) + 
                0.20 * active_day_ratio
             ) * w_visits * w_days AS work_score_raw,
             if(work_score_raw > 1.0, 1.0, work_score_raw) AS work_score,
             
             -- LEISURE calculation
             (weekend_ratio * n_pings + 2.0 * 0.286) / (n_pings + 2.0) AS weekend_shrunk,
             (evening_ratio * n_pings + 2.0 * 0.167) / (n_pings + 2.0) AS evening_shrunk,
             (1.0 - (home_score + work_score)/2.0) AS inverse_pattern,
             
             (
                0.25 * weekend_shrunk + 
                0.20 * evening_shrunk + 
                0.15 * (1.0 - entropy_hour_norm) +
                0.10 * 0.0 + -- monthly stability placeholder (no historical bins yet)
                0.30 * inverse_pattern
             ) * w_visits * w_days AS leisure_score_raw,
             
             -- Apply active days scaling for leisure (using / 15.0)
             -- min(1.0, 0.5 + 0.5 * (active_days_last_30d / 15.0))
             -- approximating active_days logic similarly to home/work if needed, or using same w_visits
             if(leisure_score_raw > 1.0, 1.0, leisure_score_raw) AS leisure_score,

             -- PINGSINK calculation
             -- std_geohash_m calc
             -- var_lat = (sum_sq_lat / N) - (mean_lat)^2
             (total_lat_sq / n_pings) - (mean_lat * mean_lat) AS var_lat,
             (total_lon_sq / n_pings) - (mean_lon * mean_lon) AS var_lon,
             sqrt(greatest(0.0, var_lat + var_lon)) * 111139.0 AS std_geohash_m, -- approx deg to meters
             
             if(std_geohash_m = 0, 1.0, 0.7 * exp(-std_geohash_m / 20.0)) AS geo_stability,
             
             -- temporal density (mean_time_diff) - complex to do exactly on array in SQL efficiently, 
             -- skipping accurate mean_diff for now or approximating? 
             -- Let's try to approximate avg gap from n_pings / span_days
             -- mean_diff_seconds ~ (span_days * 86400) / n_pings
             (span_days_raw * 86400.0) / greatest(1, n_pings) AS mean_diff_approx,
             0.1 * exp(-(mean_diff_approx / 60.0) / 60.0) AS temporal_density,
             
             (1.0 - exp(-n_pings / 50.0)) AS ping_factor,
             
             (geo_stability + temporal_density + 0.2 * ping_factor) AS pingsink_base,
             -- Relative importance needs total pings for MAID, which we don't have in this per-gh row context easily 
             -- without a window function or join. 
             -- APPROXIMATION: Assume per-geohash score is independent of total maid volume for now, 
             -- OR simply return base score.
             if(n_pings <= 5, 0.0, pingsink_base) AS pingsink_score
             
        FROM gh_metrics
    )
SELECT
    maid,
    count() AS total_geohashes,
    sum(pings) AS total_pings,
    groupArray(geohash) AS geohash,
    groupArray(pings) AS pings_array,
    groupArray(mean_lat) AS mean_lat,
    groupArray(mean_lon) AS mean_lon,
    groupArray(entropy_hour_norm) AS entropy_hour_norm,
    groupArray(home_score) AS home_score,
    groupArray(work_score) AS work_score,
    groupArray(leisure_score) AS leisure_score,
    groupArray(pingsink_score) AS pingsink_score,
    groupArray(std_geohash_m) AS std_geohash_m
FROM gh_scored
GROUP BY maid;
