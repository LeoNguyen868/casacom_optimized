# Evidence Processing Flow

This document explains how `envidence_new.py` processes data from raw pings to aggregated evidence. It references the sample files `sample_data_input.csv`, `stored_data.json`, and `aggregrated_data.json` (note the filename spelling in the repository).

## 1) Input Data (`sample_data_input.csv`)
- Columns: `maid`, `geohash` (precision 7), `timestamp` (UTC), `latitude`, `longitude`, `flux`.
- Upstream step groups rows by `geohash` (and MAID) to prepare:
  - `new_data`: `{gh7: [timestamps...]}` with timestamps converted to UTC (`_to_dt`).
  - `geohashes`: `{gh7: [geohash12...]}` computed from lat/lon for spatial mean/std.
  - `flux_data`: `{gh7: [flux types...]}` to count flux occurrences.

## 2) Incremental Update (`EvidenceStore.update`)
Called per MAID to merge a batch:
- Initializes a geohash bucket if missing (`_init`).
- Updates spatial stats via pygeohash: mean geohash, M2 accumulator, std in meters; also keeps `mean_lat`/`mean_lon`.
- Tracks pings and temporal histograms: hourly, weekday, weekend vs. weekday, monthly counts.
- Flags each day with bitmasks for night/late-evening/early-morning/weekday-work presence; builds `gap_bins` for inter-day gaps.
- Maintains `hourly_minutes` (min/max minute seen per hour) and recalculates `est_duration` as the sum of per-hour ranges.
- Computes inter-ping time deltas to derive `mean_time_diff_seconds`.
- Counts flux types (`flux_counts`) when provided.
- After all geohashes, refreshes `total_pings` for the MAID.
- Persistence: `save`/`load` wrap pickle (optionally gzip) with atomic writes and retry on load.
- `stored_data.json` illustrates a stored snapshot after such an update.

## 3) Aggregation / Derivation (`derive` and `overall_score`)
For each geohash, `derive` produces a structured summary:
- `meta`: first/last seen, span days, mean coordinate/geohash, std (meters), mean time diff.
- `level_1_primary`: total pings, unique days, active_day_ratio (span-capped), gap_bins.
- `level_2_secondary`: hourly/weekday/monthly histograms and ratios (night, weekday-day, weekend, midday weekday, evening), entropy of hourly distribution, monthly stability (1/(1+CV)), active days in the last 30 days.
- `level_3_tertiary`: POI availability/info (if previously computed).
- `level_4_duration`: `est_duration` and the per-hour minute ranges.
- `level_5_flux`: flux_counts.

`overall_score` combines the derived stats into category scores:
- Home/Work/Leisure scores use Bayesian shrinkage priors, sample-size weights, entropy, continuity/recency boosts.
- `score_pingsink` favors stable locations with low spatial std, dense time gaps, and higher ping volume relative to `total_pings`.
- If POI is available and not `path`, its confidence can blend into the relevant score.

`aggregrated_data.json` shows an example of the fully derived structure for a MAID: top-level `maid`, `total_pings`, and per-geohash blocks containing `meta`, `level_1..5`.

## 4) Typical Flow
1. Read CSV → group by geohash/MAID; build `new_data`, `geohashes`, `flux_data`.
2. Call `EvidenceStore.update(new_data, geohashes, flux_data)` to merge incrementally.
3. Optionally save the store (pickle/gzip) for later runs.
4. For reporting/scoring, call `derive(gh)` for each geohash and feed results to `overall_score` (plus optional POI blending). Persist or export summaries (as in `aggregrated_data.json`).

