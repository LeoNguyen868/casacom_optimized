import datetime as dt
import json
import math
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence
import pygeohash as pgh

# Columnar, no backward-compat: all fields are simple scalars per geohash row.

NIGHT_HOURS = {22, 23, 0, 1, 2, 3, 4, 5}
EVENING_HOURS = {18, 19, 20, 21}
MIDDAY_WEEKDAY_HOURS = {11, 12, 13, 14}
WEEKDAY_DAY_HOURS = set(range(9, 18))

HOUR_COLS = [f"hour_{h:02d}" for h in range(24)]
HOUR_WD_COLS = [f"hour_wd_{h:02d}" for h in range(24)]
HOUR_WE_COLS = [f"hour_we_{h:02d}" for h in range(24)]
WEEKDAY_COLS = [f"weekday_{i}" for i in range(7)]


def _to_dt(x: str) -> dt.datetime:
    """Parse ISO string to timezone-aware UTC datetime."""
    dt_obj = dt.datetime.fromisoformat(x.replace("Z", "+00:00"))
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=dt.timezone.utc)
    return dt_obj


def _mask_for(ts: dt.datetime) -> int:
    h = ts.hour
    wd = ts.weekday()
    mask = 0
    if 4 <= h <= 6:
        mask |= 0x1
    if 20 <= h <= 23:
        mask |= 0x2
    if (9 <= h <= 17) and (wd < 5):
        mask |= 0x4
    if (h >= 22) or (h <= 5):
        mask |= 0x8
    return mask


def _empty_store(maid: str) -> Dict[str, object]:
    store: Dict[str, object] = {
        "maid": maid,
        "total_pings": 0,
        "geohash": [],
        "pings": [],
        "first_seen": [],
        "last_seen": [],
        "span_days": [],
        "unique_days": [],
        "gap_bins_0d": [],
        "gap_bins_1_3d": [],
        "gap_bins_4_7d": [],
        "gap_bins_8_30d": [],
        "gap_bins_gt_30d": [],
        "mean_lat": [],
        "mean_lon": [],
        "mean_geohash": [],
        "std_geohash_m": [],
        "mean_time_diff_seconds": [],
        "est_duration": [],
        "night_days": [],
        "monthly_stability": [],
        "weekday_work_days": [],
        "late_evening_days": [],
        "early_morning_days": [],
        "active_days_last_30d": [],
        "flux_B": [],
        "flux_C": [],
        "flux_D": [],
        "flux_E": [],
        "flux_F": [],
    }
    for col in HOUR_COLS + HOUR_WD_COLS + HOUR_WE_COLS + WEEKDAY_COLS:
        store[col] = []
    return store


def _init_acc() -> Dict[str, object]:
    return {
        "ts": [],
        "lats": [],
        "lons": [],
        "lat_sum": 0.0,
        "lon_sum": 0.0,
        "flux_counts": {"B": 0, "C": 0, "D": 0, "E": 0, "F": 0},
        "hourly": [0] * 24,
        "hour_wd": [0] * 24,
        "hour_we": [0] * 24,
        "weekday_hist": [0] * 7,
        "monthly_hist": {},
        "hourly_minutes": {},
        "day_masks": {},
    }


def build_columnar_store(rows: Iterable[Dict[str, object]], maid: Optional[str] = None) -> Dict[str, object]:
    """Build columnar stored_data from raw rows."""
    rows_list = list(rows)
    if not rows_list:
        return _empty_store(maid or "unknown")

    maid = maid or str(rows_list[0].get("maid", "unknown"))
    tmp: Dict[str, Dict[str, object]] = {}

    for row in rows_list:
        gh = str(row["geohash"])
        ts = _to_dt(str(row["timestamp"]))
        lat = float(row.get("latitude", 0.0))
        lon = float(row.get("longitude", 0.0))
        flux = row.get("flux")

        acc = tmp.setdefault(gh, _init_acc())
        acc["ts"].append(ts)
        acc["lats"].append(lat)
        acc["lons"].append(lon)
        acc["lat_sum"] += lat
        acc["lon_sum"] += lon

        h = ts.hour
        wd = ts.weekday()
        acc["hourly"][h] += 1
        acc["weekday_hist"][wd] += 1
        if wd >= 5:
            acc["hour_we"][h] += 1
        else:
            acc["hour_wd"][h] += 1

        minute = ts.minute
        hourly_minutes = acc["hourly_minutes"]
        if h not in hourly_minutes:
            hourly_minutes[h] = {"min": minute, "max": minute}
        else:
            hourly_minutes[h]["min"] = min(hourly_minutes[h]["min"], minute)
            hourly_minutes[h]["max"] = max(hourly_minutes[h]["max"], minute)

        if flux in acc["flux_counts"]:
            acc["flux_counts"][flux] += 1

        d_ord = ts.date().toordinal()
        mask = _mask_for(ts)
        acc["day_masks"][d_ord] = acc["day_masks"].get(d_ord, 0) | mask

        mkey = f"{ts.year:04d}-{ts.month:02d}"
        acc["monthly_hist"][mkey] = acc["monthly_hist"].get(mkey, 0) + 1

    store = _empty_store(maid)
    total_pings = 0

    for gh in sorted(tmp.keys()):
        acc = tmp[gh]
        ts_list: List[dt.datetime] = sorted(acc["ts"])
        pings = len(ts_list)
        first_ts = ts_list[0]
        last_ts = ts_list[-1]
        total_pings += pings

        # Time diffs
        total_diff = 0.0
        diff_count = 0
        for i in range(1, len(ts_list)):
            delta = (ts_list[i] - ts_list[i - 1]).total_seconds()
            if delta > 0:
                total_diff += delta
                diff_count += 1
        mean_time_diff = total_diff / diff_count if diff_count else 0.0

        # Gap bins from unique days
        day_ordinals = sorted(acc["day_masks"].keys())
        gap_bins = {"0d": 0, "1-3d": 0, "4-7d": 0, "8-30d": 0, ">30d": 0}
        for i in range(1, len(day_ordinals)):
            delta = day_ordinals[i] - day_ordinals[i - 1]
            if delta == 0:
                gap_bins["0d"] += 1
            elif 1 <= delta <= 3:
                gap_bins["1-3d"] += 1
            elif 4 <= delta <= 7:
                gap_bins["4-7d"] += 1
            elif 8 <= delta <= 30:
                gap_bins["8-30d"] += 1
            else:
                gap_bins[">30d"] += 1

        # Day-level flags
        night_days = sum(1 for m in acc["day_masks"].values() if m & 0x8)
        weekday_work_days = sum(1 for m in acc["day_masks"].values() if m & 0x4)
        late_evening_days = sum(1 for m in acc["day_masks"].values() if m & 0x2)
        early_morning_days = sum(1 for m in acc["day_masks"].values() if m & 0x1)

        last_day = last_ts.date().toordinal()
        active_days_last_30d = sum(1 for d in day_ordinals if d >= (last_day - 30))

        # Estimated duration from per-hour minute ranges
        est_duration = 0
        for minmax in acc["hourly_minutes"].values():
            est_duration += max(0, minmax["max"] - minmax["min"])

        span_days = (last_ts.date() - first_ts.date()).days + 1
        unique_days = len(day_ordinals)
        mean_lat = acc["lat_sum"] / pings if pings else 0.0
        mean_lon = acc["lon_sum"] / pings if pings else 0.0

        # Calculate mean_geohash and std_geohash_m
        mean_gh = None
        std_geohash_m = 0.0
        if pings > 0:
            mean_gh = pgh.encode(mean_lat, mean_lon, precision=12)
            if pings > 1:
                M2 = 0.0
                # iterate through stored lats/lons
                for lat_i, lon_i in zip(acc["lats"], acc["lons"]):
                     # We encode each point to gh12 then measure dist to mean_gh for consistency with old pipeline
                     # Old pipeline: pgh.geohash_haversine_distance(pgh.encode(lat, lon), mean_gh)
                     gh_i = pgh.encode(lat_i, lon_i, precision=12)
                     d = pgh.geohash_haversine_distance(gh_i, mean_gh)
                     M2 += d * d
                std_geohash_m = math.sqrt(M2 / pings)

        # Calculate monthly_stability
        months = list(acc["monthly_hist"].values())
        if len(months) >= 2:
            mean_m = sum(months) / len(months)
            var_m = sum((x - mean_m) ** 2 for x in months) / len(months)
            std_m = math.sqrt(var_m)
            cv = (std_m / mean_m) if mean_m > 0 else 0.0
        else:
            cv = 0.0
        monthly_stability = 1.0 / (1.0 + cv)

        store["geohash"].append(gh)
        store["pings"].append(pings)
        store["first_seen"].append(int(first_ts.timestamp()))
        store["last_seen"].append(int(last_ts.timestamp()))
        store["span_days"].append(span_days)
        store["unique_days"].append(unique_days)
        store["gap_bins_0d"].append(gap_bins["0d"])
        store["gap_bins_1_3d"].append(gap_bins["1-3d"])
        store["gap_bins_4_7d"].append(gap_bins["4-7d"])
        store["gap_bins_8_30d"].append(gap_bins["8-30d"])
        store["gap_bins_gt_30d"].append(gap_bins[">30d"])
        store["mean_lat"].append(mean_lat)
        store["mean_lon"].append(mean_lon)
        store["mean_geohash"].append(mean_gh)
        store["std_geohash_m"].append(std_geohash_m)
        store["monthly_stability"].append(monthly_stability)
        store["mean_time_diff_seconds"].append(mean_time_diff)
        store["est_duration"].append(est_duration)
        store["night_days"].append(night_days)
        store["weekday_work_days"].append(weekday_work_days)
        store["late_evening_days"].append(late_evening_days)
        store["early_morning_days"].append(early_morning_days)
        store["active_days_last_30d"].append(active_days_last_30d)

        store["flux_B"].append(acc["flux_counts"]["B"])
        store["flux_C"].append(acc["flux_counts"]["C"])
        store["flux_D"].append(acc["flux_counts"]["D"])
        store["flux_E"].append(acc["flux_counts"]["E"])
        store["flux_F"].append(acc["flux_counts"]["F"])

        for idx, col in enumerate(HOUR_COLS):
            store[col].append(acc["hourly"][idx])
        for idx, col in enumerate(HOUR_WD_COLS):
            store[col].append(acc["hour_wd"][idx])
        for idx, col in enumerate(HOUR_WE_COLS):
            store[col].append(acc["hour_we"][idx])
        for idx, col in enumerate(WEEKDAY_COLS):
            store[col].append(acc["weekday_hist"][idx])

    store["total_pings"] = total_pings
    return store


def _entropy_from_hist(hist: Sequence[int]) -> float:
    total = sum(hist)
    if total == 0:
        return 0.0
    H = 0.0
    for cnt in hist:
        if cnt > 0:
            p = cnt / total
            H -= p * math.log(p + 1e-12)
    return H / math.log(len(hist))


def _shrink_ratio(ratio: float, n: int, p0: float, a: float = 2.0) -> float:
    return (ratio * n + a * p0) / (n + a) if (n + a) > 0 else p0


def _score_home(l1: Dict[str, float], l2: Dict[str, float], visits: int, days: int) -> float:
    night_ratio_shrunk = _shrink_ratio(l2["night_ratio"], visits, 8.0 / 24.0, a=2.0)
    w_visits = 1.0 - math.exp(-visits / 5.0)
    w_days = 1.0 - math.exp(-days / 3.0)
    base = (
        0.375 * l2["night_days_ratio"]
        + 0.10 * night_ratio_shrunk
        + 0.15 * l2["late_evening_days_ratio"]
        + 0.10 * l2["early_morning_days_ratio"]
        + 0.075 * (1.0 - l2["entropy_hour_norm"])
        + 0.25 * l1["active_day_ratio"]
        + 0.05 * l2.get("monthly_stability", 1.0)
    )
    s = base * w_visits * w_days
    s *= min(1.0, 0.5 + 0.5 * (l2["active_days_last_30d"] / 10.0))
    return max(0.0, min(1.0, s))


def _score_work(l1: Dict[str, float], l2: Dict[str, float], visits: int, days: int) -> float:
    weekday_day_ratio_shrunk = _shrink_ratio(l2["weekday_day_ratio"], visits, 45.0 / 168.0, a=2.0)
    w_visits = 1.0 - math.exp(-visits / 5.0)
    w_days = 1.0 - math.exp(-days / 3.0)
    base = (
        0.425 * l2["weekday_work_days_ratio"]
        + 0.15 * weekday_day_ratio_shrunk
        + 0.10 * l2["midday_weekday_ratio"]
        + 0.075 * (1.0 - l2["entropy_hour_norm"])
        + 0.20 * l1["active_day_ratio"]
        + 0.05 * l2.get("monthly_stability", 1.0)
    )
    s = base * w_visits * w_days
    s *= min(1.0, 0.5 + 0.5 * (l2["active_days_last_30d"] / 10.0))
    return max(0.0, min(1.0, s))


def _score_leisure(l1: Dict[str, float], l2: Dict[str, float], visits: int, days: int, home_score: float, work_score: float) -> float:
    weekend_ratio_shrunk = _shrink_ratio(l2["weekend_ratio"], visits, 2.0 / 7.0, a=2.0)
    evening_ratio_shrunk = _shrink_ratio(l2["evening_ratio"], visits, 4.0 / 24.0, a=2.0)
    w_visits = 1.0 - math.exp(-visits / 5.0)
    w_days = 1.0 - math.exp(-days / 3.0)
    inverse_pattern = 1.0 - ((home_score + work_score) / 2.0)
    base = (
        0.25 * weekend_ratio_shrunk
        + 0.20 * evening_ratio_shrunk
        + 0.15 * (1.0 - l2["entropy_hour_norm"])
        + 0.10 * (1.0 - l2.get("monthly_stability", 1.0))
        + 0.30 * inverse_pattern
    )
    s = base * w_visits * w_days
    s *= min(1.0, 0.5 + 0.5 * (l2["active_days_last_30d"] / 15.0))
    return max(0.0, min(1.0, s))


def _score_pingsink(meta: Dict[str, float], l1: Dict[str, float], total_pings: int) -> float:
    pings = l1["pings"]
    std_m = meta.get("std_geohash_m")
    
    if pings <= 5:
        return 0.0
        
    # Zero spread means all pings collapse to a point; treat as a perfect sink regardless of volume.
    if std_m == 0:
        return 1.0
    if std_m is not None:
        geo_stability = 0.7 * math.exp(-std_m / 20.0)
    else:
        geo_stability = 0.0
    mean_time_diff = meta.get("mean_time_diff_seconds")
    if mean_time_diff is not None:
        time_diff_minutes = mean_time_diff / 60.0
        temporal_density = 0.1 * math.exp(-time_diff_minutes / 60.0)
    else:
        temporal_density = 0.0
    ping_factor = 1.0 - math.exp(-pings / 50.0)
    base_score = geo_stability + temporal_density + 0.2 * ping_factor
    if total_pings > 0:
        relative_importance = pings / total_pings
        importance_multiplier = 0.8 + 0.2 * math.sqrt(relative_importance)
    else:
        importance_multiplier = 1.0
    final = max(0.0, min(1.0, base_score * importance_multiplier))
    
    # DEBUG
    if pings > 30 and final < 0.1:
        # print(f"DEBUG Pingsink: n={n} std={std_m} diff={mean_diff} -> {final}")
        pass
        
    return final


def _score_pingsink_debug(meta, l1, total_pings, maid, gh):
    val = _score_pingsink(meta, l1, total_pings)
    if maid.startswith('yHJTx929oCfoBhf') and gh == 'ey7gkn7':
        pings = l1["pings"]
        std_m = meta.get("std_geohash_m")
        mean_diff = meta.get("mean_time_diff_seconds")
        print(f"DEBUG SPECIFIC: MAID={maid} GH={gh} pings={pings} std={std_m} diff={mean_diff} -> val={val}")
    return val


def derive_columnar(store: Dict[str, object]) -> Dict[str, object]:
    derived: Dict[str, object] = {
        "maid": store["maid"],
        "total_pings": store["total_pings"],
        "geohash": [],
        "pings": [],
        "unique_days": [],
        "span_days": [],
        "active_day_ratio": [],
        "night_ratio": [],
        "weekday_day_ratio": [],
        "weekend_ratio": [],
        "midday_weekday_ratio": [],
        "evening_ratio": [],
        "night_days_ratio": [],
        "weekday_work_days_ratio": [],
        "late_evening_days_ratio": [],
        "early_morning_days_ratio": [],
        "entropy_hour_norm": [],
        "monthly_stability": [],
        "active_days_last_30d": [],
        "mean_time_diff_seconds": [],
        "mean_geohash": [],
        "std_geohash_m": [],
        "est_duration": [],
        "flux_B": [],
        "flux_C": [],
        "flux_D": [],
        "flux_E": [],
        "flux_F": [],
        "home_score": [],
        "work_score": [],
        "leisure_score": [],
        "pingsink_score": [],
    }

    for gh_idx, gh in enumerate(store["geohash"]):
        visits = int(store["pings"][gh_idx])
        unique_days = int(store["unique_days"][gh_idx])
        span_days = int(store["span_days"][gh_idx])
        # Improved active_day_ratio with span capping and continuity boost
        capped_span = max(30, span_days)
        base_active_ratio = min(1.0, unique_days / capped_span) if capped_span > 0 else 0.0

        gap_0d = int(store["gap_bins_0d"][gh_idx])
        gap_1_3d = int(store["gap_bins_1_3d"][gh_idx])
        gap_4_7d = int(store["gap_bins_4_7d"][gh_idx])
        gap_8_30d = int(store["gap_bins_8_30d"][gh_idx])
        gap_gt_30d = int(store["gap_bins_gt_30d"][gh_idx])
        
        total_gaps = max(1, gap_0d + gap_1_3d + gap_4_7d + gap_8_30d + gap_gt_30d)
        continuity = (gap_0d + gap_1_3d) / total_gaps
        active_day_ratio = base_active_ratio * (0.5 + 0.5 * continuity)

        hourly_hist = [int(store[col][gh_idx]) for col in HOUR_COLS]
        hour_wd = [int(store[col][gh_idx]) for col in HOUR_WD_COLS]
        hour_we = [int(store[col][gh_idx]) for col in HOUR_WE_COLS]
        weekday_hist = [int(store[col][gh_idx]) for col in WEEKDAY_COLS]

        night_ratio = sum(hourly_hist[h] for h in NIGHT_HOURS) / visits if visits else 0.0
        weekday_day_ratio = sum(hour_wd[h] for h in WEEKDAY_DAY_HOURS) / visits if visits else 0.0
        weekend_ratio = (weekday_hist[5] + weekday_hist[6]) / visits if visits else 0.0
        midday_weekday_ratio = sum(hour_wd[h] for h in MIDDAY_WEEKDAY_HOURS) / visits if visits else 0.0
        evening_ratio = sum(hourly_hist[h] for h in EVENING_HOURS) / visits if visits else 0.0

        night_days = int(store["night_days"][gh_idx])
        weekday_work_days = int(store["weekday_work_days"][gh_idx])
        late_evening_days = int(store["late_evening_days"][gh_idx])
        early_morning_days = int(store["early_morning_days"][gh_idx])
        active_days_last_30d = int(store["active_days_last_30d"][gh_idx])

        night_days_ratio = (night_days / unique_days) if unique_days else 0.0
        weekday_work_days_ratio = (weekday_work_days / unique_days) if unique_days else 0.0
        late_evening_days_ratio = (late_evening_days / unique_days) if unique_days else 0.0
        early_morning_days_ratio = (early_morning_days / unique_days) if unique_days else 0.0

        entropy_hour_norm = _entropy_from_hist(hourly_hist)

        l1 = {
            "pings": visits,
            "unique_days": unique_days,
            "active_day_ratio": active_day_ratio,
        }
        l2 = {
            "night_ratio": night_ratio,
            "weekday_day_ratio": weekday_day_ratio,
            "weekend_ratio": weekend_ratio,
            "midday_weekday_ratio": midday_weekday_ratio,
            "evening_ratio": evening_ratio,
            "night_days_ratio": night_days_ratio,
            "weekday_work_days_ratio": weekday_work_days_ratio,
            "late_evening_days_ratio": late_evening_days_ratio,
            "early_morning_days_ratio": early_morning_days_ratio,
            "entropy_hour_norm": entropy_hour_norm,
            "monthly_stability": store["monthly_stability"][gh_idx],
            "active_days_last_30d": active_days_last_30d,
        }
        meta = {
            "std_geohash_m": store["std_geohash_m"][gh_idx],
            "mean_time_diff_seconds": store["mean_time_diff_seconds"][gh_idx],
        }

        home_score = _score_home(l1, l2, visits, unique_days)
        work_score = _score_work(l1, l2, visits, unique_days)
        leisure_score = _score_leisure(l1, l2, visits, unique_days, home_score, work_score)
        pingsink_score = _score_pingsink_debug(meta, l1, store["total_pings"], store["maid"], gh)

        derived["geohash"].append(gh)
        derived["pings"].append(visits)
        derived["unique_days"].append(unique_days)
        derived["span_days"].append(span_days)
        derived["active_day_ratio"].append(active_day_ratio)
        derived["night_ratio"].append(night_ratio)
        derived["weekday_day_ratio"].append(weekday_day_ratio)
        derived["weekend_ratio"].append(weekend_ratio)
        derived["midday_weekday_ratio"].append(midday_weekday_ratio)
        derived["evening_ratio"].append(evening_ratio)
        derived["night_days_ratio"].append(night_days_ratio)
        derived["weekday_work_days_ratio"].append(weekday_work_days_ratio)
        derived["late_evening_days_ratio"].append(late_evening_days_ratio)
        derived["early_morning_days_ratio"].append(early_morning_days_ratio)
        derived["entropy_hour_norm"].append(entropy_hour_norm)
        derived["monthly_stability"].append(store["monthly_stability"][gh_idx])
        derived["active_days_last_30d"].append(active_days_last_30d)
        derived["mean_time_diff_seconds"].append(meta["mean_time_diff_seconds"])
        derived["mean_geohash"].append(store["mean_geohash"][gh_idx])
        derived["std_geohash_m"].append(meta["std_geohash_m"])
        derived["est_duration"].append(store["est_duration"][gh_idx])
        derived["flux_B"].append(store["flux_B"][gh_idx])
        derived["flux_C"].append(store["flux_C"][gh_idx])
        derived["flux_D"].append(store["flux_D"][gh_idx])
        derived["flux_E"].append(store["flux_E"][gh_idx])
        derived["flux_F"].append(store["flux_F"][gh_idx])
        derived["home_score"].append(home_score)
        derived["work_score"].append(work_score)
        derived["leisure_score"].append(leisure_score)
        derived["pingsink_score"].append(pingsink_score)

    return derived


def save_store(path: Path, store: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(store, indent=2))


def load_store(path: Path) -> Dict[str, object]:
    return json.loads(Path(path).read_text())


__all__ = [
    "build_columnar_store",
    "derive_columnar",
    "save_store",
    "load_store",
    "HOUR_COLS",
    "HOUR_WD_COLS",
    "HOUR_WE_COLS",
    "WEEKDAY_COLS",
]

