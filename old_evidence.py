import datetime as dt
import math
import pickle
import gzip
import os
import time
import tempfile
from pathlib import Path
from typing import Dict, List, Union, Optional
import pygeohash as pgh  # type: ignore

def _to_dt(x: Union[str, dt.datetime]) -> dt.datetime:
    """Convert input to datetime object, ensuring UTC timezone consistency"""
    if isinstance(x, str):
        # Parse string and assume it's in UTC if no timezone info
        dt_obj = dt.datetime.fromisoformat(x.replace('Z', '+00:00'))
        if dt_obj.tzinfo is None:
            dt_obj = dt_obj.replace(tzinfo=dt.timezone.utc)
        return dt_obj
    elif isinstance(x, dt.datetime):
        # Ensure datetime has UTC timezone
        if x.tzinfo is None:
            return x.replace(tzinfo=dt.timezone.utc)
        return x
    else:
        raise ValueError(f"Unsupported type for datetime conversion: {type(x)}")

class EvidenceStore:
    def __init__(self, maid: str = None):
        self.store: Dict[str, dict] = {}
        self.maid: str = maid  # Add MAID attribute
        self.total_pings: int = 0  # Track total pings across all geohashes for this MAID

    def _init(self):
        return {
            'pings': 0,
            'first_seen_ts': None,
            'last_seen_ts': None,
            'unique_days': set(),
            'hourly_hist': [0]*24,
            'weekday_hist': [0]*7,
            'hourly_hist_weekday': [0]*24,
            'hourly_hist_weekend': [0]*24,
            'monthly_hist': {},
            'daily_flags': {},
            'max_seen_date': None,
            'gap_bins': {'0d':0, '1-3d':0, '4-7d':0, '8-30d':0, '>30d':0},
            'poi_info': None,
            'poi_calculated': False,
            'mean_geohash': None,
            'std_geohash_m': None,
            'M2_geohash': 0.0,
            'mean_lat': None,
            'mean_lon': None,
            'hourly_minutes': {},
            'est_duration': 0,
            'flux_counts': {'B': 0, 'C': 0, 'D': 0, 'E': 0, 'F': 0},
            'mean_time_diff_seconds': None,
            'total_time_diff_seconds': 0.0,
            'time_diff_count': 0,
        }
    
    def _update_geohash_stats(self, c: dict, geohashes: List[str]):
        """Update geohash mean and std incrementally using pygeohash
        
        Args:
            c: storage dict for this geohash
            geohashes: list of geohashes (precision 12) to add
        """
        if not geohashes:
            return
            
        n = c['pings']
        
        if n == 0 or c['mean_geohash'] is None:
            # First update - calculate directly
            c['mean_geohash'] = pgh.mean(geohashes)
            c['M2_geohash'] = 0.0
            
            # Calculate initial std if we have multiple geohashes
            if len(geohashes) > 1:
                distances = [pgh.geohash_haversine_distance(gh, c['mean_geohash']) for gh in geohashes]
                c['M2_geohash'] = sum(d**2 for d in distances)
                c['std_geohash_m'] = math.sqrt(c['M2_geohash'] / len(geohashes))
            else:
                c['std_geohash_m'] = 0.0
            
            # Also store mean_lat and mean_lon for backward compatibility
            c['mean_lat'], c['mean_lon'] = pgh.decode(c['mean_geohash'])
        else:
            # Incremental update using Welford's algorithm for distances
            old_mean = c['mean_geohash']
            
            # Combine old and new geohashes to get new mean
            # We need to approximate this since we don't have old geohashes
            # Use weighted average of lat/lon instead
            old_lat, old_lon = pgh.decode(old_mean)
            
            # Calculate mean of new geohashes
            new_mean_gh = pgh.mean(geohashes) if len(geohashes) > 1 else geohashes[0]
            new_lat, new_lon = pgh.decode(new_mean_gh)
            
            # Weighted average for combined mean
            total_count = n + len(geohashes)
            combined_lat = (old_lat * n + new_lat * len(geohashes)) / total_count
            combined_lon = (old_lon * n + new_lon * len(geohashes)) / total_count
            c['mean_geohash'] = pgh.encode(combined_lat, combined_lon, precision=12)
            c['mean_lat'] = combined_lat
            c['mean_lon'] = combined_lon
            
            # Update M2 using distances from combined mean
            for gh in geohashes:
                dist = pgh.geohash_haversine_distance(gh, c['mean_geohash'])
                delta = dist
                c['M2_geohash'] += delta * delta
            
            # Calculate std using total ping count
            if total_count > 1:
                # Recalculate M2 for accuracy (since mean changed)
                # This is approximate but maintains reasonable accuracy
                c['std_geohash_m'] = math.sqrt(c['M2_geohash'] / total_count)
            else:
                c['std_geohash_m'] = 0.0

    @staticmethod
    def _shrink_ratio(ratio: float, n: int, p0: float, a: float = 8.0) -> float:
        return (ratio * n + a * p0) / (n + a) if (n + a) > 0 else p0

    @staticmethod
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

    def update(self, new_data: Dict[str, List[Union[str, dt.datetime]]], 
               geohashes: Optional[Dict[str, List[str]]] = None,
               flux_data: Optional[Dict[str, List[str]]] = None):
        """Update evidence store with timestamp data and simplified duration calculation
        
        Args:
            new_data: Dict mapping geohash (precision 7) to list of timestamps
            geohashes: Dict mapping geohash (precision 7) to list of geohashes (precision 12), parallel to timestamps
                       Uses pygeohash.mean() and pygeohash.geohash_haversine_distance() for accurate geographic std
            flux_data: Dict mapping geohash to list of flux types
            
        Example:
            # Prepare data from DataFrame
            rows = df[['geohash', 'timestamp', 'latitude', 'longitude', 'flux']].values.tolist()
            store = EvidenceStore(maid='user_123')
            
            # Group by geohash (precision 7)
            setin = {}
            geohashes_p12 = {}
            flux_values = {}
            
            for gh7, ts, lat, lon, flux in rows:
                # Timestamps
                setin.setdefault(gh7, []).append(ts)
                
                # Geohashes precision 12 for std calculation
                gh12 = pgh.encode(lat, lon, precision=12)
                geohashes_p12.setdefault(gh7, []).append(gh12)
                
                # Flux data
                if flux is not None:
                    flux_values.setdefault(gh7, []).append(flux)
            
            # Update store
            store.update(setin, geohashes_p12, flux_values)
        """
        for gh, ts_list in (new_data or {}).items():
            if not ts_list: 
                continue
            ts_objs = [_to_dt(x) for x in ts_list]
            ts_objs.sort()
            c = self.store.get(gh)
            if c is None:
                c = self._init()
                self.store[gh] = c

            # Update geohash statistics if provided
            if geohashes and gh in geohashes:
                gh_list = geohashes[gh]
                if isinstance(gh_list, list) and len(gh_list) > 0:
                    self._update_geohash_stats(c, gh_list)

            # Update flux counts if provided
            if flux_data and gh in flux_data:
                for flux_type in flux_data[gh]:
                    if flux_type in c['flux_counts']:
                        c['flux_counts'][flux_type] += 1

            # Initialize or reset hourly_minutes for this update
            hourly_minutes = {}
            
            # Track previous timestamp for time diff calculation
            prev_ts = c['last_seen_ts'] if c['last_seen_ts'] is not None else None
            
            for ts in ts_objs:
                d = ts.date()
                h = ts.hour
                minute = ts.minute
                wd = ts.weekday()
                
                # Calculate time diff from previous ping (incremental)
                if prev_ts is not None:
                    time_diff = (ts - prev_ts).total_seconds()
                    if time_diff > 0:  # Only count positive time differences
                        c['total_time_diff_seconds'] += time_diff
                        c['time_diff_count'] += 1
                
                # Update prev_ts for next iteration
                prev_ts = ts
                
                # Track min/max minute for each hour
                if h not in hourly_minutes:
                    hourly_minutes[h] = {'min': minute, 'max': minute}
                else:
                    hourly_minutes[h]['min'] = min(hourly_minutes[h]['min'], minute)
                    hourly_minutes[h]['max'] = max(hourly_minutes[h]['max'], minute)
                
                # Update other tracking fields
                c['first_seen_ts'] = ts if c['first_seen_ts'] is None or ts < c['first_seen_ts'] else c['first_seen_ts']
                c['last_seen_ts']  = ts if c['last_seen_ts']  is None or ts > c['last_seen_ts']  else c['last_seen_ts']
                c['pings'] += 1
                c['unique_days'].add(d)
                c['hourly_hist'][h] += 1
                c['weekday_hist'][wd] += 1
                if wd >= 5:
                    c['hourly_hist_weekend'][h] += 1
                else:
                    c['hourly_hist_weekday'][h] += 1
                mkey = f"{ts.year:04d}-{ts.month:02d}"
                c['monthly_hist'][mkey] = c['monthly_hist'].get(mkey, 0) + 1
                ord_key = d.toordinal()
                c['daily_flags'][ord_key] = c['daily_flags'].get(ord_key, 0) | self._mask_for(ts)
                if c['max_seen_date'] is None or d > c['max_seen_date']:
                    if c['max_seen_date'] is not None:
                        delta = (d - c['max_seen_date']).days
                        if delta == 0:
                            c['gap_bins']['0d'] += 1
                        elif 1 <= delta <= 3:
                            c['gap_bins']['1-3d'] += 1
                        elif 4 <= delta <= 7:
                            c['gap_bins']['4-7d'] += 1
                        elif 8 <= delta <= 30:
                            c['gap_bins']['8-30d'] += 1
                        else:
                            c['gap_bins']['>30d'] += 1
                    c['max_seen_date'] = d
            
            # Merge hourly_minutes from this update into stored data
            if not c.get('hourly_minutes'):
                c['hourly_minutes'] = {}
                
            for hour, minmax in hourly_minutes.items():
                if hour not in c['hourly_minutes']:
                    c['hourly_minutes'][hour] = minmax
                else:
                    c['hourly_minutes'][hour]['min'] = min(c['hourly_minutes'][hour]['min'], minmax['min'])
                    c['hourly_minutes'][hour]['max'] = max(c['hourly_minutes'][hour]['max'], minmax['max'])
            
            # Always recalculate estimated duration from the complete hourly_minutes
            # This ensures consistency regardless of batch size
            total_duration = 0
            for hour, minmax in c['hourly_minutes'].items():
                duration = minmax['max'] - minmax['min']
                total_duration += duration
            
            c['est_duration'] = total_duration
            
            # Update mean time diff
            if c['time_diff_count'] > 0:
                c['mean_time_diff_seconds'] = c['total_time_diff_seconds'] / c['time_diff_count']
        
        # Update total_pings for MAID after processing all geohashes
        self.total_pings = sum(c['pings'] for c in self.store.values())

    def save_to_pickle(self, filepath: str, compress: bool = False) -> None:
        """Save EvidenceStore to pickle format with atomic write
        
        Args:
            filepath: Path to save the pickle file
            compress: Whether to use gzip compression (default: False for <1MB files)
        """
        try:
            # Prepare data for serialization
            data = {
                'store': self.store,
                'version': '1.0',  # For future compatibility
                'created_at': dt.datetime.now().isoformat(),
                'maid': self.maid,  # Add MAID to saved data
                'total_pings': self.total_pings  # Add total_pings to saved data
            }
            
            # Ensure directory exists
            Path(filepath).parent.mkdir(parents=True, exist_ok=True)
            
            # Determine final filepath with proper extension
            if compress:
                if not filepath.endswith('.gz'):
                    filepath += '.gz'
            else:
                if not any(filepath.endswith(ext) for ext in ['.pkl', '.pickle']):
                    filepath += '.pkl'
            
            # Atomic write: write to temp file first, then atomically replace
            temp_dir = os.path.dirname(filepath)
            with tempfile.NamedTemporaryFile(mode='wb', dir=temp_dir, delete=False, suffix='.tmp') as temp_file:
                temp_path = temp_file.name
                
                try:
                    if compress:
                        with gzip.open(temp_file, 'wb', compresslevel=9) as f:
                            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
                    else:
                        pickle.dump(data, temp_file, protocol=pickle.HIGHEST_PROTOCOL)
                    
                    # Ensure data is written to disk
                    temp_file.flush()
                    os.fsync(temp_file.fileno())
                    
                except Exception:
                    # Clean up temp file on error
                    try:
                        os.unlink(temp_path)
                    except OSError:
                        pass
                    raise
                
                # Atomically replace target file
                os.replace(temp_path, filepath)
                    
        except Exception:
            raise
    
    def load_from_pickle(self, filepath: str, max_retries: int = 3) -> None:
        """Load EvidenceStore from pickle format with retry on transient errors
        
        Args:
            filepath: Path to the pickle file
            max_retries: Maximum number of retry attempts for transient errors
        """
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                # Auto-detect compression based on file extension
                if filepath.endswith('.gz'):
                    with gzip.open(filepath, 'rb') as f:
                        data = pickle.load(f)
                elif any(filepath.endswith(ext) for ext in ['.pkl', '.pickle']):
                    with open(filepath, 'rb') as f:
                        data = pickle.load(f)
                else:
                    # Try uncompressed first, fallback to compressed
                    try:
                        with open(filepath, 'rb') as f:
                            data = pickle.load(f)
                    except (pickle.UnpicklingError, UnicodeDecodeError):
                        with gzip.open(filepath, 'rb') as f:
                            data = pickle.load(f)
                
                # Load store data
                if 'store' in data:
                    self.store = data['store']
                else:
                    # Legacy format support (direct store)
                    self.store = data

                # Load MAID if available
                self.maid = data.get('maid', None)
                
                # Load total_pings if available, otherwise recalculate
                self.total_pings = data.get('total_pings', 0)

                # Initialize flux_counts and geohash stats for older data formats
                for gh, c in self.store.items():
                    if 'flux_counts' not in c:
                        c['flux_counts'] = {'B': 0, 'C': 0, 'D': 0, 'E': 0, 'F': 0}
                    
                    # Initialize geohash stats fields for legacy data
                    if 'mean_geohash' not in c:
                        c['mean_geohash'] = None
                    if 'std_geohash_m' not in c:
                        c['std_geohash_m'] = None
                    if 'M2_geohash' not in c:
                        c['M2_geohash'] = 0.0
                    
                    # Initialize time diff fields for legacy data
                    if 'mean_time_diff_seconds' not in c:
                        c['mean_time_diff_seconds'] = None
                    if 'total_time_diff_seconds' not in c:
                        c['total_time_diff_seconds'] = 0.0
                    if 'time_diff_count' not in c:
                        c['time_diff_count'] = 0
                    
                    # Remove old geohash_count if it exists (replaced by pings)
                    if 'geohash_count' in c:
                        del c['geohash_count']
                    
                    # If we have mean_lat/mean_lon but no mean_geohash, encode it
                    if c.get('mean_lat') is not None and c.get('mean_lon') is not None and c['mean_geohash'] is None:
                        c['mean_geohash'] = pgh.encode(c['mean_lat'], c['mean_lon'], precision=12)
                
                # Recalculate total_pings if not available
                if self.total_pings == 0:
                    self.total_pings = sum(c['pings'] for c in self.store.values())
                
                # Success - return
                return
                
            except FileNotFoundError:
                print(f"File not found: {filepath}")
                raise
            except (EOFError, pickle.UnpicklingError, OSError) as e:
                last_exception = e
                if attempt < max_retries:
                    # Transient error - retry with exponential backoff
                    sleep_time = 0.1 * (2 ** attempt)  # 0.1s, 0.2s, 0.4s
                    print(f"Transient error loading {filepath} (attempt {attempt + 1}/{max_retries + 1}): {e}. Retrying in {sleep_time}s...")
                    time.sleep(sleep_time)
                    continue
                else:
                    # Max retries exceeded
                    print(f"Error loading EvidenceStore from {filepath} after {max_retries + 1} attempts: {e}")
                    raise
            except Exception as e:
                # Non-transient error - don't retry
                print(f"Error loading EvidenceStore from {filepath}: {e}")
                raise
        
        # Should not reach here, but just in case
        if last_exception:
            raise last_exception
    
    def save(self, filepath: str, compress: bool = False) -> None:
        """Convenience method to save the store
        
        Args:
            filepath: Path to save the file
            compress: Whether to use compression (default: False for fast I/O)
        """
        self.save_to_pickle(filepath, compress=compress)
    
    def load(self, filepath: str) -> None:
        """Convenience method to load the store
        
        Args:
            filepath: Path to load from
        """
        self.load_from_pickle(filepath)
    
    def clear_store(self) -> None:
        """Clear all evidence data"""
        self.store.clear()
        
    def recalculate_durations(self) -> None:
        """Recalculate all duration estimates in the store
        
        This can be useful when loading older data formats or ensuring
        consistency across different processing batches.
        """
        for gh, data in self.store.items():
            if 'hourly_minutes' in data:
                total_duration = 0
                for hour, minmax in data['hourly_minutes'].items():
                    duration = minmax['max'] - minmax['min']
                    total_duration += duration
                data['est_duration'] = total_duration

    def derive(self, gh: str) -> dict:
        if gh not in self.store: 
            return None
        c = self.store[gh]
        visits = c['pings']
        if visits == 0:
            return None

        first, last = c['first_seen_ts'], c['last_seen_ts']
        # Convert string timestamps to datetime objects if needed
        if isinstance(first, str):
            first = _to_dt(first)
        if isinstance(last, str):
            last = _to_dt(last)
        
        span_days = (last.date() - first.date()).days + 1 if first and last else 0
        unique_days = len(c['unique_days'])
        
        # Improved active_day_ratio with span capping and continuity boost
        capped_span = max(30, span_days)
        base_active_ratio = min(1.0, unique_days / capped_span) if capped_span > 0 else 0.0
        
        # Calculate continuity boost from gap patterns
        total_gaps = max(1, sum(c['gap_bins'].values()))
        continuity = (c['gap_bins'].get('0d', 0) + c['gap_bins'].get('1-3d', 0)) / total_gaps
        active_day_ratio = base_active_ratio * (0.5 + 0.5 * continuity)

        # visit-based ratios
        night_idxs = [22, 23, 0, 1, 2, 3, 4, 5]
        night_ratio = sum(c['hourly_hist'][i] for i in night_idxs) / visits
        weekday_day_ratio = sum(c['hourly_hist_weekday'][i] for i in range(9, 18)) / visits
        weekend_ratio = (c['weekday_hist'][5] + c['weekday_hist'][6]) / visits
        midday_weekday_ratio = sum(c['hourly_hist_weekday'][i] for i in range(11, 15)) / visits
        
        # Add evening_ratio for leisure scoring
        evening_ratio = sum(c['hourly_hist'][i] for i in range(18, 22)) / visits

        # day-level ratios from flags (presence-only)
        flags = list(c['daily_flags'].values())
        days_night = sum(1 for m in flags if (m & 0x8))
        days_weekday_work = sum(1 for m in flags if (m & 0x4))
        days_late_evening = sum(1 for m in flags if (m & 0x2))
        days_early_morning = sum(1 for m in flags if (m & 0x1))
        if unique_days > 0:
            night_days_ratio = days_night / unique_days
            weekday_work_days_ratio = days_weekday_work / unique_days
            late_evening_days_ratio = days_late_evening / unique_days
            early_morning_days_ratio = days_early_morning / unique_days
        else:
            night_days_ratio = weekday_work_days_ratio = late_evening_days_ratio = early_morning_days_ratio = 0.0

        # entropy of hourly distribution (normalized)
        H = 0.0
        for cnt in c['hourly_hist']:
            if cnt > 0:
                p = cnt / visits
                H -= p * math.log(p + 1e-12)
        entropy_hour_norm = H / math.log(24)

        # monthly stability via CV -> 1/(1+CV)
        months = list(c['monthly_hist'].values())
        if len(months) >= 2:
            mean_m = sum(months) / len(months)
            var_m = sum((x - mean_m) ** 2 for x in months) / len(months)
            std_m = math.sqrt(var_m)
            cv = (std_m / mean_m) if mean_m > 0 else 0.0
        else:
            cv = 0.0
        monthly_stability = 1.0 / (1.0 + cv)

        # recency: active days in last 30 days
        last_date = last.date()
        last30_cut = last_date.toordinal() - 30
        # Convert string keys to integers for comparison
        active_days_last_30d = sum(1 for d_ord_str in c['daily_flags'].keys() if int(d_ord_str) >= last30_cut)

        # Get POI info from cached data
        poi_info = c.get('poi_info', None)
        poi_available = c.get('poi_calculated', False) and poi_info is not None

        # Simple duration info from the simplified calculation
        duration_info = {
            'est_duration': c.get('est_duration', 0),
            'hourly_minutes': c.get('hourly_minutes', {})
        }
            
        return {
            'meta': {
                'first_seen': first.isoformat() if first else None,
                'last_seen': last.isoformat() if last else None,
                'span_days': span_days,
                'mean_coordinate': [c.get('mean_lat', None), c.get('mean_lon', None)] if c.get('mean_lat') is not None else None,
                'mean_geohash': c.get('mean_geohash', None),
                'std_geohash_m': c.get('std_geohash_m', None),
                'mean_time_diff_seconds': c.get('mean_time_diff_seconds', None),
            },
            'level_1_primary': {
                'pings': visits,
                'unique_days': unique_days,
                'active_day_ratio': active_day_ratio,
                'gap_bins': dict(c['gap_bins']),
            },
            'level_2_secondary': {
                'hourly_hist': list(c['hourly_hist']),
                'weekday_hist': list(c['weekday_hist']),
                'monthly_hist': dict(c['monthly_hist']),
                'night_ratio': night_ratio,
                'weekday_day_ratio': weekday_day_ratio,
                'weekend_ratio': weekend_ratio,
                'midday_weekday_ratio': midday_weekday_ratio,
                'evening_ratio': evening_ratio,
                'early_late_overlap_day_ratio': late_evening_days_ratio,  # kept for backward compat
                'night_days_ratio': night_days_ratio,
                'weekday_work_days_ratio': weekday_work_days_ratio,
                'late_evening_days_ratio': late_evening_days_ratio,
                'early_morning_days_ratio': early_morning_days_ratio,
                'entropy_hour_norm': entropy_hour_norm,
                'monthly_stability': monthly_stability,
                'active_days_last_30d': active_days_last_30d,
            },
            'level_3_tertiary': {
                "poi_available": poi_available,
                "poi_info": poi_info
            },
            'level_4_duration': duration_info,
            'level_5_flux': {
                'flux_counts': dict(c.get('flux_counts', {'B': 0, 'C': 0, 'D': 0, 'E': 0, 'F': 0}))
            }
        }
    
    @staticmethod
    def score_home(ev: dict, a: float = 2.0) -> float:
        l1, l2 = ev['level_1_primary'], ev['level_2_secondary']
        visits = l1['pings']
        days = l1['unique_days']

        # Bayesian shrinkage priors
        p0_night = 8.0 / 24.0            # baseline night share
        night_ratio_shrunk = EvidenceStore._shrink_ratio(l2['night_ratio'], visits, p0_night, a=a)

        # sample-size weights
        w_visits = 1.0 - math.exp(-visits / 5.0)
        w_days = 1.0 - math.exp(-days / 3.0)    

        base = (
            0.375 * l2['night_days_ratio']
          + 0.10 * night_ratio_shrunk
          + 0.15 * l2['late_evening_days_ratio']
          + 0.10 * l2['early_morning_days_ratio']  # Added early morning evidence
          + 0.075 * (1.0 - l2['entropy_hour_norm'])
          + 0.25 * l1['active_day_ratio']  # Reduced from 0.30 to accommodate new features
          + 0.05 * l2['monthly_stability']  # Added monthly stability
        )
        s = base * w_visits * w_days

        # recency boost
        s *= min(1.0, 0.5 + 0.5 * (l2['active_days_last_30d'] / 10))
        return max(0.0, min(1.0, s))

    @staticmethod
    def score_work(ev: dict, a: float = 2.0) -> float:
        l1, l2 = ev['level_1_primary'], ev['level_2_secondary']
        visits = l1['pings']
        days = l1['unique_days']

        # Bayesian shrinkage priors
        p0_wd_day = 45.0 / 168.0         # (5 weekdays * 9 hours) / (7 days * 24 hours)
        weekday_day_ratio_shrunk = EvidenceStore._shrink_ratio(l2['weekday_day_ratio'], visits, p0_wd_day, a=a)

        # sample-size weights
        w_visits = 1.0 - math.exp(-visits / 5.0)
        w_days = 1.0 - math.exp(-days / 3.0)    

        base = (
            0.425 * l2['weekday_work_days_ratio']
          + 0.15 * weekday_day_ratio_shrunk
          + 0.10 * l2['midday_weekday_ratio']
          + 0.075 * (1.0 - l2['entropy_hour_norm'])
          + 0.20 * l1['active_day_ratio']  # Reduced from 0.25 to accommodate new features
          + 0.05 * l2['monthly_stability']  # Added monthly stability
        )
        s = base * w_visits * w_days

        # recency boost
        s *= min(1.0, 0.5 + 0.5 * (l2['active_days_last_30d'] / 10))
        return max(0.0, min(1.0, s))
    @staticmethod
    def score_leisure(ev: dict, a: float = 2.0) -> float:
        l1, l2 = ev['level_1_primary'], ev['level_2_secondary']
        visits = l1['pings']
        days = l1['unique_days']

        # Calculate home and work scores to invert them for leisure
        home_score = EvidenceStore.score_home(ev, a)
        work_score = EvidenceStore.score_work(ev, a)
        
        # Combine and invert home and work patterns
        combined_home_work = (home_score + work_score) / 2
        inverse_pattern = 1.0 - combined_home_work
        
        # Bayesian shrinkage priors
        p0_weekend = 2.0 / 7.0           # baseline weekend share
        weekend_ratio_shrunk = EvidenceStore._shrink_ratio(l2['weekend_ratio'], visits, p0_weekend, a=a)
        
        # Add evening_ratio (18-21h) with shrinkage
        p0_evening = 4.0 / 24.0
        evening_ratio_shrunk = EvidenceStore._shrink_ratio(l2['evening_ratio'], visits, p0_evening, a=a)
    
    

        # sample-size weights
        w_visits = 1.0 - math.exp(-visits / 5.0)
        w_days = 1.0 - math.exp(-days / 3.0)    
        
        base = (
            0.25 * weekend_ratio_shrunk
          + 0.20 * evening_ratio_shrunk
          + 0.15 * (1-l2['entropy_hour_norm'])
          + 0.10 * (1.0 - l2['monthly_stability'])
          + 0.30 * inverse_pattern  # Add inverse pattern from home and work
        )
        s = base * w_visits * w_days

        # Relaxed recency boost (leisure may not be recent frequently)
        s *= min(1.0, 0.5 + 0.5 * (l2['active_days_last_30d'] / 15))
        return max(0.0, min(1.0, s))
    def score_pingsink(self, ev: dict) -> float:
        """Calculate pingsink score based on geographic stability and temporal patterns
        
        Args:
            ev: Evidence dictionary from derive()
            
        Returns:
            Pingsink score between 0 and 1, where higher = more likely to be a stable location (sink)
            
        Formula:
            0.4 * exp(-std_geohash_m/40)              # Geographic stability (low std = stable)
          + 0.3 * exp(-mean_time_diff/60)             # Temporal density (low time_diff = frequent)
          + 0.3 * (1 - exp(-pings/50))                # Ping volume (saturates at ~100-150 pings)
          * (0.8 + 0.2 * sqrt(pings/total_pings))     # Relative importance multiplier
        """
        meta = ev['meta']
        l1 = ev['level_1_primary']
        pings = l1['pings']
        if pings <= 5:
            return 0.0
        std_m = meta.get('std_geohash_m', None)
        if std_m == 0:
            return 1.0
        if std_m is not None:
            geo_stability = 0.7 * math.exp(-std_m / 20.0)
        else:
            geo_stability = 0.0
        mean_time_diff = meta.get('mean_time_diff_seconds', None)
        if mean_time_diff is not None:
            time_diff_minutes = mean_time_diff / 60.0
            temporal_density = 0.1 * math.exp(-time_diff_minutes / 60.0)
        else:
            temporal_density = 0.0
        
        # Ping volume (30%): exponential saturation
        # Low pings → penalty, saturates at ~100-150 pings
        ping_factor = 1.0 - math.exp(-pings / 50.0)
        ping_contribution = 0.2 * ping_factor
        
        # Combine components
        base_score = geo_stability + temporal_density + ping_contribution
        
        # Relative importance boost (multiplicative)
        if self.total_pings > 0:
            relative_importance = pings / self.total_pings
            importance_multiplier = 0.8 + 0.2 * math.sqrt(relative_importance)
        else:
            importance_multiplier = 1.0
        
        final_score = base_score * importance_multiplier
        return max(0.0, min(1.0, final_score))
    def overall_score(self, ev: dict, a: float = 2.0) -> dict:
        """Calculate overall scores for all categories
        
        Args:
            ev: Evidence dictionary from derive()
            a: Bayesian shrinkage parameter
            
        Returns:
            Dictionary with scores for home, work, leisure, pingsink, and optional POI category
        """
        home_score = self.score_home(ev, a)
        work_score = self.score_work(ev, a)
        leisure_score = self.score_leisure(ev, a)
        pingsink_score = self.score_pingsink(ev)
        
        overall_score = {
            'home': home_score,
            'work': work_score,
            'leisure': leisure_score,
            'pingsink': pingsink_score
        }
        
        if ev['level_3_tertiary']['poi_available']:
            poi_info = ev['level_3_tertiary']['poi_info']
            if poi_info:
                poi_label = poi_info['primary_category']
                if poi_label != 'path':
                    if poi_label in overall_score:
                        overall_score[poi_label] = 0.25 * overall_score[poi_label] + 0.75 * poi_info.get('confidence', 0.01) / 100.0
                elif poi_label == 'path':
                    overall_score[poi_label] = poi_info.get('confidence', 0.01) / 100.0

        return overall_score
    