#include "scoring.h"
#include <algorithm>

uint64_t readVarUInt(std::istream& in) {
    uint64_t x = 0;
    for (int i = 0; i < 9; ++i) {
        char byte;
        if (!in.read(&byte, 1)) {
            // End of stream or error
            return 0; 
        }
        x |= (static_cast<uint64_t>(byte & 0x7F) << (7 * i));
        if (!(byte & 0x80)) {
            return x;
        }
    }
    return x;
}

double shrink_ratio(double ratio, uint64_t n, double p0, double a) {
    double denom = (double)n + a;
    if (denom > 0) {
        return (ratio * n + a * p0) / denom;
    }
    return p0;
}

double calculate_pingsink(const PingsinkRow& row) {
    if (row.pings <= 5) return 0.0;
    if (std::abs(row.std_geohash_m) < 1e-9) return 1.0;
    
    double geo_stability = 0.7 * std::exp(-row.std_geohash_m / 20.0);
    double time_diff_minutes = row.mean_time_diff / 60.0;
    double temporal_density = 0.1 * std::exp(-time_diff_minutes / 60.0);

    double ping_factor = 1.0 - std::exp(-(double)row.pings / 50.0);
    double ping_contribution = 0.2 * ping_factor;

    double base_score = geo_stability + temporal_density + ping_contribution;
    
    double importance_multiplier = 1.0;
    if (row.total_pings > 0) {
        double relative_importance = (double)row.pings / (double)row.total_pings;
        importance_multiplier = 0.8 + 0.2 * std::sqrt(relative_importance);
    }
    
    return std::clamp(base_score * importance_multiplier, 0.0, 1.0);
}

double calculate_home(const HomeRow& row) {
    double visits = (double)row.pings;
    double days = (double)row.unique_days;
    double a = 2.0;

    double p0_night = 8.0 / 24.0;
    double night_ratio_shrunk = shrink_ratio(row.night_ratio, row.pings, p0_night, a);

    double w_visits = 1.0 - std::exp(-visits / 5.0);
    double w_days = 1.0 - std::exp(-days / 3.0);

    double base = (
          0.375 * row.night_days_ratio
        + 0.10 * night_ratio_shrunk
        + 0.15 * row.late_evening_days_ratio
        + 0.10 * row.early_morning_days_ratio
        + 0.075 * (1.0 - row.entropy_hour_norm)
        + 0.25 * row.active_day_ratio
        + 0.05 * row.monthly_stability
    );
    double s = base * w_visits * w_days;
    
    s *= std::min(1.0, 0.5 + 0.5 * ((double)row.active_days_last_30d / 10.0));
    
    return std::clamp(s, 0.0, 1.0);
}

double calculate_work(const WorkRow& row) {
    double visits = (double)row.pings;
    double days = (double)row.unique_days;
    double a = 2.0;

    double p0_wd_day = 45.0 / 168.0;
    double weekday_day_ratio_shrunk = shrink_ratio(row.weekday_day_ratio, row.pings, p0_wd_day, a);

    double w_visits = 1.0 - std::exp(-visits / 5.0);
    double w_days = 1.0 - std::exp(-days / 3.0);

    double base = (
          0.425 * row.weekday_work_days_ratio
        + 0.15 * weekday_day_ratio_shrunk
        + 0.10 * row.midday_weekday_ratio
        + 0.075 * (1.0 - row.entropy_hour_norm)
        + 0.20 * row.active_day_ratio
        + 0.05 * row.monthly_stability
    );
    double s = base * w_visits * w_days;
    
    s *= std::min(1.0, 0.5 + 0.5 * ((double)row.active_days_last_30d / 10.0));
    
    return std::clamp(s, 0.0, 1.0);
}

double calculate_leisure(const LeisureRow& row) {
    double visits = (double)row.pings;
    double days = (double)row.unique_days;
    double a = 2.0;

    double combined_home_work = (row.home_score + row.work_score) / 2.0;
    double inverse_pattern = 1.0 - combined_home_work;

    double p0_weekend = 2.0 / 7.0;
    double weekend_ratio_shrunk = shrink_ratio(row.weekend_ratio, row.pings, p0_weekend, a);

    double p0_evening = 4.0 / 24.0;
    double evening_ratio_shrunk = shrink_ratio(row.evening_ratio, row.pings, p0_evening, a);

    double w_visits = 1.0 - std::exp(-visits / 5.0);
    double w_days = 1.0 - std::exp(-days / 3.0);

    double base = (
          0.25 * weekend_ratio_shrunk
        + 0.20 * evening_ratio_shrunk
        + 0.15 * (1.0 - row.entropy_hour_norm)
        + 0.10 * (1.0 - row.monthly_stability)
        + 0.30 * inverse_pattern
    );
    double s = base * w_visits * w_days;
    
    s *= std::min(1.0, 0.5 + 0.5 * ((double)row.active_days_last_30d / 15.0));
    
    return std::clamp(s, 0.0, 1.0);
}

SpatialResult calculate_spatial(const std::vector<double>& lats, const std::vector<double>& lons) {
    if (lats.empty() || lats.size() != lons.size()) {
        return {0.0, 0.0, 0.0};
    }

    double sum_lat = 0.0;
    double sum_lon = 0.0;
    for (size_t i = 0; i < lats.size(); ++i) {
        sum_lat += lats[i];
        sum_lon += lons[i];
    }
    double mean_lat = sum_lat / lats.size();
    double mean_lon = sum_lon / lons.size();

    double m2 = 0.0;
    for (size_t i = 0; i < lats.size(); ++i) {
        // Haversine approx
        double dLat = (lats[i] - mean_lat) * M_PI / 180.0;
        double dLon = (lons[i] - mean_lon) * M_PI / 180.0;
        double lat1 = mean_lat * M_PI / 180.0;
        double lat2 = lats[i] * M_PI / 180.0;

        double a = std::sin(dLat/2) * std::sin(dLat/2) +
                   std::sin(dLon/2) * std::sin(dLon/2) * std::cos(lat1) * std::cos(lat2);
        double c = 2 * std::atan2(std::sqrt(a), std::sqrt(1-a));
        double dist = 6371000.0 * c;
        
        m2 += dist * dist;
    }
    
    double std_m = std::sqrt(m2 / lats.size());
    
    return {mean_lat, mean_lon, std_m};
}
