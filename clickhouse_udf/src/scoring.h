#ifndef SCORING_H
#define SCORING_H

#include <cmath>
#include <iostream>
#include <vector>
#include <cstdint>

// Reads a VarUInt from the stream
uint64_t readVarUInt(std::istream& in);

// Helper for shrink ratio
double shrink_ratio(double ratio, uint64_t n, double p0, double a = 8.0);

// Pingsink
struct PingsinkRow {
    uint64_t pings;
    double std_geohash_m;
    double mean_time_diff;
    uint64_t total_pings;
};
double calculate_pingsink(const PingsinkRow& row);

// Home/Work/Leisure common args
struct HomeWorkRow {
    uint64_t pings;
    uint64_t unique_days;
    double night_ratio;
    double night_days_ratio;
    double late_evening_days_ratio;
    double early_morning_days_ratio;
    double entropy_hour_norm;
    double active_day_ratio;
    double monthly_stability;
    uint64_t active_days_last_30d;
    
    // For Work:
    double weekday_day_ratio;
    double weekday_work_days_ratio;
    double midday_weekday_ratio;

    // For Leisure:
    double weekend_ratio;
    double evening_ratio;
    // inverse pattern is derived, but depends on home/work score?
    // Wait, Python code: combined_home_work = (home_score + work_score) / 2
    // So Leisure depends on Home and Work scores!
    // We should probably pass home_score and work_score if calculating leisure?
    // Or recalculate them inside? Recalculating is safer but slower.
    // Or calculate all 3 in one go?
    // "overall_score" function in Python calculates all.
    // Let's make a function "calculate_all_scores" that returns a Tuple?
    // Input would be ALL metrics.
};

// Simplified inputs for specific scores
struct HomeRow {
    uint64_t pings;
    uint64_t unique_days;
    double night_ratio;
    double night_days_ratio;
    double late_evening_days_ratio;
    double early_morning_days_ratio;
    double entropy_hour_norm;
    double active_day_ratio;
    double monthly_stability;
    uint64_t active_days_last_30d;
};
double calculate_home(const HomeRow& row);

struct WorkRow {
    uint64_t pings;
    uint64_t unique_days;
    double weekday_day_ratio;
    double weekday_work_days_ratio;
    double midday_weekday_ratio;
    double entropy_hour_norm;
    double active_day_ratio;
    double monthly_stability;
    uint64_t active_days_last_30d;
};
double calculate_work(const WorkRow& row);

// Leisure needs result of Home and Work... 
// Pass home_score and work_score as inputs?
struct LeisureRow {
    uint64_t pings;
    uint64_t unique_days;
    double weekend_ratio;
    double evening_ratio;
    double entropy_hour_norm;
    double monthly_stability;
    uint64_t active_days_last_30d;
    double home_score;
    double work_score;
};
double calculate_leisure(const LeisureRow& row);

// Spatial stats
struct SpatialResult {
    double mean_lat;
    double mean_lon;
    double std_m;
};
SpatialResult calculate_spatial(const std::vector<double>& lats, const std::vector<double>& lons);


#endif
