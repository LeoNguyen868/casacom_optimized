#include "scoring.h"
#include <iostream>
#include <cstring>
#include <string>

// To avoid boilerplate, define readRow helpers
template<typename T>
bool readVal(std::istream& in, T& val) {
    return (bool)in.read(reinterpret_cast<char*>(&val), sizeof(T));
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: scoring_tool <mode>" << std::endl;
        return 1;
    }
    
    std::string mode = argv[1];
    
    if (mode == "pingsink") {
        PingsinkRow row;
        while (std::cin && !std::cin.eof()) {
            if (!readVal(std::cin, row.pings)) break;
            readVal(std::cin, row.std_geohash_m);
            readVal(std::cin, row.mean_time_diff);
            readVal(std::cin, row.total_pings);
            
            double res = calculate_pingsink(row);
            std::cout.write(reinterpret_cast<const char*>(&res), sizeof(res));
            std::cout.flush();
        }
    } else if (mode == "home") {
        HomeRow row;
        while (std::cin && !std::cin.eof()) {
            if (!readVal(std::cin, row.pings)) break;
            readVal(std::cin, row.unique_days);
            readVal(std::cin, row.night_ratio);
            readVal(std::cin, row.night_days_ratio);
            readVal(std::cin, row.late_evening_days_ratio);
            readVal(std::cin, row.early_morning_days_ratio);
            readVal(std::cin, row.entropy_hour_norm);
            readVal(std::cin, row.active_day_ratio);
            readVal(std::cin, row.monthly_stability);
            readVal(std::cin, row.active_days_last_30d);
            
            double res = calculate_home(row);
            std::cout.write(reinterpret_cast<const char*>(&res), sizeof(res));
            std::cout.flush();
        }
    } else if (mode == "work") {
        WorkRow row;
        while (std::cin && !std::cin.eof()) {
            if (!readVal(std::cin, row.pings)) break;
            readVal(std::cin, row.unique_days);
            readVal(std::cin, row.weekday_day_ratio);
            readVal(std::cin, row.weekday_work_days_ratio);
            readVal(std::cin, row.midday_weekday_ratio);
            readVal(std::cin, row.entropy_hour_norm);
            readVal(std::cin, row.active_day_ratio);
            readVal(std::cin, row.monthly_stability);
            readVal(std::cin, row.active_days_last_30d);
            
            double res = calculate_work(row);
            std::cout.write(reinterpret_cast<const char*>(&res), sizeof(res));
            std::cout.flush();
        }
    } else if (mode == "leisure") {
        LeisureRow row;
        while (std::cin && !std::cin.eof()) {
            if (!readVal(std::cin, row.pings)) break;
            readVal(std::cin, row.unique_days);
            readVal(std::cin, row.weekend_ratio);
            readVal(std::cin, row.evening_ratio);
            readVal(std::cin, row.entropy_hour_norm);
            readVal(std::cin, row.monthly_stability);
            readVal(std::cin, row.active_days_last_30d);
            readVal(std::cin, row.home_score);
            readVal(std::cin, row.work_score);
            
            double res = calculate_leisure(row);
            std::cout.write(reinterpret_cast<const char*>(&res), sizeof(res));
            std::cout.flush();
        }
    } else if (mode == "spatial") {
        // Input: Array(Float64) lats, Array(Float64) lons
        // Output: Float64 mean_lat, Float64 mean_lon, Float64 std_m
        
        while (std::cin && std::cin.peek() != EOF) { // peek check to avoid reading header
             // Check EOF properly
             if (std::cin.eof()) break;
             if (std::cin.peek() == EOF) break;

             // Read lats array
             uint64_t lat_len = readVarUInt(std::cin);
             if (!std::cin) break;
             std::vector<double> lats(lat_len);
             if (lat_len > 0) {
                if (!std::cin.read(reinterpret_cast<char*>(lats.data()), lat_len * sizeof(double))) break;
             }
             
             // Read lons array
             uint64_t lon_len = readVarUInt(std::cin);
             if (!std::cin) break;
             std::vector<double> lons(lon_len);
             if (lon_len > 0) {
                if (!std::cin.read(reinterpret_cast<char*>(lons.data()), lon_len * sizeof(double))) break;
             }
             
             SpatialResult res = calculate_spatial(lats, lons);
             
             std::cout.write(reinterpret_cast<const char*>(&res.mean_lat), sizeof(double));
             std::cout.write(reinterpret_cast<const char*>(&res.mean_lon), sizeof(double));
             std::cout.write(reinterpret_cast<const char*>(&res.std_m), sizeof(double));
             std::cout.flush();
        }
    } else {
        std::cerr << "Unknown mode: " << mode << std::endl;
        return 1;
    }
    
    return 0;
}
