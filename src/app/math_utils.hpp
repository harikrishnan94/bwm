#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "app/config_types.hpp"

namespace bwm::app {

bool is_power_of_two(size_t v);
size_t ceil_pow2(size_t v);
Stats calc_stats(std::vector<double> values);
double to_gbps(uint64_t bytes, double sec);
uint64_t xorshift64(uint64_t& s);

}  // namespace bwm::app
