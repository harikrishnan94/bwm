#include "app/math_utils.hpp"

#include <algorithm>
#include <cmath>
#include <numeric>

namespace bwm::app {

bool is_power_of_two(size_t v) { return v != 0 && (v & (v - 1)) == 0; }

size_t ceil_pow2(size_t v) {
  if (v < 2) {
    return 2;
  }
  size_t n = 1;
  while (n < v) {
    n <<= 1;
  }
  return n;
}

Stats calc_stats(std::vector<double> values) {
  Stats out{};
  if (values.empty()) {
    return out;
  }
  std::sort(values.begin(), values.end());
  out.min = values.front();
  out.max = values.back();
  const double sum = std::accumulate(values.begin(), values.end(), 0.0);
  out.mean = sum / static_cast<double>(values.size());

  const auto percentile = [&](double p) {
    const double idx = p * static_cast<double>(values.size() - 1);
    const auto lo = static_cast<size_t>(std::floor(idx));
    const auto hi = static_cast<size_t>(std::ceil(idx));
    if (lo == hi) {
      return values[lo];
    }
    const double frac = idx - static_cast<double>(lo);
    return values[lo] * (1.0 - frac) + values[hi] * frac;
  };

  out.median = percentile(0.50);
  out.p95 = percentile(0.95);
  return out;
}

double to_gbps(uint64_t bytes, double sec) {
  if (sec <= 0.0) {
    return 0.0;
  }
  return static_cast<double>(bytes) / sec / 1e9;
}

uint64_t xorshift64(uint64_t& s) {
  s ^= s << 13;
  s ^= s >> 7;
  s ^= s << 17;
  return s;
}

}  // namespace bwm::app
