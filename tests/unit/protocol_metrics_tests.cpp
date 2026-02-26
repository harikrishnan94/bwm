#include <cmath>
#include <iostream>
#include <vector>

#include "bwm/bench/protocol.hpp"

int main() {
  std::vector<bwm::TimedPhaseResult> repeats(5);
  repeats[0].metrics.eff_gbps = 1.0;
  repeats[1].metrics.eff_gbps = 2.0;
  repeats[2].metrics.eff_gbps = 3.0;
  repeats[3].metrics.eff_gbps = 4.0;
  repeats[4].metrics.eff_gbps = 5.0;

  const auto m = bwm::summarize_repeats(repeats);

  auto close = [](double a, double b) { return std::fabs(a - b) < 1e-9; };

  if (!close(m.mean, 3.0)) {
    std::cerr << "mean mismatch: " << m.mean << "\n";
    return 1;
  }
  if (!close(m.median, 3.0)) {
    std::cerr << "median mismatch: " << m.median << "\n";
    return 1;
  }
  if (!close(m.min, 1.0) || !close(m.max, 5.0)) {
    std::cerr << "min/max mismatch: " << m.min << "/" << m.max << "\n";
    return 1;
  }
  if (!(m.p95 >= 4.7 && m.p95 <= 5.0)) {
    std::cerr << "p95 out of expected range: " << m.p95 << "\n";
    return 1;
  }

  return 0;
}
