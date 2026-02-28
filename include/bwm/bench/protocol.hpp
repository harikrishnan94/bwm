#pragma once

#include <chrono>
#include <cstdint>
#include <span>

#include "bwm/core/types.hpp"

namespace bwm {

struct TimedPhaseResult {
  PhaseMetrics metrics{};
  uint64_t total_uncompressed_bytes{};
  uint64_t total_compressed_bytes{};
  uint64_t sink_hash{};
};

TimedPhaseResult run_timed_phase(const PhaseConfig& cfg,
                                 std::chrono::seconds duration,
                                 std::chrono::seconds warmup);

AggregateMetrics summarize_repeats(std::span<const TimedPhaseResult> repeats);

}  // namespace bwm
