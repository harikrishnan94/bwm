#include "bwm/bench/protocol.hpp"

#include <algorithm>
#include <string>
#include <vector>

#include "app/runtime_runner.hpp"
#include "app/math_utils.hpp"
#include "bwm/core/error.hpp"

namespace bwm {
namespace {

bwm::app::Medium to_app_medium(MediumKind medium) {
  switch (medium) {
    case MediumKind::Disk:
      return bwm::app::Medium::Disk;
    case MediumKind::Tcp:
      return bwm::app::Medium::Tcp;
  }
  return bwm::app::Medium::Disk;
}

bwm::app::Mode to_app_mode(Mode mode) {
  switch (mode) {
    case Mode::RawWrite:
      return bwm::app::Mode::RawWrite;
    case Mode::CompWrite:
      return bwm::app::Mode::CompWrite;
    case Mode::RawRead:
      return bwm::app::Mode::RawRead;
    case Mode::DecompRead:
      return bwm::app::Mode::DecompRead;
    case Mode::SuiteAll:
      return bwm::app::Mode::SuiteAll;
  }
  return bwm::app::Mode::RawWrite;
}

bwm::app::Algo to_app_algo(CodecId codec) {
  switch (codec) {
    case CodecId::None:
      return bwm::app::Algo::None;
    case CodecId::Lz4:
      return bwm::app::Algo::Lz4;
    case CodecId::Zstd:
      return bwm::app::Algo::Zstd;
  }
  return bwm::app::Algo::None;
}

AggregateMetrics calc_stats(std::vector<double> values) {
  AggregateMetrics out{};
  if (values.empty()) {
    return out;
  }

  std::sort(values.begin(), values.end());
  out.min = values.front();
  out.max = values.back();

  double sum = 0.0;
  for (double v : values) {
    sum += v;
  }
  out.mean = sum / static_cast<double>(values.size());

  const auto percentile = [&](double p) {
    const double idx = p * static_cast<double>(values.size() - 1);
    const auto lo = static_cast<size_t>(idx);
    const auto hi = (lo + 1 < values.size()) ? lo + 1 : lo;
    const double frac = idx - static_cast<double>(lo);
    return values[lo] * (1.0 - frac) + values[hi] * frac;
  };

  out.median = percentile(0.50);
  out.p95 = percentile(0.95);
  return out;
}

}  // namespace

TimedPhaseResult run_timed_phase(const PhaseConfig& cfg,
                                 std::chrono::seconds duration,
                                 std::chrono::seconds warmup) {
  if (cfg.mode == Mode::SuiteAll) {
    throw Error{ErrorCode::InvalidArgument,
                "run_timed_phase requires a concrete phase mode"};
  }
  if (duration.count() <= 0) {
    throw Error{ErrorCode::InvalidArgument,
                "duration must be > 0"};
  }

  bwm::app::Config app_cfg{};
  app_cfg.medium = to_app_medium(cfg.run.medium);
  app_cfg.mode = to_app_mode(cfg.mode);
  app_cfg.role = bwm::app::Role::Auto;
  app_cfg.algo = to_app_algo(cfg.codec);

  app_cfg.zstd_level = cfg.codec_level;
  app_cfg.port = cfg.run.tcp_port;
  app_cfg.threads = std::max(1u, cfg.run.worker_threads);
  app_cfg.queue_depth = std::max(16u, cfg.run.queue_depth);
  app_cfg.chunk_size = cfg.chunk_size;
  app_cfg.seed = cfg.seed;
  app_cfg.direct_io = cfg.run.direct_io;
  app_cfg.output_dir = cfg.run.output_dir;

  const auto phase_mode = to_app_mode(cfg.mode);

  if (warmup.count() > 0) {
    static_cast<void>(bwm::app::run_phase_for_protocol(app_cfg, phase_mode, warmup, 0, true));
  }

  const auto timed = bwm::app::run_phase_for_protocol(app_cfg, phase_mode, duration, 0, false);

  TimedPhaseResult out{};
  out.total_uncompressed_bytes = timed.uncompressed_bytes;
  out.total_compressed_bytes = timed.compressed_bytes;
  out.sink_hash = timed.sink_hash;

  out.metrics.wall_sec = timed.wall_sec;
  out.metrics.bytes_uncompressed = timed.uncompressed_bytes;
  out.metrics.bytes_compressed = timed.compressed_bytes;
  out.metrics.eff_gbps = bwm::app::to_gbps(timed.uncompressed_bytes, timed.wall_sec);
  out.metrics.cr_observed = timed.compressed_bytes == 0
                                ? 0.0
                                : static_cast<double>(timed.uncompressed_bytes) /
                                      static_cast<double>(timed.compressed_bytes);

  return out;
}

AggregateMetrics summarize_repeats(std::span<const TimedPhaseResult> repeats) {
  std::vector<double> eff_values;
  eff_values.reserve(repeats.size());

  for (const auto& r : repeats) {
    eff_values.push_back(r.metrics.eff_gbps);
  }

  return calc_stats(std::move(eff_values));
}

}  // namespace bwm
