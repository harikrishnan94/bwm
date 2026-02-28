#include <chrono>
#include <filesystem>
#include <format>
#include <iostream>

#include "bwm/bench/protocol.hpp"
#include "bwm/core/error.hpp"

namespace {

bool test_invalid_mode_rejected() {
  bwm::PhaseConfig cfg{};
  cfg.mode = bwm::Mode::SuiteAll;

  bool rejected = false;
  try {
    static_cast<void>(bwm::run_timed_phase(cfg, std::chrono::seconds(1),
                                           std::chrono::seconds(0)));
  } catch (const bwm::Error &e) {
    rejected = (e.code() == bwm::ErrorCode::InvalidArgument);
  }
  if (!rejected) {
    std::cerr << std::format("expected invalid argument for SuiteAll mode\n");
    return false;
  }
  return true;
}

bool test_invalid_duration_rejected() {
  bwm::PhaseConfig cfg{};
  cfg.mode = bwm::Mode::RawWrite;

  bool rejected = false;
  try {
    static_cast<void>(bwm::run_timed_phase(cfg, std::chrono::seconds(0),
                                           std::chrono::seconds(0)));
  } catch (const bwm::Error &e) {
    rejected = (e.code() == bwm::ErrorCode::InvalidArgument);
  }
  if (!rejected) {
    std::cerr << std::format("expected invalid argument for zero duration\n");
    return false;
  }
  return true;
}

bool test_disk_raw_write_metrics() {
  const std::filesystem::path out_dir = "./bwm_test_protocol_out";
  std::error_code ec;
  std::filesystem::remove_all(out_dir, ec);

  bwm::PhaseConfig cfg{};
  cfg.mode = bwm::Mode::RawWrite;
  cfg.codec = bwm::CodecId::None;
  cfg.seed = 7;
  cfg.chunk_size = 4096;
  cfg.run.medium = bwm::MediumKind::Disk;
  cfg.run.output_dir = out_dir.string();
  cfg.run.worker_threads = 1;
  cfg.run.queue_depth = 64;

  bwm::TimedPhaseResult r{};
  try {
    r = bwm::run_timed_phase(cfg, std::chrono::seconds(1),
                             std::chrono::seconds(0));
  } catch (const bwm::Error &e) {
    std::cerr << std::format("run_timed_phase failed: {}\n", e.what());
    return false;
  }
  if (r.total_uncompressed_bytes == 0) {
    std::cerr << std::format("expected non-zero uncompressed bytes\n");
    return false;
  }
  if (r.metrics.eff_gbps <= 0.0) {
    std::cerr << std::format("expected positive effective throughput\n");
    return false;
  }
  if (r.metrics.bytes_uncompressed != r.total_uncompressed_bytes) {
    std::cerr << std::format("bytes_uncompressed mismatch\n");
    return false;
  }

  std::filesystem::remove_all(out_dir, ec);
  return true;
}

} // namespace

int main() {
  if (!test_invalid_mode_rejected()) {
    return 1;
  }
  if (!test_invalid_duration_rejected()) {
    return 1;
  }
  if (!test_disk_raw_write_metrics()) {
    return 1;
  }
  return 0;
}
