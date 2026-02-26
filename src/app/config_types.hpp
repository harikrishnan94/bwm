#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace bwm::app {

enum class Medium { Disk, Tcp };
enum class Mode { RawWrite, CompWrite, RawRead, DecompRead, SuiteAll };
enum class Role { Auto, Sender, Receiver };
enum class Algo : uint8_t { None = 0, Lz4 = 1, Zstd = 2 };

struct Config {
  Medium medium{Medium::Disk};
  Mode mode{Mode::SuiteAll};
  Role role{Role::Auto};
  Algo algo{Algo::Lz4};

  int zstd_level{1};
  uint16_t port{9191};

  uint32_t repeats{5};
  uint32_t warmup_sec{0};
  uint32_t duration_sec{10};
  uint32_t threads{std::max(1u, std::thread::hardware_concurrency())};
  uint32_t queue_depth{1024};
  size_t chunk_size{1024ULL * 1024ULL};
  size_t segment_size_bytes{1024ULL * 1024ULL * 1024ULL};

  uint64_t seed{1};
  double target_cr{0.0};
  double cr_tolerance{0.20};

  bool direct_io{false};

  std::filesystem::path output_dir{"./bwm_out"};
  std::optional<std::filesystem::path> json_output;
  std::string executable_path;
};

struct Stats {
  double mean{0.0};
  double median{0.0};
  double p95{0.0};
  double min{0.0};
  double max{0.0};
};

struct PhaseResult {
  Mode mode{};
  double wall_sec{0.0};
  uint64_t uncompressed_bytes{0};
  uint64_t compressed_bytes{0};
  uint64_t sink_hash{0};
};

struct Summary {
  std::map<Mode, std::vector<PhaseResult>> phases;
};

}  // namespace bwm::app
