#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace bwm {

enum class MediumKind { Disk, Tcp };
enum class Mode { RawWrite, CompWrite, RawRead, DecompRead, SuiteAll };
enum class CodecId : uint8_t { None = 0, Lz4 = 1, Zstd = 2 };

struct RunConfig {
  MediumKind medium{MediumKind::Disk};
  std::string output_dir{"./bwm_out"};
  uint16_t tcp_port{9191};
  bool direct_io{false};
  uint32_t worker_threads{1};
  uint32_t queue_depth{1024};
};

struct PhaseConfig {
  RunConfig run{};
  Mode mode{Mode::RawWrite};
  CodecId codec{CodecId::None};
  int codec_level{1};
  uint64_t seed{1};
  size_t chunk_size{1024 * 1024};
};

struct ChunkView {
  std::span<const std::byte> bytes{};
  uint64_t sequence{};
};

struct OwnedChunk {
  std::vector<std::byte> storage{};
  uint64_t sequence{};
};

struct ChunkHeader {
  uint32_t raw_size{};
  uint32_t comp_size{};
  uint32_t checksum{};
  uint8_t algo_id{};
};

struct SegmentHeader {
  uint32_t version{};
  uint32_t segment_id{};
  uint64_t chunk_count{};
};

struct SegmentIndexEntry {
  uint64_t offset{};
  uint32_t size{};
  uint32_t raw_size{};
};

struct PhaseMetrics {
  double wall_sec{};
  double eff_gbps{};
  double cr_observed{};
  uint64_t bytes_uncompressed{};
  uint64_t bytes_compressed{};
};

struct AggregateMetrics {
  double mean{};
  double median{};
  double p95{};
  double min{};
  double max{};
};

}  // namespace bwm
