#pragma once

#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "bwm/core/types.hpp"

namespace bwm {

struct WriteOpenParams {
  std::string path{};
  std::string host{"127.0.0.1"};
  uint16_t port{9191};
  uint32_t segment_id{0};
  bool truncate{true};
};

struct ReadOpenParams {
  std::vector<std::string> segment_paths{};
  std::string host{"127.0.0.1"};
  uint16_t port{9191};
};

struct MediumCaps {
  bool direct_io_supported{false};
  bool cache_hint_supported{false};
  bool requires_sync_for_durability{true};
};

class IMediumWriter {
 public:
  virtual ~IMediumWriter() = default;

  virtual void append_chunk(const ChunkHeader& hdr,
                            std::span<const std::byte> payload) = 0;
  virtual void finalize_segment() = 0;
  virtual void sync() = 0;
  virtual void close() = 0;
};

class IMediumReader {
 public:
  virtual ~IMediumReader() = default;

  virtual OwnedChunk read_chunk_by_index(uint32_t segment_id,
                                         uint32_t chunk_id) = 0;
  virtual void close() = 0;
};

class IMedium {
 public:
  virtual ~IMedium() = default;

  virtual MediumKind kind() const noexcept = 0;
  virtual MediumCaps capabilities() const noexcept = 0;

  virtual std::unique_ptr<IMediumWriter> open_writer(const WriteOpenParams&) = 0;
  virtual std::unique_ptr<IMediumReader> open_reader(const ReadOpenParams&) = 0;
};

std::unique_ptr<IMedium> make_disk_medium(const RunConfig&);
std::unique_ptr<IMedium> make_tcp_sender_medium(const RunConfig&);
std::unique_ptr<IMedium> make_tcp_receiver_medium(const RunConfig&);

}  // namespace bwm
