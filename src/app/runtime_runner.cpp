#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <span>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <numeric>
#include <limits>

#include "bwm/core/expected.hpp"
#include "app/config_types.hpp"
#include "app/math_utils.hpp"
#include "codec/runtime_codec.hpp"
#include "generator/low_entropy_generator.hpp"
#include "sink/hash_sink.hpp"

#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#if __has_include(<argparse/argparse.hpp>)
#define BWM_HAS_ARGPARSE 1
#include <argparse/argparse.hpp>
#else
#define BWM_HAS_ARGPARSE 0
#endif

#if __has_include(<lz4.h>)
#define BWM_HAS_LZ4 1
#include <lz4.h>
#else
#define BWM_HAS_LZ4 0
#endif

#if __has_include(<zstd.h>)
#define BWM_HAS_ZSTD 1
#include <zstd.h>
#else
#define BWM_HAS_ZSTD 0
#endif

#if __has_include(<xxhash.h>)
#include <xxhash.h>
#else
#error "xxhash.h is required"
#endif

namespace {

using Clock = std::chrono::steady_clock;
using Sec = std::chrono::seconds;

constexpr uint32_t kSegmentMagic = 0x424d5753;  // BMWS
constexpr uint32_t kSegmentVersion = 1;
#ifdef __linux__
constexpr size_t kIoAlign = 4096;
#endif

using AppError = bwm::Error;

template <typename T>
using Result = bwm::Expected<T>;

using bwm::app::Algo;
using bwm::app::Codec;
using bwm::app::Config;
using bwm::app::HashSink;
using bwm::app::LowEntropyGenerator;
using bwm::app::Medium;
using bwm::app::Mode;
using bwm::app::PhaseResult;
using bwm::app::Role;
using bwm::app::Stats;
using bwm::app::Summary;
using bwm::app::calc_stats;
using bwm::app::ceil_pow2;
using bwm::app::to_gbps;

std::string mode_to_string(Mode mode) {
  switch (mode) {
    case Mode::RawWrite:
      return "raw_write";
    case Mode::CompWrite:
      return "comp_write";
    case Mode::RawRead:
      return "raw_read";
    case Mode::DecompRead:
      return "decomp_read";
    case Mode::SuiteAll:
      return "all";
  }
  return "unknown";
}

std::string medium_to_string(Medium medium) {
  return medium == Medium::Disk ? "disk" : "tcp";
}

std::string algo_to_string(Algo algo) {
  switch (algo) {
    case Algo::None:
      return "none";
    case Algo::Lz4:
      return "lz4";
    case Algo::Zstd:
      return "zstd";
  }
  return "none";
}


template <typename T>
class BoundedMPMCQueue {
 private:
  struct Cell {
    std::atomic<size_t> sequence{};
    T data{};
  };

 public:
  explicit BoundedMPMCQueue(size_t capacity_pow2)
      : capacity_(ceil_pow2(capacity_pow2)),
        mask_(capacity_ - 1),
        buffer_(capacity_) {
    for (size_t i = 0; i < capacity_; ++i) {
      buffer_[i].sequence.store(i, std::memory_order_relaxed);
    }
  }

  template <typename U>
  bool try_push(U&& item) {
    Cell* cell = nullptr;
    size_t pos = enqueue_pos_.load(std::memory_order_relaxed);
    for (;;) {
      cell = &buffer_[pos & mask_];
      const size_t seq = cell->sequence.load(std::memory_order_acquire);
      const intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
      if (dif == 0) {
        if (enqueue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
          break;
        }
      } else if (dif < 0) {
        return false;
      } else {
        pos = enqueue_pos_.load(std::memory_order_relaxed);
      }
    }
    cell->data = std::forward<U>(item);
    cell->sequence.store(pos + 1, std::memory_order_release);
    return true;
  }

  bool try_pop(T& out) {
    Cell* cell = nullptr;
    size_t pos = dequeue_pos_.load(std::memory_order_relaxed);
    for (;;) {
      cell = &buffer_[pos & mask_];
      const size_t seq = cell->sequence.load(std::memory_order_acquire);
      const intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);
      if (dif == 0) {
        if (dequeue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
          break;
        }
      } else if (dif < 0) {
        return false;
      } else {
        pos = dequeue_pos_.load(std::memory_order_relaxed);
      }
    }
    out = std::move(cell->data);
    cell->sequence.store(pos + capacity_, std::memory_order_release);
    return true;
  }

 private:
  size_t capacity_;
  size_t mask_;
  std::vector<Cell> buffer_;
  std::atomic<size_t> enqueue_pos_{0};
  std::atomic<size_t> dequeue_pos_{0};
};

struct alignas(16) ChunkFrameHeader {
  uint32_t raw_size{0};
  uint32_t comp_size{0};
  uint32_t checksum{0};
  uint8_t algo_id{0};
  uint8_t reserved[3]{};
};
static_assert(sizeof(ChunkFrameHeader) == 16);

struct alignas(16) SegmentHeader {
  uint32_t magic{kSegmentMagic};
  uint32_t version{kSegmentVersion};
  uint32_t flags{0};
  uint32_t reserved{0};
};

struct ChunkLoc {
  size_t file_id{};
  uint64_t offset{};
  uint32_t stored_size{};
  uint32_t raw_size{};
  uint32_t checksum{};
  uint8_t algo_id{};
};

struct Dataset {
  std::filesystem::path dir;
  std::vector<std::filesystem::path> files;
  std::vector<int> fds;
  std::vector<ChunkLoc> chunks;
  bool compressed{false};
  uint64_t total_uncompressed{0};
  uint64_t total_stored{0};
};

Result<void> write_all_fd(int fd, const uint8_t* data, size_t size) {
  size_t done = 0;
  while (done < size) {
    const ssize_t n = ::write(fd, data + done, size - done);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return std::unexpected(AppError{"write failed: " + std::string(std::strerror(errno))});
    }
    done += static_cast<size_t>(n);
  }
  return {};
}

Result<void> pwrite_all_fd(int fd, const uint8_t* data, size_t size, off_t offset) {
  size_t done = 0;
  while (done < size) {
    const ssize_t n = ::pwrite(fd, data + done, size - done, offset + static_cast<off_t>(done));
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return std::unexpected(AppError{"pwrite failed: " + std::string(std::strerror(errno))});
    }
    if (n == 0) {
      return std::unexpected(AppError{"pwrite failed: no forward progress"});
    }
    done += static_cast<size_t>(n);
  }
  return {};
}

Result<void> pread_all_fd(int fd, uint8_t* data, size_t size, off_t offset) {
  size_t done = 0;
  while (done < size) {
    const ssize_t n = ::pread(fd, data + done, size - done, offset + static_cast<off_t>(done));
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return std::unexpected(AppError{"pread failed: " + std::string(std::strerror(errno))});
    }
    if (n == 0) {
      return std::unexpected(AppError{"unexpected EOF during pread"});
    }
    done += static_cast<size_t>(n);
  }
  return {};
}

void close_dataset(Dataset& ds) {
  for (const int fd : ds.fds) {
    if (fd >= 0) {
      ::close(fd);
    }
  }
  ds.fds.clear();
}

Result<int> open_file_write(const std::filesystem::path& p, bool direct_io) {
  int flags = O_CREAT | O_TRUNC | O_WRONLY;
#ifdef __linux__
  if (direct_io) {
    flags |= O_DIRECT;
  }
#else
  (void)direct_io;
#endif
  const int fd = ::open(p.c_str(), flags, 0644);
  if (fd < 0) {
    return std::unexpected(AppError{"open for write failed: " + p.string() + ": " + std::strerror(errno)});
  }
#ifdef __APPLE__
  (void)fcntl(fd, F_NOCACHE, 1);
#endif
  return fd;
}

Result<int> open_file_read(const std::filesystem::path& p, bool direct_io) {
  int flags = O_RDONLY;
#ifdef __linux__
  if (direct_io) {
    flags |= O_DIRECT;
  }
#else
  (void)direct_io;
#endif
  const int fd = ::open(p.c_str(), flags);
  if (fd < 0) {
    return std::unexpected(AppError{"open for read failed: " + p.string() + ": " + std::strerror(errno)});
  }
#ifdef __APPLE__
  (void)fcntl(fd, F_NOCACHE, 1);
#endif
  return fd;
}

Result<void> ensure_dir(const std::filesystem::path& p) {
  std::error_code ec;
  std::filesystem::create_directories(p, ec);
  if (ec) {
    return std::unexpected(AppError{"create directory failed: " + p.string() + ": " + ec.message()});
  }
  return {};
}

Result<void> calibrate_generator(LowEntropyGenerator& gen,
                                 Codec& codec,
                                 const Config& cfg,
                                 bool compressed_mode) {
  if (!compressed_mode || cfg.target_cr <= 1.0) {
    return {};
  }

  const std::array<uint32_t, 6> alphabets{2, 4, 8, 16, 32, 64};
  const std::array<uint32_t, 4> runs{8, 16, 32, 64};
  const std::array<uint32_t, 5> noise{0, 200, 1000, 5000, 10000};

  const size_t sample_chunks = 8;
  const size_t sample_size = cfg.chunk_size;

  double best_err = std::numeric_limits<double>::max();
  uint32_t best_a = 8;
  uint32_t best_r = 16;
  uint32_t best_n = 2000;

  std::vector<uint8_t> raw;
  std::vector<uint8_t> comp(codec.max_compressed_size(sample_size));

  for (const auto a : alphabets) {
    for (const auto r : runs) {
      for (const auto n : noise) {
        gen.set_profile(a, r, n);
        uint64_t unc = 0;
        uint64_t cmp = 0;
        bool failed = false;
        for (size_t i = 0; i < sample_chunks; ++i) {
          gen.generate(i, raw, sample_size);
          auto c = codec.compress(raw, comp);
          if (!c) {
            failed = true;
            break;
          }
          unc += static_cast<uint64_t>(raw.size());
          cmp += static_cast<uint64_t>(*c);
        }
        if (failed || cmp == 0) {
          continue;
        }
        const double cr = static_cast<double>(unc) / static_cast<double>(cmp);
        const double err = std::abs(cr - cfg.target_cr);
        if (err < best_err) {
          best_err = err;
          best_a = a;
          best_r = r;
          best_n = n;
        }
      }
    }
  }

  gen.set_profile(best_a, best_r, best_n);
  return {};
}

struct Job {
  uint64_t seq{0};
  uint64_t index{0};
  bool stop{false};
};

struct ProducedChunk {
  uint64_t seq{0};
  uint32_t raw_size{0};
  uint32_t comp_size{0};
  uint32_t checksum{0};
  uint8_t algo_id{0};
  std::vector<uint8_t> payload;
};

Result<void> maybe_fsync(int fd) {
#if defined(__APPLE__)
  if (::fsync(fd) != 0) {
    return std::unexpected(AppError{"fsync failed: " + std::string(std::strerror(errno))});
  }
#else
  if (::fdatasync(fd) != 0) {
    return std::unexpected(AppError{"fdatasync failed: " + std::string(std::strerror(errno))});
  }
#endif
  return {};
}

Result<void> validate_io_mode(const Config& cfg, bool compressed) {
#ifdef __linux__
  if (cfg.direct_io) {
    if (compressed) {
      return std::unexpected(
          AppError{"O_DIRECT requested, but compressed mode cannot satisfy alignment with variable chunk sizes"});
    }
    if (cfg.chunk_size % kIoAlign != 0) {
      return std::unexpected(AppError{"O_DIRECT requested, but chunk size is not 4096-byte aligned"});
    }
  }
#else
  (void)compressed;
  if (cfg.direct_io) {
    std::cerr << "[warn] --direct-io requested but unsupported on this platform; using cache hinting where available\n";
  }
#endif
  return {};
}

struct SegmentReservation {
  int fd{-1};
  size_t file_id{0};
  uint64_t offset{0};
};

class SegmentedFileWriter {
 public:
  SegmentedFileWriter(std::filesystem::path dir,
                      size_t segment_size_bytes,
                      bool compressed,
                      bool direct_io)
      : dir_(std::move(dir)),
        segment_size_bytes_(segment_size_bytes),
        compressed_(compressed),
        direct_io_(direct_io) {}

  Result<void> open_first() { return rotate_segment(); }

  Result<SegmentReservation> reserve(size_t bytes) {
    for (;;) {
      SegmentState* seg = current_.load(std::memory_order_acquire);
      if (seg == nullptr) {
        return std::unexpected(AppError{"segment writer not initialized"});
      }

      uint64_t start = seg->next_offset.load(std::memory_order_relaxed);
      while (true) {
        if (!fits_in_segment(start, bytes)) {
          break;
        }
        const uint64_t next = start + static_cast<uint64_t>(bytes);
        if (seg->next_offset.compare_exchange_weak(start,
                                                   next,
                                                   std::memory_order_acq_rel,
                                                   std::memory_order_relaxed)) {
          return SegmentReservation{seg->fd, seg->file_id, start};
        }
      }

      std::scoped_lock lock(rotate_mu_);
      SegmentState* cur = current_.load(std::memory_order_acquire);
      if (cur != seg) {
        continue;
      }
      const uint64_t cur_start = cur->next_offset.load(std::memory_order_relaxed);
      if (fits_in_segment(cur_start, bytes)) {
        continue;
      }
      auto rot = rotate_segment_locked();
      if (!rot) {
        return std::unexpected(rot.error());
      }
    }
  }

  const std::vector<std::filesystem::path>& files() const { return files_; }

  Result<void> finalize() {
    std::scoped_lock lock(rotate_mu_);
    for (auto& seg : segments_) {
      if (seg->fd >= 0) {
        auto s = maybe_fsync(seg->fd);
        if (!s) {
          return std::unexpected(s.error());
        }
        ::close(seg->fd);
        seg->fd = -1;
      }
    }
    return {};
  }

  ~SegmentedFileWriter() {
    for (auto& seg : segments_) {
      if (seg->fd >= 0) {
        ::close(seg->fd);
        seg->fd = -1;
      }
    }
  }

 private:
  struct SegmentState {
    int fd{-1};
    size_t file_id{0};
    std::atomic<uint64_t> next_offset{0};
  };

  bool fits_in_segment(uint64_t start, size_t bytes) const {
    if (bytes > std::numeric_limits<uint64_t>::max() - start) {
      return false;
    }
    const uint64_t end = start + static_cast<uint64_t>(bytes);
    if (end <= static_cast<uint64_t>(segment_size_bytes_)) {
      return true;
    }
    return start == sizeof(SegmentHeader);
  }

  Result<void> rotate_segment() {
    std::scoped_lock lock(rotate_mu_);
    return rotate_segment_locked();
  }

  Result<void> rotate_segment_locked() {
    const auto path = dir_ / ("segment_" + std::to_string(segments_.size()) + ".dat");
    auto fdv = open_file_write(path, direct_io_);
    if (!fdv) {
      return std::unexpected(fdv.error());
    }
    const int fd = *fdv;

    SegmentHeader sh{};
    sh.flags = compressed_ ? 1u : 0u;
    auto wr = write_all_fd(fd, reinterpret_cast<const uint8_t*>(&sh), sizeof(sh));
    if (!wr) {
      ::close(fd);
      return std::unexpected(wr.error());
    }

    auto state = std::make_unique<SegmentState>();
    state->fd = fd;
    state->file_id = segments_.size();
    state->next_offset.store(sizeof(SegmentHeader), std::memory_order_relaxed);
    SegmentState* ptr = state.get();

    segments_.push_back(std::move(state));
    files_.push_back(path);
    current_.store(ptr, std::memory_order_release);
    return {};
  }

  std::filesystem::path dir_;
  size_t segment_size_bytes_{0};
  bool compressed_{false};
  bool direct_io_{false};

  std::mutex rotate_mu_;
  std::vector<std::unique_ptr<SegmentState>> segments_;
  std::vector<std::filesystem::path> files_;
  std::atomic<SegmentState*> current_{nullptr};
};

Result<Dataset> create_disk_dataset(const Config& cfg,
                                    Codec& codec,
                                    bool compressed,
                                    const std::string& name,
                                    size_t chunk_count) {
  auto valid = validate_io_mode(cfg, compressed);
  if (!valid) {
    return std::unexpected(valid.error());
  }

  Dataset ds{};
  ds.compressed = compressed;
  ds.dir = cfg.output_dir / name;
  {
    std::error_code ec;
    std::filesystem::remove_all(ds.dir, ec);
  }
  auto mk = ensure_dir(ds.dir);
  if (!mk) {
    return std::unexpected(mk.error());
  }

  LowEntropyGenerator generator(cfg.seed);
  auto cal = calibrate_generator(generator, codec, cfg, compressed);
  if (!cal) {
    return std::unexpected(cal.error());
  }

  SegmentedFileWriter writer(ds.dir, cfg.segment_size_bytes, compressed, cfg.direct_io);
  auto open = writer.open_first();
  if (!open) {
    return std::unexpected(open.error());
  }

  ds.chunks.resize(chunk_count);

  std::atomic<size_t> next_index{0};
  std::atomic<uint64_t> total_uncompressed{0};
  std::atomic<uint64_t> total_stored{0};
  std::atomic<bool> failed{false};
  std::optional<AppError> error;
  std::mutex err_mu;

  auto set_error = [&](AppError e) {
    std::scoped_lock lock(err_mu);
    if (!error.has_value()) {
      error = std::move(e);
      failed.store(true, std::memory_order_release);
    }
  };

  std::vector<std::thread> workers;
  workers.reserve(cfg.threads);
  for (uint32_t w = 0; w < cfg.threads; ++w) {
    workers.emplace_back([&]() {
      LowEntropyGenerator worker_generator = generator;
      std::vector<uint8_t> raw;
      std::vector<uint8_t> comp;
      if (compressed) {
        comp.resize(codec.max_compressed_size(cfg.chunk_size));
      }

      while (!failed.load(std::memory_order_acquire)) {
        const size_t i = next_index.fetch_add(1, std::memory_order_relaxed);
        if (i >= chunk_count) {
          break;
        }

        worker_generator.generate(i, raw, cfg.chunk_size);
        const uint32_t chk = XXH32(raw.data(), raw.size(), 0);

        std::span<const uint8_t> payload;
        uint32_t comp_sz = 0;
        if (compressed) {
          auto c = codec.compress(raw, comp);
          if (!c) {
            set_error(c.error());
            break;
          }
          comp_sz = static_cast<uint32_t>(*c);
          payload = std::span<const uint8_t>(comp.data(), *c);
        } else {
          comp_sz = static_cast<uint32_t>(raw.size());
          payload = raw;
        }

        const size_t record_size = compressed ? (sizeof(ChunkFrameHeader) + payload.size()) : payload.size();
        auto reservation = writer.reserve(record_size);
        if (!reservation) {
          set_error(reservation.error());
          break;
        }

        ChunkLoc loc{};
        loc.file_id = reservation->file_id;
        loc.raw_size = static_cast<uint32_t>(raw.size());
        loc.stored_size = comp_sz;
        loc.checksum = chk;
        loc.algo_id = static_cast<uint8_t>(compressed ? codec.id() : Algo::None);

        if (compressed) {
          ChunkFrameHeader hdr{};
          hdr.raw_size = static_cast<uint32_t>(raw.size());
          hdr.comp_size = comp_sz;
          hdr.checksum = chk;
          hdr.algo_id = static_cast<uint8_t>(codec.id());

          auto wrh = pwrite_all_fd(reservation->fd,
                                   reinterpret_cast<const uint8_t*>(&hdr),
                                   sizeof(hdr),
                                   static_cast<off_t>(reservation->offset));
          if (!wrh) {
            set_error(wrh.error());
            break;
          }

          loc.offset = reservation->offset + sizeof(ChunkFrameHeader);
          auto wrp = pwrite_all_fd(reservation->fd,
                                   payload.data(),
                                   payload.size(),
                                   static_cast<off_t>(loc.offset));
          if (!wrp) {
            set_error(wrp.error());
            break;
          }

          total_stored.fetch_add(sizeof(hdr) + payload.size(), std::memory_order_relaxed);
        } else {
          loc.offset = reservation->offset;
          auto wrp = pwrite_all_fd(reservation->fd,
                                   payload.data(),
                                   payload.size(),
                                   static_cast<off_t>(loc.offset));
          if (!wrp) {
            set_error(wrp.error());
            break;
          }

          total_stored.fetch_add(payload.size(), std::memory_order_relaxed);
        }

        ds.chunks[i] = loc;
        total_uncompressed.fetch_add(raw.size(), std::memory_order_relaxed);
      }
    });
  }

  for (auto& t : workers) {
    t.join();
  }

  auto close = writer.finalize();
  if (!close) {
    close_dataset(ds);
    return std::unexpected(close.error());
  }

  if (error.has_value()) {
    close_dataset(ds);
    return std::unexpected(*error);
  }

  ds.files = writer.files();
  ds.total_uncompressed = total_uncompressed.load(std::memory_order_relaxed);
  ds.total_stored = total_stored.load(std::memory_order_relaxed);

  for (const auto& file : ds.files) {
    auto rfd = open_file_read(file, cfg.direct_io && !compressed);
    if (!rfd) {
      close_dataset(ds);
      return std::unexpected(rfd.error());
    }
    ds.fds.push_back(*rfd);
  }

  return ds;
}

Result<PhaseResult> run_disk_write_phase(const Config& cfg,
                                         Codec& codec,
                                         bool compressed,
                                         const std::string& run_name,
                                         Sec duration) {
  auto valid = validate_io_mode(cfg, compressed);
  if (!valid) {
    return std::unexpected(valid.error());
  }

  const auto out_dir = cfg.output_dir / run_name;
  {
    std::error_code ec;
    std::filesystem::remove_all(out_dir, ec);
  }
  auto mk = ensure_dir(out_dir);
  if (!mk) {
    return std::unexpected(mk.error());
  }

  SegmentedFileWriter writer(out_dir, cfg.segment_size_bytes, compressed, cfg.direct_io && !compressed);
  auto open = writer.open_first();
  if (!open) {
    return std::unexpected(open.error());
  }

  BoundedMPMCQueue<Job> jobs(cfg.queue_depth);

  std::atomic<uint64_t> issued_chunks{0};
  std::atomic<uint64_t> total_stored_bytes{0};

  HashSink sink(cfg.seed);
  LowEntropyGenerator generator(cfg.seed);
  auto cal = calibrate_generator(generator, codec, cfg, compressed);
  if (!cal) {
    return std::unexpected(cal.error());
  }

  std::atomic<bool> schedule_finished{false};
  std::atomic<bool> failed{false};
  std::optional<AppError> error;
  std::mutex err_mu;

  auto set_error = [&](AppError e) {
    std::scoped_lock lock(err_mu);
    if (!error.has_value()) {
      error = std::move(e);
      failed.store(true, std::memory_order_release);
    }
  };

  std::vector<std::thread> workers;
  workers.reserve(cfg.threads);
  for (uint32_t w = 0; w < cfg.threads; ++w) {
    workers.emplace_back([&]() {
      LowEntropyGenerator worker_generator = generator;
      std::vector<uint8_t> raw;
      std::vector<uint8_t> comp;
      if (compressed) {
        comp.resize(codec.max_compressed_size(cfg.chunk_size));
      }

      for (;;) {
        Job job;
        if (!jobs.try_pop(job)) {
          if (schedule_finished.load(std::memory_order_relaxed)) {
            std::this_thread::yield();
          } else {
            std::this_thread::yield();
          }
          continue;
        }
        if (job.stop) {
          break;
        }
        if (failed.load(std::memory_order_acquire)) {
          break;
        }

        uint32_t raw_size = 0;
        uint32_t comp_size = 0;
        uint32_t checksum = 0;
        uint8_t algo_id = static_cast<uint8_t>(compressed ? codec.id() : Algo::None);
        std::span<const uint8_t> payload;

        if (compressed) {
          worker_generator.generate(job.seq, raw, cfg.chunk_size);
          sink.consume(raw);

          raw_size = static_cast<uint32_t>(raw.size());
          checksum = XXH32(raw.data(), raw.size(), 0);
          auto c = codec.compress(raw, comp);
          if (!c) {
            set_error(c.error());
            break;
          }
          comp_size = static_cast<uint32_t>(*c);
          payload = std::span<const uint8_t>(comp.data(), *c);
        } else {
          worker_generator.generate(job.seq, raw, cfg.chunk_size);
          sink.consume(raw);

          raw_size = static_cast<uint32_t>(raw.size());
          checksum = XXH32(raw.data(), raw.size(), 0);
          comp_size = raw_size;
          payload = raw;
        }

        const size_t record_size = compressed ? (sizeof(ChunkFrameHeader) + payload.size()) : payload.size();
        auto reservation = writer.reserve(record_size);
        if (!reservation) {
          set_error(reservation.error());
          break;
        }

        if (compressed) {
          ChunkFrameHeader h{};
          h.raw_size = raw_size;
          h.comp_size = comp_size;
          h.checksum = checksum;
          h.algo_id = algo_id;

          auto wrh = pwrite_all_fd(reservation->fd,
                                   reinterpret_cast<const uint8_t*>(&h),
                                   sizeof(h),
                                   static_cast<off_t>(reservation->offset));
          if (!wrh) {
            set_error(wrh.error());
            break;
          }

          auto wrp = pwrite_all_fd(reservation->fd,
                                   payload.data(),
                                   payload.size(),
                                   static_cast<off_t>(reservation->offset + sizeof(ChunkFrameHeader)));
          if (!wrp) {
            set_error(wrp.error());
            break;
          }

          total_stored_bytes.fetch_add(sizeof(h) + payload.size(), std::memory_order_relaxed);
        } else {
          auto wrp = pwrite_all_fd(reservation->fd,
                                   payload.data(),
                                   payload.size(),
                                   static_cast<off_t>(reservation->offset));
          if (!wrp) {
            set_error(wrp.error());
            break;
          }

          total_stored_bytes.fetch_add(payload.size(), std::memory_order_relaxed);
        }
      }
    });
  }

  const auto start = Clock::now();
  std::thread scheduler([&]() {
    const auto cutoff = start + duration;
    uint64_t seq = 0;
    while (Clock::now() < cutoff) {
      Job j{};
      j.seq = seq++;
      while (!jobs.try_push(std::move(j))) {
        if (failed.load(std::memory_order_acquire)) {
          break;
        }
        std::this_thread::yield();
      }
      if (failed.load(std::memory_order_acquire)) {
        break;
      }
    }
    issued_chunks.store(seq, std::memory_order_relaxed);
    for (uint32_t i = 0; i < cfg.threads; ++i) {
      Job stop{};
      stop.stop = true;
      while (!jobs.try_push(std::move(stop))) {
        std::this_thread::yield();
      }
    }
    schedule_finished.store(true, std::memory_order_relaxed);
  });

  scheduler.join();
  for (auto& t : workers) {
    t.join();
  }

  const auto stop = Clock::now();

  auto close = writer.finalize();
  if (!close) {
    return std::unexpected(close.error());
  }

  if (error.has_value()) {
    return std::unexpected(*error);
  }

  const uint64_t uncompressed = issued_chunks.load(std::memory_order_relaxed) * cfg.chunk_size;
  const uint64_t compressed_bytes = total_stored_bytes.load(std::memory_order_relaxed);

  PhaseResult pr{};
  pr.mode = compressed ? Mode::CompWrite : Mode::RawWrite;
  pr.wall_sec = std::chrono::duration<double>(stop - start).count();
  pr.uncompressed_bytes = uncompressed;
  pr.compressed_bytes = compressed ? compressed_bytes : uncompressed;
  pr.sink_hash = sink.digest();
  return pr;
}

Result<PhaseResult> run_disk_read_phase(const Config& cfg,
                                        Codec& codec,
                                        bool compressed,
                                        const std::string& run_name,
                                        Sec duration) {
  const size_t prep_chunks = std::max<size_t>(1, cfg.segment_size_bytes / cfg.chunk_size);
  auto ds = create_disk_dataset(cfg, codec, compressed, run_name + "_dataset", prep_chunks);
  if (!ds) {
    return std::unexpected(ds.error());
  }

  BoundedMPMCQueue<Job> jobs(cfg.queue_depth);
  std::atomic<uint32_t> workers_done{0};
  std::atomic<uint64_t> issued{0};
  std::atomic<bool> schedule_done{false};
  std::atomic<uint64_t> consumed_stored{0};

  HashSink sink(cfg.seed);
  std::optional<AppError> error;
  std::mutex err_mu;

  auto set_error = [&](AppError e) {
    std::scoped_lock lock(err_mu);
    if (!error.has_value()) {
      error = std::move(e);
    }
  };

  std::vector<std::thread> workers;
  workers.reserve(cfg.threads);

  for (uint32_t w = 0; w < cfg.threads; ++w) {
    workers.emplace_back([&]() {
      std::vector<uint8_t> payload;
      std::vector<uint8_t> raw;
      for (;;) {
        Job job;
        if (!jobs.try_pop(job)) {
          if (schedule_done.load(std::memory_order_relaxed)) {
            std::this_thread::yield();
          } else {
            std::this_thread::yield();
          }
          continue;
        }
        if (job.stop) {
          break;
        }

        const auto& loc = ds->chunks[job.index % ds->chunks.size()];
        payload.resize(loc.stored_size);
        auto rd = pread_all_fd(ds->fds[loc.file_id], payload.data(), payload.size(), static_cast<off_t>(loc.offset));
        if (!rd) {
          set_error(rd.error());
          break;
        }

        if (compressed) {
          raw.resize(loc.raw_size);
          auto dec = codec.decompress(payload, raw, loc.raw_size);
          if (!dec) {
            set_error(dec.error());
            break;
          }
          if (XXH32(raw.data(), raw.size(), 0) != loc.checksum) {
            set_error(AppError{"checksum mismatch in decompressed chunk"});
            break;
          }
          sink.consume(raw);
        } else {
          if (XXH32(payload.data(), payload.size(), 0) != loc.checksum) {
            set_error(AppError{"checksum mismatch in raw chunk"});
            break;
          }
          sink.consume(payload);
        }

        consumed_stored.fetch_add(loc.stored_size, std::memory_order_relaxed);
      }

      workers_done.fetch_add(1, std::memory_order_relaxed);
    });
  }

  const auto start = Clock::now();
  std::thread scheduler([&]() {
    const auto cutoff = start + duration;
    uint64_t seq = 0;
    while (Clock::now() < cutoff) {
      Job j{};
      j.seq = seq;
      j.index = seq % ds->chunks.size();
      ++seq;
      while (!jobs.try_push(std::move(j))) {
        if (error.has_value()) {
          break;
        }
        std::this_thread::yield();
      }
      if (error.has_value()) {
        break;
      }
    }
    issued.store(seq, std::memory_order_relaxed);
    for (uint32_t i = 0; i < cfg.threads; ++i) {
      Job stop{};
      stop.stop = true;
      while (!jobs.try_push(std::move(stop))) {
        std::this_thread::yield();
      }
    }
    schedule_done.store(true, std::memory_order_relaxed);
  });

  scheduler.join();
  for (auto& t : workers) {
    t.join();
  }

  const auto stop = Clock::now();

  if (error.has_value()) {
    close_dataset(*ds);
    return std::unexpected(*error);
  }

  const uint64_t unc = sink.bytes();
  const uint64_t comp_bytes = compressed ? consumed_stored.load(std::memory_order_relaxed) : unc;

  close_dataset(*ds);

  std::thread cleanup([dir = ds->dir]() {
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
  });
  cleanup.detach();

  PhaseResult pr{};
  pr.mode = compressed ? Mode::DecompRead : Mode::RawRead;
  pr.wall_sec = std::chrono::duration<double>(stop - start).count();
  pr.uncompressed_bytes = unc;
  pr.compressed_bytes = comp_bytes;
  pr.sink_hash = sink.digest();
  return pr;
}

bool send_all(int fd, const void* p, size_t n) {
  size_t done = 0;
  auto* data = static_cast<const uint8_t*>(p);
  while (done < n) {
    const ssize_t w = ::send(fd, data + done, n - done, 0);
    if (w < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    done += static_cast<size_t>(w);
  }
  return true;
}

bool recv_all(int fd, void* p, size_t n) {
  size_t done = 0;
  auto* data = static_cast<uint8_t*>(p);
  while (done < n) {
    const ssize_t r = ::recv(fd, data + done, n - done, 0);
    if (r < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (r == 0) {
      return false;
    }
    done += static_cast<size_t>(r);
  }
  return true;
}

struct TcpAck {
  uint64_t uncompressed{0};
  uint64_t compressed{0};
  uint64_t hash{0};
  uint64_t wall_ns{0};
};

Result<int> connect_loopback(uint16_t port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return std::unexpected(AppError{"socket create failed"});
  }
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

  for (int i = 0; i < 50; ++i) {
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
      return fd;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ::close(fd);
  return std::unexpected(AppError{"connect loopback failed"});
}

Result<PhaseResult> run_tcp_sender_phase(const Config& cfg,
                                         Codec& codec,
                                         bool compressed,
                                         Sec duration) {
  auto cfd = connect_loopback(cfg.port);
  if (!cfd) {
    return std::unexpected(cfd.error());
  }
  const int sock = *cfd;

  BoundedMPMCQueue<Job> jobs(cfg.queue_depth);
  BoundedMPMCQueue<ProducedChunk> outbound(cfg.queue_depth);
  std::atomic<uint64_t> issued{0};
  std::atomic<uint32_t> workers_done{0};
  std::atomic<bool> schedule_done{false};
  std::atomic<uint64_t> total_comp_sent{0};
  std::optional<AppError> error;
  std::mutex err_mu;

  auto set_error = [&](AppError e) {
    std::scoped_lock lock(err_mu);
    if (!error.has_value()) {
      error = std::move(e);
    }
  };

  LowEntropyGenerator generator(cfg.seed);
  auto cal = calibrate_generator(generator, codec, cfg, compressed);
  if (!cal) {
    ::close(sock);
    return std::unexpected(cal.error());
  }

  std::thread sender([&]() {
    std::unordered_map<uint64_t, ProducedChunk> pending;
    pending.reserve(static_cast<size_t>(cfg.queue_depth) * 2);
    uint64_t next = 0;

    while (true) {
      ProducedChunk item;
      if (outbound.try_pop(item)) {
        pending.emplace(item.seq, std::move(item));
      } else {
        if (schedule_done.load(std::memory_order_relaxed) &&
            workers_done.load(std::memory_order_relaxed) == cfg.threads && pending.empty() &&
            next == issued.load(std::memory_order_relaxed)) {
          break;
        }
        std::this_thread::yield();
      }

      auto it = pending.find(next);
      while (it != pending.end()) {
        ChunkFrameHeader h{};
        h.raw_size = it->second.raw_size;
        h.comp_size = it->second.comp_size;
        h.checksum = it->second.checksum;
        h.algo_id = it->second.algo_id;
        if (!send_all(sock, &h, sizeof(h))) {
          set_error(AppError{"send header failed"});
          return;
        }
        if (!send_all(sock, it->second.payload.data(), it->second.payload.size())) {
          set_error(AppError{"send payload failed"});
          return;
        }
        total_comp_sent.fetch_add(sizeof(h) + it->second.payload.size(), std::memory_order_relaxed);
        pending.erase(it);
        ++next;
        it = pending.find(next);
      }
    }

    ChunkFrameHeader end{};
    if (!send_all(sock, &end, sizeof(end))) {
      set_error(AppError{"send end marker failed"});
    }
  });

  std::vector<std::thread> workers;
  workers.reserve(cfg.threads);
  for (uint32_t w = 0; w < cfg.threads; ++w) {
    workers.emplace_back([&]() {
      std::vector<uint8_t> raw;
      std::vector<uint8_t> comp;
      if (compressed) {
        comp.resize(codec.max_compressed_size(cfg.chunk_size));
      }
      for (;;) {
        Job job;
        if (!jobs.try_pop(job)) {
          if (schedule_done.load(std::memory_order_relaxed)) {
            std::this_thread::yield();
          } else {
            std::this_thread::yield();
          }
          continue;
        }
        if (job.stop) {
          break;
        }

        ProducedChunk pc{};
        pc.seq = job.seq;
        pc.algo_id = static_cast<uint8_t>(compressed ? codec.id() : Algo::None);
        if (compressed) {
          generator.generate(job.seq, raw, cfg.chunk_size);

          pc.raw_size = static_cast<uint32_t>(raw.size());
          pc.checksum = XXH32(raw.data(), raw.size(), 0);
          auto c = codec.compress(raw, comp);
          if (!c) {
            set_error(c.error());
            break;
          }
          pc.comp_size = static_cast<uint32_t>(*c);
          pc.payload.assign(comp.begin(), comp.begin() + static_cast<std::ptrdiff_t>(*c));
        } else {
          generator.generate(job.seq, pc.payload, cfg.chunk_size);

          pc.raw_size = static_cast<uint32_t>(pc.payload.size());
          pc.checksum = XXH32(pc.payload.data(), pc.payload.size(), 0);
          pc.comp_size = pc.raw_size;
        }
        while (!outbound.try_push(std::move(pc))) {
          if (error.has_value()) {
            break;
          }
          std::this_thread::yield();
        }
        if (error.has_value()) {
          break;
        }
      }
      workers_done.fetch_add(1, std::memory_order_relaxed);
    });
  }

  const auto start = Clock::now();
  std::thread scheduler([&]() {
    const auto cutoff = start + duration;
    uint64_t seq = 0;
    while (Clock::now() < cutoff) {
      Job j{};
      j.seq = seq++;
      while (!jobs.try_push(std::move(j))) {
        if (error.has_value()) {
          break;
        }
        std::this_thread::yield();
      }
      if (error.has_value()) {
        break;
      }
    }
    issued.store(seq, std::memory_order_relaxed);
    for (uint32_t i = 0; i < cfg.threads; ++i) {
      Job s{};
      s.stop = true;
      while (!jobs.try_push(std::move(s))) {
        std::this_thread::yield();
      }
    }
    schedule_done.store(true, std::memory_order_relaxed);
  });

  scheduler.join();
  for (auto& t : workers) {
    t.join();
  }
  sender.join();

  TcpAck ack{};
  if (!recv_all(sock, &ack, sizeof(ack))) {
    ::close(sock);
    return std::unexpected(AppError{"failed to receive receiver ack"});
  }
  const auto stop = Clock::now();
  ::close(sock);

  if (error.has_value()) {
    return std::unexpected(*error);
  }

  PhaseResult pr{};
  pr.mode = compressed ? Mode::CompWrite : Mode::RawWrite;
  pr.wall_sec = std::chrono::duration<double>(stop - start).count();
  pr.uncompressed_bytes = issued.load(std::memory_order_relaxed) * cfg.chunk_size;
  pr.compressed_bytes = total_comp_sent.load(std::memory_order_relaxed);
  pr.sink_hash = ack.hash;
  return pr;
}

Result<int> run_tcp_receiver(const Config& cfg, Codec& codec) {
  (void)codec;
  const int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (lfd < 0) {
    return std::unexpected(AppError{"receiver socket create failed"});
  }

  int yes = 1;
  setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(cfg.port);
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

  if (::bind(lfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    ::close(lfd);
    return std::unexpected(AppError{"receiver bind failed"});
  }
  if (::listen(lfd, 1) != 0) {
    ::close(lfd);
    return std::unexpected(AppError{"receiver listen failed"});
  }

  const int cfd = ::accept(lfd, nullptr, nullptr);
  ::close(lfd);
  if (cfd < 0) {
    return std::unexpected(AppError{"receiver accept failed"});
  }

  HashSink sink(cfg.seed);
  uint64_t unc = 0;
  uint64_t comp = 0;
  bool started = false;
  Clock::time_point start{};

  std::vector<uint8_t> payload;
  std::vector<uint8_t> raw;
  std::optional<Codec> recv_codec;
  Algo recv_algo = Algo::None;

  for (;;) {
    ChunkFrameHeader h{};
    if (!recv_all(cfd, &h, sizeof(h))) {
      ::close(cfd);
      return std::unexpected(AppError{"receiver header read failed"});
    }
    if (h.raw_size == 0 && h.comp_size == 0) {
      break;
    }
    if (!started) {
      started = true;
      start = Clock::now();
    }
    payload.resize(h.comp_size);
    if (!recv_all(cfd, payload.data(), payload.size())) {
      ::close(cfd);
      return std::unexpected(AppError{"receiver payload read failed"});
    }
    comp += sizeof(h) + payload.size();

    const auto algo = static_cast<Algo>(h.algo_id);
    if (algo == Algo::None) {
      if (XXH32(payload.data(), payload.size(), 0) != h.checksum) {
        ::close(cfd);
        return std::unexpected(AppError{"receiver checksum mismatch (raw)"});
      }
      sink.consume(payload);
      unc += payload.size();
    } else {
      if (!recv_codec.has_value() || recv_algo != algo) {
        recv_codec.emplace(algo, cfg.zstd_level);
        recv_algo = algo;
      }
      raw.resize(h.raw_size);
      auto dec = recv_codec->decompress(payload, raw, h.raw_size);
      if (!dec) {
        ::close(cfd);
        return std::unexpected(dec.error());
      }
      if (XXH32(raw.data(), raw.size(), 0) != h.checksum) {
        ::close(cfd);
        return std::unexpected(AppError{"receiver checksum mismatch (decomp)"});
      }
      sink.consume(raw);
      unc += raw.size();
    }
  }

  const auto stop = Clock::now();
  TcpAck ack{};
  ack.uncompressed = unc;
  ack.compressed = comp;
  ack.hash = sink.digest();
  ack.wall_ns = started ? std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count() : 0;

  if (!send_all(cfd, &ack, sizeof(ack))) {
    ::close(cfd);
    return std::unexpected(AppError{"receiver ack send failed"});
  }

  ::close(cfd);
  return 0;
}

Result<pid_t> spawn_receiver_process(const Config& cfg, Algo algo) {
  pid_t pid = ::fork();
  if (pid < 0) {
    return std::unexpected(AppError{"fork failed"});
  }
  if (pid == 0) {
    std::vector<std::string> argv_str{
        cfg.executable_path,
        "--medium",
        "tcp",
        "--role",
        "receiver",
        "--port",
        std::to_string(cfg.port),
        "--algo",
        algo_to_string(algo),
        "--zstd-level",
        std::to_string(cfg.zstd_level),
        "--seed",
        std::to_string(cfg.seed),
    };

    std::vector<char*> argv;
    argv.reserve(argv_str.size() + 1);
    for (auto& s : argv_str) {
      argv.push_back(s.data());
    }
    argv.push_back(nullptr);
    ::execv(cfg.executable_path.c_str(), argv.data());
    std::cerr << "execv receiver failed: " << std::strerror(errno) << "\n";
    std::_Exit(127);
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  return pid;
}

Result<PhaseResult> run_tcp_phase(const Config& cfg, Mode mode, Sec duration) {
  const bool compressed = (mode == Mode::CompWrite || mode == Mode::DecompRead);
  const Algo algo = compressed ? cfg.algo : Algo::None;

  Codec codec(algo, cfg.zstd_level);

  if (cfg.role == Role::Receiver) {
    return std::unexpected(AppError{"receiver role does not run benchmark orchestration"});
  }

  std::optional<pid_t> child;
  if (cfg.role == Role::Auto) {
    auto sp = spawn_receiver_process(cfg, algo);
    if (!sp) {
      return std::unexpected(sp.error());
    }
    child = *sp;
  }

  auto send = run_tcp_sender_phase(cfg, codec, compressed, duration);

  if (child.has_value()) {
    int st = 0;
    ::waitpid(*child, &st, 0);
  }

  if (!send) {
    return std::unexpected(send.error());
  }

  auto result = *send;
  if (mode == Mode::RawRead) {
    result.mode = Mode::RawRead;
  } else if (mode == Mode::DecompRead) {
    result.mode = Mode::DecompRead;
  }
  return result;
}

Result<PhaseResult> run_phase(const Config& cfg, Mode mode, Sec duration, uint32_t repeat_id, bool warmup) {
  const std::string prefix = warmup ? "warmup" : "timed";
  const std::string name = prefix + "_" + mode_to_string(mode) + "_r" + std::to_string(repeat_id);

  if (cfg.medium == Medium::Disk) {
    Codec codec((mode == Mode::CompWrite || mode == Mode::DecompRead) ? cfg.algo : Algo::None, cfg.zstd_level);
    if (mode == Mode::RawWrite) {
      return run_disk_write_phase(cfg, codec, false, name, duration);
    }
    if (mode == Mode::CompWrite) {
      return run_disk_write_phase(cfg, codec, true, name, duration);
    }
    if (mode == Mode::RawRead) {
      return run_disk_read_phase(cfg, codec, false, name, duration);
    }
    if (mode == Mode::DecompRead) {
      return run_disk_read_phase(cfg, codec, true, name, duration);
    }
  }

  return run_tcp_phase(cfg, mode, duration);
}

std::vector<Mode> select_modes(const Config& cfg) {
  if (cfg.mode == Mode::SuiteAll) {
    return {Mode::RawWrite, Mode::CompWrite, Mode::RawRead, Mode::DecompRead};
  }
  return {cfg.mode};
}

void print_phase(const PhaseResult& r) {
  const double eff = to_gbps(r.uncompressed_bytes, r.wall_sec);
  const double cr = r.compressed_bytes == 0
                        ? 0.0
                        : static_cast<double>(r.uncompressed_bytes) / static_cast<double>(r.compressed_bytes);
  std::cout << "phase=" << mode_to_string(r.mode) << " wall_sec=" << std::fixed << std::setprecision(3)
            << r.wall_sec << " eff_GBps=" << eff << " CR=" << cr << " hash=0x" << std::hex << r.sink_hash
            << std::dec << "\n";
}

Stats stats_for_mode(const Summary& s, Mode mode) {
  auto it = s.phases.find(mode);
  if (it == s.phases.end()) {
    return {};
  }
  std::vector<double> eff;
  eff.reserve(it->second.size());
  for (const auto& r : it->second) {
    eff.push_back(to_gbps(r.uncompressed_bytes, r.wall_sec));
  }
  return calc_stats(std::move(eff));
}

Stats stats_ratio(const Summary& s, Mode num_mode, Mode den_mode) {
  const auto itn = s.phases.find(num_mode);
  const auto itd = s.phases.find(den_mode);
  if (itn == s.phases.end() || itd == s.phases.end()) {
    return {};
  }
  const size_t n = std::min(itn->second.size(), itd->second.size());
  std::vector<double> ratios;
  ratios.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    const double num = to_gbps(itn->second[i].uncompressed_bytes, itn->second[i].wall_sec);
    const double den = to_gbps(itd->second[i].uncompressed_bytes, itd->second[i].wall_sec);
    ratios.push_back(den > 0.0 ? num / den : 0.0);
  }
  return calc_stats(std::move(ratios));
}

Stats stats_cr(const Summary& s, Mode mode) {
  auto it = s.phases.find(mode);
  if (it == s.phases.end()) {
    return {};
  }
  std::vector<double> cr;
  cr.reserve(it->second.size());
  for (const auto& r : it->second) {
    if (r.compressed_bytes > 0) {
      cr.push_back(static_cast<double>(r.uncompressed_bytes) / static_cast<double>(r.compressed_bytes));
    }
  }
  return calc_stats(std::move(cr));
}

std::string stats_json(const Stats& s) {
  std::ostringstream os;
  os << std::fixed << std::setprecision(6) << "{\"mean\":" << s.mean << ",\"median\":" << s.median
     << ",\"p95\":" << s.p95 << ",\"min\":" << s.min << ",\"max\":" << s.max << "}";
  return os.str();
}

Result<void> write_json_summary(const Config& cfg, const Summary& summary) {
  if (!cfg.json_output.has_value()) {
    return {};
  }
  std::ofstream out(*cfg.json_output, std::ios::trunc);
  if (!out.is_open()) {
    return std::unexpected(AppError{"failed to open JSON output path"});
  }

  const auto w_raw = stats_for_mode(summary, Mode::RawWrite);
  const auto w_cmp = stats_for_mode(summary, Mode::CompWrite);
  const auto r_raw = stats_for_mode(summary, Mode::RawRead);
  const auto r_dec = stats_for_mode(summary, Mode::DecompRead);
  const auto m_write = stats_ratio(summary, Mode::CompWrite, Mode::RawWrite);
  const auto m_read = stats_ratio(summary, Mode::DecompRead, Mode::RawRead);
  const auto cr_cmp = stats_cr(summary, Mode::CompWrite);
  const auto cr_dec = stats_cr(summary, Mode::DecompRead);

  out << "{\n";
  out << "  \"config\": {\"medium\": \"" << medium_to_string(cfg.medium) << "\", \"mode\": \""
      << mode_to_string(cfg.mode) << "\", \"algo\": \"" << algo_to_string(cfg.algo) << "\", "
      << "\"duration_sec\": " << cfg.duration_sec << ", \"repeats\": " << cfg.repeats
      << ", \"warmup_sec\": " << cfg.warmup_sec << "},\n";
  out << "  \"metrics\": {\n";
  out << "    \"W_eff_raw_GBps\": " << stats_json(w_raw) << ",\n";
  out << "    \"W_eff_cmp_GBps\": " << stats_json(w_cmp) << ",\n";
  out << "    \"R_eff_raw_GBps\": " << stats_json(r_raw) << ",\n";
  out << "    \"R_eff_dec_GBps\": " << stats_json(r_dec) << ",\n";
  out << "    \"M_write\": " << stats_json(m_write) << ",\n";
  out << "    \"M_read\": " << stats_json(m_read) << ",\n";
  out << "    \"CR_comp_write\": " << stats_json(cr_cmp) << ",\n";
  out << "    \"CR_decomp_read\": " << stats_json(cr_dec) << "\n";
  out << "  }\n";
  out << "}\n";

  return {};
}

void print_human_summary(const Summary& summary) {
  const auto w_raw = stats_for_mode(summary, Mode::RawWrite);
  const auto w_cmp = stats_for_mode(summary, Mode::CompWrite);
  const auto r_raw = stats_for_mode(summary, Mode::RawRead);
  const auto r_dec = stats_for_mode(summary, Mode::DecompRead);
  const auto m_write = stats_ratio(summary, Mode::CompWrite, Mode::RawWrite);
  const auto m_read = stats_ratio(summary, Mode::DecompRead, Mode::RawRead);
  const auto cr_cmp = stats_cr(summary, Mode::CompWrite);
  const auto cr_dec = stats_cr(summary, Mode::DecompRead);

  std::cout << "\n=== Summary (mean/median/p95/min/max) ===\n";
  std::cout << "W_eff_raw_GBps: " << w_raw.mean << " / " << w_raw.median << " / " << w_raw.p95 << " / "
            << w_raw.min << " / " << w_raw.max << "\n";
  std::cout << "W_eff_cmp_GBps: " << w_cmp.mean << " / " << w_cmp.median << " / " << w_cmp.p95 << " / "
            << w_cmp.min << " / " << w_cmp.max << "\n";
  std::cout << "R_eff_raw_GBps: " << r_raw.mean << " / " << r_raw.median << " / " << r_raw.p95 << " / "
            << r_raw.min << " / " << r_raw.max << "\n";
  std::cout << "R_eff_dec_GBps: " << r_dec.mean << " / " << r_dec.median << " / " << r_dec.p95 << " / "
            << r_dec.min << " / " << r_dec.max << "\n";
  std::cout << "M_write: " << m_write.mean << " / " << m_write.median << " / " << m_write.p95 << " / "
            << m_write.min << " / " << m_write.max << "\n";
  std::cout << "M_read: " << m_read.mean << " / " << m_read.median << " / " << m_read.p95 << " / "
            << m_read.min << " / " << m_read.max << "\n";
  std::cout << "CR_observed(comp_write): " << cr_cmp.mean << " / " << cr_cmp.median << " / " << cr_cmp.p95
            << " / " << cr_cmp.min << " / " << cr_cmp.max << "\n";
  std::cout << "CR_observed(decomp_read): " << cr_dec.mean << " / " << cr_dec.median << " / " << cr_dec.p95
            << " / " << cr_dec.min << " / " << cr_dec.max << "\n";
}

Result<Medium> parse_medium(const std::string& s) {
  if (s == "disk") {
    return Medium::Disk;
  }
  if (s == "tcp") {
    return Medium::Tcp;
  }
  return std::unexpected(AppError{"invalid --medium"});
}

Result<Mode> parse_mode(const std::string& s) {
  if (s == "raw_write") {
    return Mode::RawWrite;
  }
  if (s == "comp_write") {
    return Mode::CompWrite;
  }
  if (s == "raw_read") {
    return Mode::RawRead;
  }
  if (s == "decomp_read") {
    return Mode::DecompRead;
  }
  if (s == "all") {
    return Mode::SuiteAll;
  }
  return std::unexpected(AppError{"invalid --mode/suite"});
}

Result<Role> parse_role(const std::string& s) {
  if (s == "auto") {
    return Role::Auto;
  }
  if (s == "sender") {
    return Role::Sender;
  }
  if (s == "receiver") {
    return Role::Receiver;
  }
  return std::unexpected(AppError{"invalid --role"});
}

Result<Algo> parse_algo(const std::string& s) {
  if (s == "none") {
    return Algo::None;
  }
  if (s == "lz4") {
    return Algo::Lz4;
  }
  if (s == "zstd") {
    return Algo::Zstd;
  }
  return std::unexpected(AppError{"invalid --algo"});
}

bool has_help_flag(int argc, char** argv) {
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      return true;
    }
  }
  return false;
}

void print_cli_help(const std::string& exe_path) {
  const std::string exe = exe_path.empty() ? "bwm" : exe_path;
  std::cout << "Usage:\n"
            << "  " << exe << " [options]\n\n"
            << "Modes and media:\n"
            << "  --medium <disk|tcp>             Benchmark medium (default: disk)\n"
            << "  --mode <raw_write|comp_write|raw_read|decomp_read|all>\n"
            << "                                  Phase(s) to run (default: all)\n"
            << "  --role <auto|sender|receiver>   TCP role (default: auto)\n"
            << "  --algo <none|lz4|zstd>          Compression algorithm (default: lz4)\n"
            << "  --zstd-level <int>              Zstd level when algo=zstd (default: 1)\n\n"
            << "Workload and runtime:\n"
            << "  --duration-sec <u32>            Timed phase duration in seconds (default: 10)\n"
            << "  --warmup-sec <u32>              Warmup duration before each repeat (default: 0)\n"
            << "  --repeats <u32>                 Number of repeats per mode (default: 5)\n"
            << "  --threads <u32>                 Worker thread count (default: HW concurrency)\n"
            << "  --chunk-size <bytes>            Chunk payload size (default: 1048576)\n"
            << "  --queue-depth <u32>             Bounded queue depth, rounded to pow2 (default: 1024)\n"
            << "  --segment-size-gib <u32>        Segment target size in GiB (default: 1)\n\n"
            << "Data and output:\n"
            << "  --seed <u64>                    Deterministic data seed (default: 1)\n"
            << "  --target-cr <double>            Optional target compression ratio hint\n"
            << "  --cr-tol <double>               Compression ratio tolerance (default: 0.20)\n"
            << "  --output-dir <path>             Output directory (default: ./bwm_out)\n"
            << "  --json <path>                   Write JSON summary to file\n\n"
            << "Platform and TCP:\n"
            << "  --port <u16>                    TCP loopback port (default: 9191)\n"
            << "  --direct-io                     Request direct I/O where supported\n\n"
            << "Help:\n"
            << "  -h, --help                      Show this help and exit\n\n"
            << "Examples:\n"
            << "  " << exe << " --medium disk --mode all --algo lz4 --duration-sec 5 --repeats 3\n"
            << "  " << exe << " --medium tcp --mode comp_write --algo zstd --zstd-level 3 --port 9321\n"
            << "  " << exe << " --medium disk --mode all --json ./result.json\n";
}

Result<Config> parse_args(int argc, char** argv) {
  Config cfg{};
  if (argc > 0) {
    cfg.executable_path = argv[0];
  }

#if BWM_HAS_ARGPARSE
  argparse::ArgumentParser program("bwm");
  program.add_argument("--medium").default_value(std::string("disk"));
  program.add_argument("--mode").default_value(std::string("all"));
  program.add_argument("--role").default_value(std::string("auto"));
  program.add_argument("--algo").default_value(std::string("lz4"));
  program.add_argument("--zstd-level").scan<'i', int>().default_value(1);
  program.add_argument("--duration-sec").scan<'u', uint32_t>().default_value(10);
  program.add_argument("--warmup-sec").scan<'u', uint32_t>().default_value(0);
  program.add_argument("--repeats").scan<'u', uint32_t>().default_value(5);
  program.add_argument("--threads")
      .scan<'u', uint32_t>()
      .default_value(std::max(1u, std::thread::hardware_concurrency()));
  program.add_argument("--chunk-size")
      .scan<'u', size_t>()
      .default_value(static_cast<size_t>(1024ULL * 1024ULL));
  program.add_argument("--queue-depth").scan<'u', uint32_t>().default_value(1024);
  program.add_argument("--segment-size-gib").scan<'u', uint32_t>().default_value(1);
  program.add_argument("--output-dir").default_value(std::string("./bwm_out"));
  program.add_argument("--json").default_value(std::string(""));
  program.add_argument("--seed").scan<'u', uint64_t>().default_value(1ULL);
  program.add_argument("--target-cr").scan<'g', double>().default_value(0.0);
  program.add_argument("--cr-tol").scan<'g', double>().default_value(0.20);
  program.add_argument("--port").scan<'u', uint16_t>().default_value(static_cast<uint16_t>(9191));
  program.add_argument("--direct-io").default_value(false).implicit_value(true);

  try {
    program.parse_args(argc, argv);
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << "\n";
    std::cerr << program;
    return std::unexpected(AppError{"argument parsing failed"});
  }

  auto med = parse_medium(program.get<std::string>("--medium"));
  if (!med) {
    return std::unexpected(med.error());
  }
  cfg.medium = *med;

  auto mode = parse_mode(program.get<std::string>("--mode"));
  if (!mode) {
    return std::unexpected(mode.error());
  }
  cfg.mode = *mode;

  auto role = parse_role(program.get<std::string>("--role"));
  if (!role) {
    return std::unexpected(role.error());
  }
  cfg.role = *role;

  auto algo = parse_algo(program.get<std::string>("--algo"));
  if (!algo) {
    return std::unexpected(algo.error());
  }
  cfg.algo = *algo;

  cfg.zstd_level = program.get<int>("--zstd-level");
  cfg.duration_sec = program.get<uint32_t>("--duration-sec");
  cfg.warmup_sec = program.get<uint32_t>("--warmup-sec");
  cfg.repeats = program.get<uint32_t>("--repeats");
  cfg.threads = std::max(1u, program.get<uint32_t>("--threads"));
  cfg.chunk_size = program.get<size_t>("--chunk-size");
  cfg.queue_depth = std::max(16u, program.get<uint32_t>("--queue-depth"));
  cfg.segment_size_bytes =
      static_cast<size_t>(program.get<uint32_t>("--segment-size-gib")) * 1024ULL * 1024ULL * 1024ULL;
  cfg.output_dir = program.get<std::string>("--output-dir");
  const auto json = program.get<std::string>("--json");
  if (!json.empty()) {
    cfg.json_output = std::filesystem::path(json);
  }
  cfg.seed = program.get<uint64_t>("--seed");
  cfg.target_cr = program.get<double>("--target-cr");
  cfg.cr_tolerance = program.get<double>("--cr-tol");
  cfg.port = program.get<uint16_t>("--port");
  cfg.direct_io = program.get<bool>("--direct-io");
#else
  for (int i = 1; i < argc; ++i) {
    const std::string key = argv[i];
    auto take = [&](const std::string& opt) -> std::string {
      if (i + 1 >= argc) {
        throw std::runtime_error("missing value for " + opt);
      }
      return argv[++i];
    };
    if (key == "--medium") {
      auto v = parse_medium(take(key));
      if (!v) {
        return std::unexpected(v.error());
      }
      cfg.medium = *v;
    } else if (key == "--mode") {
      auto v = parse_mode(take(key));
      if (!v) {
        return std::unexpected(v.error());
      }
      cfg.mode = *v;
    } else if (key == "--role") {
      auto v = parse_role(take(key));
      if (!v) {
        return std::unexpected(v.error());
      }
      cfg.role = *v;
    } else if (key == "--algo") {
      auto v = parse_algo(take(key));
      if (!v) {
        return std::unexpected(v.error());
      }
      cfg.algo = *v;
    } else if (key == "--zstd-level") {
      cfg.zstd_level = std::stoi(take(key));
    } else if (key == "--duration-sec") {
      cfg.duration_sec = static_cast<uint32_t>(std::stoul(take(key)));
    } else if (key == "--warmup-sec") {
      cfg.warmup_sec = static_cast<uint32_t>(std::stoul(take(key)));
    } else if (key == "--repeats") {
      cfg.repeats = static_cast<uint32_t>(std::stoul(take(key)));
    } else if (key == "--threads") {
      cfg.threads = static_cast<uint32_t>(std::stoul(take(key)));
    } else if (key == "--chunk-size") {
      cfg.chunk_size = static_cast<size_t>(std::stoull(take(key)));
    } else if (key == "--queue-depth") {
      cfg.queue_depth = static_cast<uint32_t>(std::stoul(take(key)));
    } else if (key == "--segment-size-gib") {
      cfg.segment_size_bytes = static_cast<size_t>(std::stoull(take(key))) * 1024ULL * 1024ULL * 1024ULL;
    } else if (key == "--output-dir") {
      cfg.output_dir = take(key);
    } else if (key == "--json") {
      cfg.json_output = std::filesystem::path(take(key));
    } else if (key == "--seed") {
      cfg.seed = std::stoull(take(key));
    } else if (key == "--target-cr") {
      cfg.target_cr = std::stod(take(key));
    } else if (key == "--cr-tol") {
      cfg.cr_tolerance = std::stod(take(key));
    } else if (key == "--port") {
      cfg.port = static_cast<uint16_t>(std::stoul(take(key)));
    } else if (key == "--direct-io") {
      cfg.direct_io = true;
    }
  }
#endif

  cfg.queue_depth = static_cast<uint32_t>(ceil_pow2(std::max<uint32_t>(16, cfg.queue_depth)));
  if (cfg.chunk_size == 0 || cfg.duration_sec == 0 || cfg.repeats == 0) {
    return std::unexpected(AppError{"invalid zero-valued core options"});
  }
  return cfg;
}

Result<int> run_sender_or_suite(const Config& cfg) {
  auto mk = ensure_dir(cfg.output_dir);
  if (!mk) {
    return std::unexpected(mk.error());
  }

  Summary summary{};
  const auto modes = select_modes(cfg);

  for (const auto mode : modes) {
    for (uint32_t r = 0; r < cfg.repeats; ++r) {
      if (cfg.warmup_sec > 0) {
        auto warm = run_phase(cfg, mode, Sec(cfg.warmup_sec), r, true);
        if (!warm) {
          return std::unexpected(warm.error());
        }
      }

      auto timed = run_phase(cfg, mode, Sec(cfg.duration_sec), r, false);
      if (!timed) {
        return std::unexpected(timed.error());
      }
      summary.phases[mode].push_back(*timed);
      print_phase(*timed);
    }
  }

  print_human_summary(summary);
  auto json = write_json_summary(cfg, summary);
  if (!json) {
    return std::unexpected(json.error());
  }
  return 0;
}

}  // namespace

namespace bwm::app {

bwm::Expected<PhaseResult> run_phase_for_protocol(const Config& cfg,
                                                  Mode mode,
                                                  std::chrono::seconds duration,
                                                  uint32_t repeat_id,
                                                  bool warmup) {
  return ::run_phase(cfg, mode, duration, repeat_id, warmup);
}

}  // namespace bwm::app

int run_cli_impl(int argc, char** argv) {
  if (has_help_flag(argc, argv)) {
    print_cli_help(argc > 0 ? std::string(argv[0]) : std::string("bwm"));
    return 0;
  }

  auto cfg = parse_args(argc, argv);
  if (!cfg) {
    std::cerr << "error: " << cfg.error().message << "\n";
    return 2;
  }

  if (cfg->role == Role::Receiver) {
    Codec codec(cfg->algo, cfg->zstd_level);
    auto recv = run_tcp_receiver(*cfg, codec);
    if (!recv) {
      std::cerr << "receiver error: " << recv.error().message << "\n";
      return 3;
    }
    return *recv;
  }

  auto run = run_sender_or_suite(*cfg);
  if (!run) {
    std::cerr << "run error: " << run.error().message << "\n";
    return 1;
  }
  return *run;
}
