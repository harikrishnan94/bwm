#include "bwm/medium/medium.hpp"

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <utility>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "bwm/core/error.hpp"

namespace bwm {
namespace {

Expected<void> write_all(int fd, const std::byte* data, size_t size) noexcept {
  size_t done = 0;
  while (done < size) {
    const ssize_t n = ::write(fd, data + done, size - done);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return std::unexpected(Error{ErrorCode::IoError, std::strerror(errno)});
    }
    done += static_cast<size_t>(n);
  }
  return {};
}

Expected<void> send_all(int fd, const std::byte* data, size_t size) noexcept {
  size_t done = 0;
  while (done < size) {
    const ssize_t n = ::send(fd, data + done, size - done, 0);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return std::unexpected(Error{ErrorCode::IoError, std::strerror(errno)});
    }
    done += static_cast<size_t>(n);
  }
  return {};
}

Expected<void> recv_all(int fd, std::byte* data, size_t size) noexcept {
  size_t done = 0;
  while (done < size) {
    const ssize_t n = ::recv(fd, data + done, size - done, 0);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return std::unexpected(Error{ErrorCode::IoError, std::strerror(errno)});
    }
    if (n == 0) {
      return std::unexpected(Error{ErrorCode::IoError, "unexpected EOF on socket"});
    }
    done += static_cast<size_t>(n);
  }
  return {};
}

class DiskWriter final : public IMediumWriter {
 public:
  explicit DiskWriter(int fd) : fd_(fd) {}

  ~DiskWriter() override { (void)close(); }

  Expected<void> append_chunk(const ChunkHeader& hdr,
                              std::span<const std::byte> payload) noexcept override {
    if (closed_ || fd_ < 0) {
      return std::unexpected(Error{ErrorCode::InvalidArgument, "disk writer is closed"});
    }
    if (payload.size() != hdr.comp_size) {
      return std::unexpected(Error{ErrorCode::InvalidArgument, "payload size does not match chunk header"});
    }

    auto wh = write_all(fd_, reinterpret_cast<const std::byte*>(&hdr), sizeof(hdr));
    if (!wh) {
      return std::unexpected(wh.error());
    }
    return write_all(fd_, payload.data(), payload.size());
  }

  Expected<void> finalize_segment() noexcept override { return {}; }

  Expected<void> sync() noexcept override {
    if (closed_ || fd_ < 0) {
      return std::unexpected(Error{ErrorCode::InvalidArgument, "disk writer is closed"});
    }
#if defined(__APPLE__)
    if (::fsync(fd_) != 0) {
#else
    if (::fdatasync(fd_) != 0) {
#endif
      return std::unexpected(Error{ErrorCode::IoError, std::strerror(errno)});
    }
    return {};
  }

  Expected<void> close() noexcept override {
    if (closed_) {
      return {};
    }
    closed_ = true;
    if (fd_ >= 0) {
      if (::close(fd_) != 0) {
        fd_ = -1;
        return std::unexpected(Error{ErrorCode::IoError, std::strerror(errno)});
      }
      fd_ = -1;
    }
    return {};
  }

 private:
  int fd_{-1};
  bool closed_{false};
};

class DiskReader final : public IMediumReader {
 public:
  explicit DiskReader(std::vector<std::vector<OwnedChunk>> segments)
      : segments_(std::move(segments)) {}

  Expected<OwnedChunk> read_chunk_by_index(uint32_t segment_id,
                                           uint32_t chunk_id) noexcept override {
    if (closed_) {
      return std::unexpected(Error{ErrorCode::InvalidArgument, "disk reader is closed"});
    }
    if (segment_id >= segments_.size()) {
      return std::unexpected(Error{ErrorCode::InvalidArgument, "segment_id out of range"});
    }
    const auto& seg = segments_[segment_id];
    if (chunk_id >= seg.size()) {
      return std::unexpected(Error{ErrorCode::InvalidArgument, "chunk_id out of range"});
    }
    return seg[chunk_id];
  }

  Expected<void> close() noexcept override {
    closed_ = true;
    segments_.clear();
    return {};
  }

 private:
  std::vector<std::vector<OwnedChunk>> segments_;
  bool closed_{false};
};

class DiskMedium final : public IMedium {
 public:
  explicit DiskMedium(RunConfig run) : run_(std::move(run)) {}

  MediumKind kind() const noexcept override { return MediumKind::Disk; }

  MediumCaps capabilities() const noexcept override {
    MediumCaps caps{};
#ifdef __linux__
    caps.direct_io_supported = true;
#else
    caps.direct_io_supported = false;
#endif
#ifdef __APPLE__
    caps.cache_hint_supported = true;
#else
    caps.cache_hint_supported = false;
#endif
    caps.requires_sync_for_durability = true;
    return caps;
  }

  Expected<std::unique_ptr<IMediumWriter>> open_writer(const WriteOpenParams& params) noexcept override {
    std::filesystem::path path = params.path;
    if (path.empty()) {
      path = std::filesystem::path(run_.output_dir) /
             ("segment_" + std::to_string(params.segment_id) + ".dat");
    }
    std::error_code ec;
    std::filesystem::create_directories(path.parent_path(), ec);
    int flags = O_CREAT | O_WRONLY;
    flags |= params.truncate ? O_TRUNC : O_APPEND;
    const int fd = ::open(path.c_str(), flags, 0644);
    if (fd < 0) {
      return std::unexpected(Error{ErrorCode::IoError, std::strerror(errno)});
    }
    return std::make_unique<DiskWriter>(fd);
  }

  Expected<std::unique_ptr<IMediumReader>> open_reader(const ReadOpenParams& params) noexcept override {
    if (params.segment_paths.empty()) {
      return std::unexpected(Error{ErrorCode::InvalidArgument, "no disk segment paths provided"});
    }

    std::vector<std::vector<OwnedChunk>> segments;
    segments.reserve(params.segment_paths.size());

    for (const auto& path : params.segment_paths) {
      std::ifstream in(path, std::ios::binary);
      if (!in.is_open()) {
        return std::unexpected(Error{ErrorCode::IoError, "failed to open segment: " + path});
      }

      std::vector<OwnedChunk> chunk_vec;
      for (;;) {
        ChunkHeader hdr{};
        in.read(reinterpret_cast<char*>(&hdr), static_cast<std::streamsize>(sizeof(hdr)));
        if (in.eof()) {
          break;
        }
        if (!in) {
          return std::unexpected(Error{ErrorCode::IoError, "failed to read chunk header"});
        }

        OwnedChunk chunk{};
        chunk.sequence = static_cast<uint64_t>(chunk_vec.size());
        chunk.storage.resize(hdr.comp_size);
        if (!chunk.storage.empty()) {
          in.read(reinterpret_cast<char*>(chunk.storage.data()),
                  static_cast<std::streamsize>(chunk.storage.size()));
          if (!in) {
            return std::unexpected(Error{ErrorCode::IoError, "failed to read chunk payload"});
          }
        }
        chunk_vec.push_back(std::move(chunk));
      }

      segments.push_back(std::move(chunk_vec));
    }

    return std::make_unique<DiskReader>(std::move(segments));
  }

 private:
  RunConfig run_;
};

class TcpSenderWriter final : public IMediumWriter {
 public:
  explicit TcpSenderWriter(int sockfd) : sockfd_(sockfd) {}

  ~TcpSenderWriter() override { (void)close(); }

  Expected<void> append_chunk(const ChunkHeader& hdr,
                              std::span<const std::byte> payload) noexcept override {
    if (closed_ || sockfd_ < 0) {
      return std::unexpected(Error{ErrorCode::InvalidArgument, "tcp sender writer is closed"});
    }
    if (payload.size() != hdr.comp_size) {
      return std::unexpected(Error{ErrorCode::InvalidArgument, "payload size does not match chunk header"});
    }

    auto sh = send_all(sockfd_, reinterpret_cast<const std::byte*>(&hdr), sizeof(hdr));
    if (!sh) {
      return std::unexpected(sh.error());
    }
    return send_all(sockfd_, payload.data(), payload.size());
  }

  Expected<void> finalize_segment() noexcept override {
    if (closed_ || sockfd_ < 0) {
      return {};
    }
    const ChunkHeader end{};
    return send_all(sockfd_, reinterpret_cast<const std::byte*>(&end), sizeof(end));
  }

  Expected<void> sync() noexcept override { return {}; }

  Expected<void> close() noexcept override {
    if (closed_) {
      return {};
    }
    closed_ = true;
    if (sockfd_ >= 0) {
      if (::close(sockfd_) != 0) {
        sockfd_ = -1;
        return std::unexpected(Error{ErrorCode::IoError, std::strerror(errno)});
      }
      sockfd_ = -1;
    }
    return {};
  }

 private:
  int sockfd_{-1};
  bool closed_{false};
};

class TcpReceiverReader final : public IMediumReader {
 public:
  explicit TcpReceiverReader(int connfd) : connfd_(connfd) {}

  ~TcpReceiverReader() override { (void)close(); }

  Expected<OwnedChunk> read_chunk_by_index(uint32_t segment_id,
                                           uint32_t chunk_id) noexcept override {
    if (closed_ || connfd_ < 0) {
      return std::unexpected(Error{ErrorCode::InvalidArgument, "tcp receiver reader is closed"});
    }
    if (segment_id != 0) {
      return std::unexpected(Error{ErrorCode::InvalidArgument, "tcp medium exposes only segment 0"});
    }

    while (!eof_ && chunks_.size() <= chunk_id) {
      ChunkHeader hdr{};
      auto rh = recv_all(connfd_, reinterpret_cast<std::byte*>(&hdr), sizeof(hdr));
      if (!rh) {
        return std::unexpected(rh.error());
      }
      if (hdr.raw_size == 0 && hdr.comp_size == 0) {
        eof_ = true;
        break;
      }

      OwnedChunk chunk{};
      chunk.sequence = static_cast<uint64_t>(chunks_.size());
      chunk.storage.resize(hdr.comp_size);
      if (!chunk.storage.empty()) {
        auto rp = recv_all(connfd_, chunk.storage.data(), chunk.storage.size());
        if (!rp) {
          return std::unexpected(rp.error());
        }
      }
      chunks_.push_back(std::move(chunk));
    }

    if (chunk_id >= chunks_.size()) {
      return std::unexpected(Error{ErrorCode::InvalidArgument, "chunk_id out of range"});
    }
    return chunks_[chunk_id];
  }

  Expected<void> close() noexcept override {
    if (closed_) {
      return {};
    }
    closed_ = true;
    chunks_.clear();
    if (connfd_ >= 0) {
      if (::close(connfd_) != 0) {
        connfd_ = -1;
        return std::unexpected(Error{ErrorCode::IoError, std::strerror(errno)});
      }
      connfd_ = -1;
    }
    return {};
  }

 private:
  int connfd_{-1};
  bool eof_{false};
  bool closed_{false};
  std::vector<OwnedChunk> chunks_;
};

class TcpSenderMedium final : public IMedium {
 public:
  explicit TcpSenderMedium(RunConfig run) : run_(std::move(run)) {}

  MediumKind kind() const noexcept override { return MediumKind::Tcp; }

  MediumCaps capabilities() const noexcept override {
    MediumCaps caps{};
    caps.direct_io_supported = false;
    caps.cache_hint_supported = false;
    caps.requires_sync_for_durability = false;
    return caps;
  }

  Expected<std::unique_ptr<IMediumWriter>> open_writer(const WriteOpenParams& params) noexcept override {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
      return std::unexpected(Error{ErrorCode::IoError, std::strerror(errno)});
    }

    sockaddr_in addr{};
    const auto host = params.host.empty() ? std::string{"127.0.0.1"} : params.host;
    const auto port = params.port == 0 ? run_.tcp_port : params.port;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
      ::close(fd);
      return std::unexpected(Error{ErrorCode::InvalidArgument, "invalid tcp host"});
    }

    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
      ::close(fd);
      return std::unexpected(Error{ErrorCode::IoError, std::strerror(errno)});
    }

    return std::make_unique<TcpSenderWriter>(fd);
  }

  Expected<std::unique_ptr<IMediumReader>> open_reader(const ReadOpenParams&) noexcept override {
    return std::unexpected(Error{ErrorCode::Unsupported, "tcp sender medium does not support read"});
  }

 private:
  RunConfig run_;
};

class TcpReceiverMedium final : public IMedium {
 public:
  explicit TcpReceiverMedium(RunConfig run) : run_(std::move(run)) {}

  MediumKind kind() const noexcept override { return MediumKind::Tcp; }

  MediumCaps capabilities() const noexcept override {
    MediumCaps caps{};
    caps.direct_io_supported = false;
    caps.cache_hint_supported = false;
    caps.requires_sync_for_durability = false;
    return caps;
  }

  Expected<std::unique_ptr<IMediumWriter>> open_writer(const WriteOpenParams&) noexcept override {
    return std::unexpected(Error{ErrorCode::Unsupported, "tcp receiver medium does not support write"});
  }

  Expected<std::unique_ptr<IMediumReader>> open_reader(const ReadOpenParams& params) noexcept override {
    const int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) {
      return std::unexpected(Error{ErrorCode::IoError, std::strerror(errno)});
    }

    int yes = 1;
    (void)::setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    sockaddr_in addr{};
    const auto host = params.host.empty() ? std::string{"127.0.0.1"} : params.host;
    const auto port = params.port == 0 ? run_.tcp_port : params.port;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
      ::close(lfd);
      return std::unexpected(Error{ErrorCode::InvalidArgument, "invalid tcp host"});
    }

    if (::bind(lfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
      ::close(lfd);
      return std::unexpected(Error{ErrorCode::IoError, std::strerror(errno)});
    }
    if (::listen(lfd, 1) != 0) {
      ::close(lfd);
      return std::unexpected(Error{ErrorCode::IoError, std::strerror(errno)});
    }

    const int cfd = ::accept(lfd, nullptr, nullptr);
    ::close(lfd);
    if (cfd < 0) {
      return std::unexpected(Error{ErrorCode::IoError, std::strerror(errno)});
    }

    return std::make_unique<TcpReceiverReader>(cfd);
  }

 private:
  RunConfig run_;
};

}  // namespace

Expected<std::unique_ptr<IMedium>> make_disk_medium(const RunConfig& run) noexcept {
  try {
    return std::make_unique<DiskMedium>(run);
  } catch (...) {
    return std::unexpected(Error{ErrorCode::Internal, "failed to allocate disk medium"});
  }
}

Expected<std::unique_ptr<IMedium>> make_tcp_sender_medium(const RunConfig& run) noexcept {
  try {
    return std::make_unique<TcpSenderMedium>(run);
  } catch (...) {
    return std::unexpected(Error{ErrorCode::Internal, "failed to allocate tcp sender medium"});
  }
}

Expected<std::unique_ptr<IMedium>> make_tcp_receiver_medium(const RunConfig& run) noexcept {
  try {
    return std::make_unique<TcpReceiverMedium>(run);
  } catch (...) {
    return std::unexpected(Error{ErrorCode::Internal, "failed to allocate tcp receiver medium"});
  }
}

}  // namespace bwm
