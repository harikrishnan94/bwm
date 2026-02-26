#include "codec/runtime_codec.hpp"

#include <cstring>
#include <string>

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

namespace bwm::app {

using AppError = bwm::Error;

Codec::Codec(Algo algo, int zstd_level) : algo_(algo), zstd_level_(zstd_level) {}

Algo Codec::id() const { return algo_; }

size_t Codec::max_compressed_size(size_t raw_size) const {
  switch (algo_) {
    case Algo::None:
      return raw_size;
    case Algo::Lz4:
#if BWM_HAS_LZ4
      return static_cast<size_t>(LZ4_compressBound(static_cast<int>(raw_size)));
#else
      return raw_size;
#endif
    case Algo::Zstd:
#if BWM_HAS_ZSTD
      return static_cast<size_t>(ZSTD_compressBound(raw_size));
#else
      return raw_size;
#endif
  }
  return raw_size;
}

bwm::Expected<size_t> Codec::compress(std::span<const uint8_t> raw, std::span<uint8_t> out) const {
  switch (algo_) {
    case Algo::None: {
      if (out.size() < raw.size()) {
        return std::unexpected(AppError{"output buffer too small"});
      }
      std::memcpy(out.data(), raw.data(), raw.size());
      return raw.size();
    }
    case Algo::Lz4: {
#if BWM_HAS_LZ4
      const int n = LZ4_compress_default(reinterpret_cast<const char*>(raw.data()),
                                         reinterpret_cast<char*>(out.data()),
                                         static_cast<int>(raw.size()),
                                         static_cast<int>(out.size()));
      if (n <= 0) {
        return std::unexpected(AppError{"lz4 compress failed"});
      }
      return static_cast<size_t>(n);
#else
      return std::unexpected(AppError{"lz4 support not available in this build"});
#endif
    }
    case Algo::Zstd: {
#if BWM_HAS_ZSTD
      const size_t n = ZSTD_compress(out.data(), out.size(), raw.data(), raw.size(), zstd_level_);
      if (ZSTD_isError(n)) {
        return std::unexpected(AppError{std::string("zstd compress failed: ") + ZSTD_getErrorName(n)});
      }
      return n;
#else
      return std::unexpected(AppError{"zstd support not available in this build"});
#endif
    }
  }
  return std::unexpected(AppError{"unknown codec"});
}

bwm::Expected<size_t> Codec::decompress(std::span<const uint8_t> comp,
                                        std::span<uint8_t> out,
                                        size_t expected_raw) const {
  switch (algo_) {
    case Algo::None: {
      if (comp.size() != expected_raw || out.size() < expected_raw) {
        return std::unexpected(AppError{"none codec size mismatch"});
      }
      std::memcpy(out.data(), comp.data(), expected_raw);
      return expected_raw;
    }
    case Algo::Lz4: {
#if BWM_HAS_LZ4
      const int n = LZ4_decompress_safe(reinterpret_cast<const char*>(comp.data()),
                                        reinterpret_cast<char*>(out.data()),
                                        static_cast<int>(comp.size()),
                                        static_cast<int>(out.size()));
      if (n < 0 || static_cast<size_t>(n) != expected_raw) {
        return std::unexpected(AppError{"lz4 decompress failed"});
      }
      return static_cast<size_t>(n);
#else
      return std::unexpected(AppError{"lz4 support not available in this build"});
#endif
    }
    case Algo::Zstd: {
#if BWM_HAS_ZSTD
      const size_t n = ZSTD_decompress(out.data(), out.size(), comp.data(), comp.size());
      if (ZSTD_isError(n) || n != expected_raw) {
        return std::unexpected(AppError{"zstd decompress failed"});
      }
      return n;
#else
      return std::unexpected(AppError{"zstd support not available in this build"});
#endif
    }
  }
  return std::unexpected(AppError{"unknown codec"});
}

}  // namespace bwm::app
