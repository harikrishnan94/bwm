#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

#include "bwm/core/types.hpp"

namespace bwm {

struct CodecParams {
  CodecId id{CodecId::None};
  int level{0};
};

class ICodec {
 public:
  virtual ~ICodec() = default;

  virtual CodecId id() const noexcept = 0;
  virtual const char* name() const noexcept = 0;

  virtual size_t max_compressed_size(size_t raw_size) const = 0;

  virtual size_t compress(std::span<const std::byte> raw,
                          std::span<std::byte> out) = 0;

  virtual size_t decompress(std::span<const std::byte> comp,
                            std::span<std::byte> out,
                            size_t expected_raw_size) = 0;
};

std::unique_ptr<ICodec> make_codec(const CodecParams& params);

}  // namespace bwm
