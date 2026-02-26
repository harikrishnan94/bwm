#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

#include "app/config_types.hpp"
#include "bwm/core/expected.hpp"

namespace bwm::app {

class Codec {
 public:
  Codec(Algo algo, int zstd_level);

  Algo id() const;
  size_t max_compressed_size(size_t raw_size) const;

  bwm::Expected<size_t> compress(std::span<const uint8_t> raw, std::span<uint8_t> out) const;
  bwm::Expected<size_t> decompress(std::span<const uint8_t> comp,
                                   std::span<uint8_t> out,
                                   size_t expected_raw) const;

 private:
  Algo algo_{Algo::None};
  int zstd_level_{1};
};

}  // namespace bwm::app
