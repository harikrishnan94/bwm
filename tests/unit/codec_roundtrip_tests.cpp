#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <span>

#include "bwm/codec/codec.hpp"

namespace {

bool test_none_roundtrip() {
  bwm::CodecParams params{};
  params.id = bwm::CodecId::None;
  params.level = 0;

  auto codec = bwm::make_codec(params);
  if (!codec) {
    std::cerr << "make_codec(None) failed\n";
    return false;
  }

  std::array<std::byte, 16> raw{};
  for (size_t i = 0; i < raw.size(); ++i) {
    raw[i] = static_cast<std::byte>(i + 1);
  }

  std::array<std::byte, 16> comp{};
  auto csize = (*codec)->compress(raw, comp);
  if (!csize || *csize != raw.size()) {
    std::cerr << "None compress failed\n";
    return false;
  }

  std::array<std::byte, 16> out{};
  auto dsize = (*codec)->decompress(std::span<const std::byte>(comp.data(), *csize), out, raw.size());
  if (!dsize || *dsize != raw.size()) {
    std::cerr << "None decompress failed\n";
    return false;
  }

  for (size_t i = 0; i < raw.size(); ++i) {
    if (raw[i] != out[i]) {
      std::cerr << "None roundtrip mismatch at index " << i << "\n";
      return false;
    }
  }
  return true;
}

bool test_small_buffer_error() {
  bwm::CodecParams params{};
  params.id = bwm::CodecId::None;

  auto codec = bwm::make_codec(params);
  if (!codec) {
    std::cerr << "make_codec(None) failed\n";
    return false;
  }

  std::array<std::byte, 16> raw{};
  std::array<std::byte, 4> comp{};
  auto csize = (*codec)->compress(raw, comp);
  if (csize) {
    std::cerr << "Expected compress to fail for too-small output buffer\n";
    return false;
  }
  return true;
}

}  // namespace

int main() {
  if (!test_none_roundtrip()) {
    return 1;
  }
  if (!test_small_buffer_error()) {
    return 1;
  }
  return 0;
}
