#include "sink/hash_sink.hpp"

#include <array>

#if __has_include(<xxhash.h>)
#include <xxhash.h>
#else
#error "xxhash.h is required"
#endif

namespace bwm::app {

HashSink::HashSink(uint64_t seed) : seed_(seed), mix_lane_(seed) {}

void HashSink::consume(std::span<const uint8_t> data) {
  constexpr uint64_t kMixMul = 0x9e3779b97f4a7c15ULL;
  const uint64_t size = static_cast<uint64_t>(data.size());
  const uint64_t h = XXH64(data.data(), data.size(), seed_);

  bytes_.fetch_add(size, std::memory_order_relaxed);
  xor_lane_.fetch_xor(h ^ (size * kMixMul), std::memory_order_relaxed);
  mix_lane_.fetch_add((h * kMixMul) + size + seed_, std::memory_order_relaxed);
}

uint64_t HashSink::digest() const {
  const std::array<uint64_t, 4> state{
      seed_,
      bytes_.load(std::memory_order_relaxed),
      xor_lane_.load(std::memory_order_relaxed),
      mix_lane_.load(std::memory_order_relaxed),
  };
  return XXH64(state.data(), sizeof(state), seed_ ^ 0x243f6a8885a308d3ULL);
}

uint64_t HashSink::bytes() const { return bytes_.load(std::memory_order_relaxed); }

}  // namespace bwm::app
