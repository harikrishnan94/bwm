#pragma once

#include <atomic>
#include <cstdint>
#include <span>

namespace bwm::app {

class HashSink {
 public:
  explicit HashSink(uint64_t seed);
  ~HashSink() = default;

  HashSink(const HashSink&) = delete;
  HashSink& operator=(const HashSink&) = delete;

  void consume(std::span<const uint8_t> data);
  uint64_t digest() const;
  uint64_t bytes() const;

 private:
  uint64_t seed_{0};
  std::atomic<uint64_t> bytes_{0};
  std::atomic<uint64_t> xor_lane_{0};
  std::atomic<uint64_t> mix_lane_{0};
};

}  // namespace bwm::app
