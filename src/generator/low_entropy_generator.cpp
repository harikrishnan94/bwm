#include "generator/low_entropy_generator.hpp"

#include <algorithm>

#include "app/math_utils.hpp"

namespace bwm::app {

LowEntropyGenerator::LowEntropyGenerator(uint64_t seed) : seed_(seed) {}

void LowEntropyGenerator::set_profile(uint32_t alphabet, uint32_t run_len, uint32_t noise_ppm) {
  alphabet_ = std::max(2u, std::min(256u, alphabet));
  run_len_ = std::max(1u, run_len);
  noise_ppm_ = std::min(1'000'000u, noise_ppm);
}

void LowEntropyGenerator::generate(uint64_t seq, std::vector<uint8_t>& out, size_t size) const {
  out.resize(size);
  uint64_t s = seed_ ^ (seq * 0x9e3779b97f4a7c15ULL);
  uint8_t symbol = static_cast<uint8_t>(xorshift64(s) % alphabet_);
  uint32_t run_left = static_cast<uint32_t>(xorshift64(s) % run_len_) + 1;

  for (size_t i = 0; i < size; ++i) {
    const auto rnd = static_cast<uint32_t>(xorshift64(s) % 1'000'000ULL);
    if (run_left == 0 || rnd < noise_ppm_) {
      symbol = static_cast<uint8_t>(xorshift64(s) % alphabet_);
      run_left = static_cast<uint32_t>(xorshift64(s) % run_len_) + 1;
    }
    out[i] = static_cast<uint8_t>('A' + (symbol % 26));
    if (run_left > 0) {
      --run_left;
    }
  }
}

}  // namespace bwm::app
