#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace bwm::app {

class LowEntropyGenerator {
 public:
  explicit LowEntropyGenerator(uint64_t seed);

  void set_profile(uint32_t alphabet, uint32_t run_len, uint32_t noise_ppm);
  void generate(uint64_t seq, std::vector<uint8_t>& out, size_t size) const;

 private:
  uint64_t seed_;
  uint32_t alphabet_{8};
  uint32_t run_len_{16};
  uint32_t noise_ppm_{2000};
};

}  // namespace bwm::app
