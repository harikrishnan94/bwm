#include <cstdint>
#include <format>
#include <iostream>
#include <vector>

#include "generator/low_entropy_generator.hpp"

int main() {
  bwm::app::LowEntropyGenerator gen1(42);
  bwm::app::LowEntropyGenerator gen2(42);
  bwm::app::LowEntropyGenerator gen3(99);

  gen1.set_profile(8, 16, 2000);
  gen2.set_profile(8, 16, 2000);
  gen3.set_profile(8, 16, 2000);

  std::vector<uint8_t> a;
  std::vector<uint8_t> b;
  std::vector<uint8_t> c;

  gen1.generate(7, a, 4096);
  gen2.generate(7, b, 4096);
  gen3.generate(7, c, 4096);

  if (a != b) {
    std::cerr << std::format(
        "Determinism failed: same seed and seq produced different output\n");
    return 1;
  }

  if (a == c) {
    std::cerr << std::format(
        "Diversity failed: different seed produced identical output\n");
    return 1;
  }

  return 0;
}
