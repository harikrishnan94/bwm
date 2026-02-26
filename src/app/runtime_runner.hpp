#pragma once

#include <chrono>

#include "app/config_types.hpp"
#include "bwm/core/expected.hpp"

int run_cli_impl(int argc, char** argv);

namespace bwm::app {

bwm::Expected<PhaseResult> run_phase_for_protocol(const Config& cfg,
                                                  Mode mode,
                                                  std::chrono::seconds duration,
                                                  uint32_t repeat_id,
                                                  bool warmup);

}
