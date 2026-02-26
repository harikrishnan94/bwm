#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include "bwm/core/expected.hpp"

namespace bwm {

struct Job {
  uint64_t seq{};
  uint32_t segment_id{};
  uint32_t chunk_id{};
};

struct SchedulerConfig {
  uint32_t worker_threads{};
  uint32_t queue_depth{};
  size_t chunk_size{};
};

class IScheduler {
 public:
  virtual ~IScheduler() = default;

  virtual Expected<void> start() noexcept = 0;
  virtual Expected<void> submit(Job job) noexcept = 0;
  virtual Expected<void> stop_issue_new_work() noexcept = 0;
  virtual Expected<void> drain() noexcept = 0;
  virtual Expected<void> join() noexcept = 0;
};

Expected<std::unique_ptr<IScheduler>> make_scheduler(const SchedulerConfig&) noexcept;

}  // namespace bwm
