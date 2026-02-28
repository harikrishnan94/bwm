#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

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

  virtual void start() = 0;
  virtual void submit(Job job) = 0;
  virtual void stop_issue_new_work() = 0;
  virtual void drain() = 0;
  virtual void join() = 0;
};

std::unique_ptr<IScheduler> make_scheduler(const SchedulerConfig&);

}  // namespace bwm
