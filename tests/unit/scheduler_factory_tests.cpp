#include <chrono>
#include <iostream>

#include "bwm/core/error.hpp"
#include "bwm/scheduler/scheduler.hpp"

namespace {

bool test_invalid_factory_args() {
  bwm::SchedulerConfig bad_workers{};
  bad_workers.worker_threads = 0;
  bad_workers.queue_depth = 16;
  bad_workers.chunk_size = 4096;

  auto s1 = bwm::make_scheduler(bad_workers);
  if (s1 || s1.error().code != bwm::ErrorCode::InvalidArgument) {
    std::cerr << "expected invalid argument for worker_threads=0\n";
    return false;
  }

  bwm::SchedulerConfig bad_queue{};
  bad_queue.worker_threads = 1;
  bad_queue.queue_depth = 0;
  bad_queue.chunk_size = 4096;

  auto s2 = bwm::make_scheduler(bad_queue);
  if (s2 || s2.error().code != bwm::ErrorCode::InvalidArgument) {
    std::cerr << "expected invalid argument for queue_depth=0\n";
    return false;
  }

  return true;
}

bool test_scheduler_lifecycle() {
  bwm::SchedulerConfig cfg{};
  cfg.worker_threads = 2;
  cfg.queue_depth = 32;
  cfg.chunk_size = 4096;

  auto sched = bwm::make_scheduler(cfg);
  if (!sched) {
    std::cerr << "make_scheduler failed: " << sched.error().message << "\n";
    return false;
  }

  auto submit_before_start = (*sched)->submit(bwm::Job{.seq = 1, .segment_id = 0, .chunk_id = 0});
  if (submit_before_start || submit_before_start.error().code != bwm::ErrorCode::InvalidArgument) {
    std::cerr << "submit before start should fail\n";
    return false;
  }

  auto start = (*sched)->start();
  if (!start) {
    std::cerr << "start failed: " << start.error().message << "\n";
    return false;
  }

  for (uint32_t i = 0; i < 64; ++i) {
    auto s = (*sched)->submit(bwm::Job{.seq = i, .segment_id = 1, .chunk_id = i});
    if (!s) {
      std::cerr << "submit failed at i=" << i << ": " << s.error().message << "\n";
      return false;
    }
  }

  auto stop = (*sched)->stop_issue_new_work();
  if (!stop) {
    std::cerr << "stop_issue_new_work failed: " << stop.error().message << "\n";
    return false;
  }

  auto submit_after_stop = (*sched)->submit(bwm::Job{.seq = 65, .segment_id = 0, .chunk_id = 0});
  if (submit_after_stop || submit_after_stop.error().code != bwm::ErrorCode::Unsupported) {
    std::cerr << "submit after stop should be unsupported\n";
    return false;
  }

  auto drain = (*sched)->drain();
  if (!drain) {
    std::cerr << "drain failed: " << drain.error().message << "\n";
    return false;
  }

  auto join = (*sched)->join();
  if (!join) {
    std::cerr << "join failed: " << join.error().message << "\n";
    return false;
  }

  return true;
}

}  // namespace

int main() {
  if (!test_invalid_factory_args()) {
    return 1;
  }
  if (!test_scheduler_lifecycle()) {
    return 1;
  }
  return 0;
}
