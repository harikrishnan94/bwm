#include <chrono>
#include <format>
#include <iostream>

#include "bwm/core/error.hpp"
#include "bwm/scheduler/scheduler.hpp"

namespace {

bool test_invalid_factory_args() {
  bwm::SchedulerConfig bad_workers{};
  bad_workers.worker_threads = 0;
  bad_workers.queue_depth = 16;
  bad_workers.chunk_size = 4096;

  bool bad_workers_rejected = false;
  try {
    static_cast<void>(bwm::make_scheduler(bad_workers));
  } catch (const bwm::Error &e) {
    bad_workers_rejected = (e.code() == bwm::ErrorCode::InvalidArgument);
  }
  if (!bad_workers_rejected) {
    std::cerr <<
        std::format("expected invalid argument for worker_threads=0\n");
    return false;
  }

  bwm::SchedulerConfig bad_queue{};
  bad_queue.worker_threads = 1;
  bad_queue.queue_depth = 0;
  bad_queue.chunk_size = 4096;

  bool bad_queue_rejected = false;
  try {
    static_cast<void>(bwm::make_scheduler(bad_queue));
  } catch (const bwm::Error &e) {
    bad_queue_rejected = (e.code() == bwm::ErrorCode::InvalidArgument);
  }
  if (!bad_queue_rejected) {
    std::cerr << std::format("expected invalid argument for queue_depth=0\n");
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
    std::cerr << std::format("make_scheduler failed\n");
    return false;
  }

  bool submit_before_start_failed = false;
  try {
    sched->submit(bwm::Job{.seq = 1, .segment_id = 0, .chunk_id = 0});
  } catch (const bwm::Error &e) {
    submit_before_start_failed = (e.code() == bwm::ErrorCode::InvalidArgument);
  }
  if (!submit_before_start_failed) {
    std::cerr << std::format("submit before start should fail\n");
    return false;
  }

  try {
    sched->start();
  } catch (const bwm::Error &e) {
    std::cerr << std::format("start failed: {}\n", e.what());
    return false;
  }

  for (uint32_t i = 0; i < 64; ++i) {
    try {
      sched->submit(bwm::Job{.seq = i, .segment_id = 1, .chunk_id = i});
    } catch (const bwm::Error &e) {
      std::cerr << std::format("submit failed at i={}: {}\n", i, e.what());
      return false;
    }
  }

  try {
    sched->stop_issue_new_work();
  } catch (const bwm::Error &e) {
    std::cerr << std::format("stop_issue_new_work failed: {}\n", e.what());
    return false;
  }

  bool submit_after_stop_failed = false;
  try {
    sched->submit(bwm::Job{.seq = 65, .segment_id = 0, .chunk_id = 0});
  } catch (const bwm::Error &e) {
    submit_after_stop_failed = (e.code() == bwm::ErrorCode::Unsupported);
  }
  if (!submit_after_stop_failed) {
    std::cerr << std::format("submit after stop should be unsupported\n");
    return false;
  }

  try {
    sched->drain();
  } catch (const bwm::Error &e) {
    std::cerr << std::format("drain failed: {}\n", e.what());
    return false;
  }

  try {
    sched->join();
  } catch (const bwm::Error &e) {
    std::cerr << std::format("join failed: {}\n", e.what());
    return false;
  }

  return true;
}

} // namespace

int main() {
  if (!test_invalid_factory_args()) {
    return 1;
  }
  if (!test_scheduler_lifecycle()) {
    return 1;
  }
  return 0;
}
