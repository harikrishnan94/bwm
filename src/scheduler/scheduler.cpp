#include "bwm/scheduler/scheduler.hpp"

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <thread>
#include <vector>

#include "bwm/core/error.hpp"

namespace bwm {
namespace {

class BasicScheduler final : public IScheduler {
 public:
  explicit BasicScheduler(SchedulerConfig cfg)
      : cfg_(cfg),
        queue_depth_(cfg.queue_depth == 0 ? 1u : cfg.queue_depth),
        worker_threads_(cfg.worker_threads == 0 ? 1u : cfg.worker_threads) {}

  ~BasicScheduler() override { (void)join(); }

  Expected<void> start() noexcept override {
    {
      std::scoped_lock lock(mu_);
      if (started_) {
        return {};
      }
      accepting_ = true;
      stopping_ = false;
      started_ = true;
    }

    std::vector<std::thread> local_workers;
    try {
      local_workers.reserve(worker_threads_);
      for (uint32_t i = 0; i < worker_threads_; ++i) {
        local_workers.emplace_back([this]() { this->worker_loop(); });
      }
    } catch (...) {
      {
        std::scoped_lock lock(mu_);
        accepting_ = false;
        stopping_ = true;
        started_ = false;
      }
      for (auto& t : local_workers) {
        if (t.joinable()) {
          t.join();
        }
      }
      return std::unexpected(Error{ErrorCode::Internal, "failed to spawn scheduler workers"});
    }

    {
      std::scoped_lock lock(mu_);
      workers_ = std::move(local_workers);
    }
    return {};
  }

  Expected<void> submit(Job job) noexcept override {
    std::unique_lock lock(mu_);
    if (!started_) {
      return std::unexpected(Error{ErrorCode::InvalidArgument, "scheduler has not started"});
    }
    if (!accepting_) {
      return std::unexpected(Error{ErrorCode::Unsupported, "scheduler is not accepting new jobs"});
    }

    cv_not_full_.wait(lock, [this]() { return queue_.size() < queue_depth_ || stopping_; });
    if (stopping_) {
      return std::unexpected(Error{ErrorCode::Unsupported, "scheduler is stopping"});
    }

    queue_.push_back(job);
    cv_not_empty_.notify_one();
    return {};
  }

  Expected<void> stop_issue_new_work() noexcept override {
    std::scoped_lock lock(mu_);
    accepting_ = false;
    cv_not_full_.notify_all();
    return {};
  }

  Expected<void> drain() noexcept override {
    std::unique_lock lock(mu_);
    if (!started_) {
      return std::unexpected(Error{ErrorCode::InvalidArgument, "scheduler has not started"});
    }
    cv_drained_.wait(lock, [this]() { return queue_.empty() && in_flight_ == 0; });
    return {};
  }

  Expected<void> join() noexcept override {
    std::vector<std::thread> local_workers;
    {
      std::scoped_lock lock(mu_);
      if (!started_) {
        return {};
      }
      accepting_ = false;
      stopping_ = true;
      local_workers.swap(workers_);
      queue_.clear();
      started_ = false;
    }

    cv_not_empty_.notify_all();
    cv_not_full_.notify_all();
    cv_drained_.notify_all();

    for (auto& t : local_workers) {
      if (t.joinable()) {
        t.join();
      }
    }
    return {};
  }

 private:
  void worker_loop() {
    for (;;) {
      Job job{};
      {
        std::unique_lock lock(mu_);
        cv_not_empty_.wait(lock, [this]() { return stopping_ || !queue_.empty(); });
        if (stopping_ && queue_.empty()) {
          return;
        }
        job = queue_.front();
        queue_.pop_front();
        ++in_flight_;
        cv_not_full_.notify_one();
      }

      (void)job;

      {
        std::scoped_lock lock(mu_);
        --in_flight_;
        ++processed_count_;
        if (queue_.empty() && in_flight_ == 0) {
          cv_drained_.notify_all();
        }
      }
    }
  }

  SchedulerConfig cfg_{};
  size_t queue_depth_{1};
  uint32_t worker_threads_{1};

  std::mutex mu_;
  std::condition_variable cv_not_empty_;
  std::condition_variable cv_not_full_;
  std::condition_variable cv_drained_;

  std::deque<Job> queue_;
  std::vector<std::thread> workers_;
  size_t in_flight_{0};
  uint64_t processed_count_{0};

  bool started_{false};
  bool accepting_{false};
  bool stopping_{false};
};

}  // namespace

Expected<std::unique_ptr<IScheduler>> make_scheduler(const SchedulerConfig& cfg) noexcept {
  if (cfg.worker_threads == 0) {
    return std::unexpected(Error{ErrorCode::InvalidArgument, "worker_threads must be > 0"});
  }
  if (cfg.queue_depth == 0) {
    return std::unexpected(Error{ErrorCode::InvalidArgument, "queue_depth must be > 0"});
  }
  try {
    return std::make_unique<BasicScheduler>(cfg);
  } catch (...) {
    return std::unexpected(Error{ErrorCode::Internal, "failed to allocate scheduler"});
  }
}

}  // namespace bwm
