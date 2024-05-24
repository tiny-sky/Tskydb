#pragma once

#include <condition_variable>
#include <mutex>

#include "util/macros.h"

namespace Tskydb {

class MutexLock {
 public:
  explicit MutexLock(std::mutex *mu) : mutex_(mu) { mutex_->lock(); }
  ~MutexLock() { mutex_->unlock(); }

  DISALLOW_COPY(MutexLock);

 private:
  std::mutex *const mutex_;
};

// condition variable
class CondVar {
 public:
  explicit CondVar(std::mutex *mutex) : mutex_(mutex) {}
  ~CondVar() = default;

  DISALLOW_COPY(CondVar);

  void Wait() {
    std::unique_lock<std::mutex> lock(*mutex_, std::adopt_lock);
    cv_.wait(lock);
    lock.release();
  }
  void Signal() { cv_.notify_one(); }
  void SignalAll() { cv_.notify_all(); }

 private:
  std::condition_variable cv_;
  std::mutex *mutex_;
};
}  // namespace Tskydb
