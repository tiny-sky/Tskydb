#pragma once

#include <cstdint>
#include <vector>
#include <atomic>

#include "util/macros.h"

namespace Tskydb {
class Arena {
 public:
  Arena();

  ~Arena();

  DISALLOW_COPY_AND_MOVE(Arena);

  auto Allocate(size_t bytes) -> char *;

  auto AllocateAligned(size_t bytes) -> char *;

  size_t MemoryUsage() const {
    return memory_usage_.load(std::memory_order_relaxed);
  }

 private:
  char *AllocateFallback(size_t bytes);
  char *AllocateNewBlock(size_t block_bytes);

  char *alloc_ptr_;
  size_t alloc_bytes_remaining_;

  std::vector<char *> blocks_;

  std::atomic<size_t> memory_usage_;
};

inline auto Arena::Allocate(size_t bytes) -> char * {
  if (bytes <= alloc_bytes_remaining_) {
    char *result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  return AllocateFallback(bytes);
}

}  // namespace Tskydb
