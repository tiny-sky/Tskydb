#pragma once

#include <cstdint>
#include <vector>

#include "macros.h"

namespace Tskydb {
class Arena {
 public:
  Arena();

  ~Arena();

  DISALLOW_COPY_AND_MOVE(Arena);

  auto Allocate(size_t bytes) -> char *;

  auto AllocateAligned(size_t bytes) -> char *;

 private:
  char *AllocateFallback(size_t bytes);
  char *AllocateNewBlock(size_t block_bytes);

  char *alloc_ptr_;
  size_t alloc_bytes_remaining_;

  std::vector<char *> blocks_;

  size_t memory_usage_;
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
