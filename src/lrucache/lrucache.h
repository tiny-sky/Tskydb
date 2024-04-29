#pragma once

#include <mutex>

#include "hashtable.h"
#include "util/hash.h"
#include "util/slice.h"

namespace Tskydb {
class LRUCache {
 public:
  LRUCache(size_t capacity = 0);
  ~LRUCache();

  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  auto Insert(const Slice &key, size_t charge, void *value) -> LRUNode *;

  auto Lookup(const Slice &key) -> LRUNode *;

  void Release(LRUNode *node);

  void Erase(const Slice &key);

  auto FinishErase(LRUNode *e) -> bool;

 private:
  static inline uint32_t HashSlice(const Slice &s) {
    return Hash(s.data(), s.size(), 0);
  }

  void LRU_Remove(LRUNode *e);
  void LRU_Append(LRUNode *list, LRUNode *e);
  void Pin(LRUNode *e);
  void UnPin(LRUNode *e);

 private:
  size_t capacity_;
  size_t usage_;
  LRUNode lru_;
  LRUNode in_use_;
  HashTable table_;
  std::mutex latch_;
};
}  // namespace Tskydb
