#pragma once

#include <cstdint>

#include <mutex>

#include "util/hash.h"
#include "util/slice.h"

namespace Tskydb {

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

struct LRUNode {
  void *value;
  LRUNode *next_hash;
  LRUNode *next;
  LRUNode *prev;
  uint32_t hash;
  uint32_t refs;
  bool in_cache;
  size_t charge;
  size_t key_length;
  char key_data[0];

  Slice key() const {
    assert(next != this);
    return Slice(key_data, key_length);
  }
};

class HashTable {
 public:
  HashTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }

  auto Lookup(const Slice &key, uint32_t hash) -> LRUNode * {
    return *FindPointer(key, hash);
  };

  auto Insert(LRUNode *h) -> LRUNode *;

  auto Remove(const Slice &key, uint32_t hash) -> LRUNode *;

 private:
  auto FindPointer(const Slice &key, uint32_t hash) -> LRUNode **;

  void Resize();

 private:
  uint32_t length_;
  uint32_t elems_;
  LRUNode **list_;
};

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
