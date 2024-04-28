#include <cstdint>

#include "util/slice.h"

namespace Tskydb {

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
}  // namespace Tskydb
