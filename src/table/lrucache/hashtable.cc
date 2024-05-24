#include "hashtable.h"

namespace Tskydb {

auto HashTable::FindPointer(const Slice &key, uint32_t hash) -> LRUNode ** {
  LRUNode **ptr = &list_[hash & (length_ - 1)];
  while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
    ptr = &(*ptr)->next_hash;
  }
  return ptr;
}

auto HashTable::Insert(LRUNode *h) -> LRUNode * {
  LRUNode **ptr = FindPointer(h->key(), h->hash);
  LRUNode *old = *ptr;
  h->next_hash = (old == nullptr ? nullptr : old->next_hash);
  *ptr = h;
  if (old == nullptr) {
    ++elems_;
    if (elems_ > length_) {
      Resize();
    }
  }
  return old;
}

auto HashTable::Remove(const Slice &key, uint32_t hash) -> LRUNode * {
  LRUNode **ptr = FindPointer(key, hash);
  LRUNode *result = *ptr;
  if (result != nullptr) {
    *ptr = result->next_hash;
    --elems_;
  }
  return result;
}

void HashTable::Resize() {
  uint32_t new_length = 4;
  while (new_length < elems_) {
    new_length *= 2;
  }
  LRUNode **new_list = new LRUNode *[new_length];
  memset(new_list, 0, sizeof(new_list[0]) * new_length);
  for (uint32_t i = 0; i < length_; i++) {
    LRUNode *h = list_[i];
    while (h != nullptr) {
      LRUNode *next = h->next_hash;
      uint32_t hash = h->hash;
      LRUNode **ptr = &new_list[hash & (new_length - 1)];
      h->next_hash = *ptr;
      *ptr = h;
      h = next;
    }
  }
  delete[] list_;
  list_ = new_list;
  length_ = new_length;
}

}  // namespace Tskydb
