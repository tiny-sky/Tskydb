#include "lrucache.h"

namespace Tskydb {
LRUCache::LRUCache(size_t capacity) : capacity_(capacity), usage_(0) {
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

LRUCache::~LRUCache() {
  assert(in_use_.next = &in_use_);

  for (LRUNode *e = lru_.next; e != &lru_;) {
    LRUNode *next = e->next;
    e->in_cache = false;
    UnPin(e);
    e = next;
  }
}

void LRUCache::Pin(LRUNode *e) {
  if (e->refs == 1 && e->in_cache) {
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

void LRUCache::UnPin(LRUNode *e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) {
    free(e);
  } else if (e->in_cache && e->refs == 1) {
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

auto LRUCache::FinishErase(LRUNode *e) -> bool {
  if (e != nullptr) {
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    UnPin(e);
  }
  return e != nullptr;
}

void LRUCache::Erase(const Slice &key) {
  std::lock_guard<std::mutex> guard(latch_);
  const uint32_t hash = HashSlice(key);
  FinishErase(table_.Remove(key, hash));
}

void LRUCache::LRU_Remove(LRUNode *e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUNode *list, LRUNode *e) {
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

auto LRUCache::Lookup(const Slice &key) -> LRUNode * {
  std::lock_guard<std::mutex> guard(latch_);
  const uint32_t hash = HashSlice(key);
  LRUNode *e = table_.Lookup(key, hash);
  if (e != nullptr) {
    Pin(e);
  }
  return e;
}

void LRUCache::Release(LRUNode *node) {
  std::lock_guard<std::mutex> guard(latch_);
  UnPin(node);
}

auto LRUCache::Insert(const Slice &key, size_t charge, void *value)
    -> LRUNode * {
  std::lock_guard<std::mutex> guard(latch_);

  const uint32_t hash = HashSlice(key);
  LRUNode *e =
      reinterpret_cast<LRUNode *>(malloc(sizeof(LRUNode) + key.size()));
  e->value = value;
  e->key_length = key.size();
  e->hash = hash;
  e->charge = charge;
  e->in_cache = false;
  e->refs = 1;
  std::memcpy(e->key_data, key.data(), key.size());

  if (capacity_ > 0) {
    e->refs++;
    e->in_cache = true;
    LRU_Append(&in_use_, e);
    usage_ += charge;
    FinishErase(table_.Insert(e));
  }

  while (usage_ > capacity_ && lru_.next != &lru_) {
    LRUNode *old = lru_.next;
    FinishErase(table_.Remove(old->key(), old->hash));
  }

  return e;
}
}  // namespace Tskydb
