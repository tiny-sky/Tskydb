#include <new>
#include <iostream>

#include "skiplist.h"

namespace Tskydb {

template <typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena *arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0, MaxHeight)),
      max_height_(1),
      rnd_(0xdeadbeef) {
  for (int i = 0; i < MaxHeight; i++) {
    head_->SetNext(i, nullptr);
  }
}

template <typename Key, class Comparator>
auto SkipList<Key, Comparator>::RandomHeight() -> int {
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < MaxHeight && rnd_.OneIn(kBranching)) {
    height++;
  }
  assert(height > 0);
  assert(height <= MaxHeight);
  return height;
}

template <typename Key, class Comparator>
auto SkipList<Key, Comparator>::NewNode(const Key &key, int height)
    -> SkipNode * {
  char *memory = arena_->AllocateAligned(
      sizeof(SkipNode) + sizeof(std::atomic<SkipNode *>) * (height));
  return new (memory) SkipNode(key);
}

template <typename Key, class Comparator>
auto SkipList<Key, Comparator>::lookup(const Key &key) const -> bool {
  SkipNode *node = FindGreaterOrEqual(key, nullptr);
  if (node != nullptr && (compare_(key, node->key) == 0)) {
    return true;
  }
  return false;
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key &key) {
  SkipNode *prev[MaxHeight];
  SkipNode *cur = FindGreaterOrEqual(key, prev);

  int height = RandomHeight();
  if (height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < height; i++) {
      prev[i] = head_;
    }
    max_height_.store(height, std::memory_order_relaxed);
  }

  cur = NewNode(key, height);
  for (int i = 0; i < height; i++) {
    cur->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    prev[i]->SetNext(i, cur);
  }
  
}


template <typename Key, class Comparator>
auto SkipList<Key, Comparator>::FindGreaterOrEqual(const Key &key,
                                                   SkipNode **prev) const
    -> SkipNode * {
  SkipNode *x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    SkipNode *next = x->Next(level);
    if ((next != nullptr) && (compare_(next->key, key) < 0)) {
      x = next;
    } else {
      if (prev != nullptr) prev[level] = x;
      if (level == 0) {
        return next;
      } else {
        level--;
      }
    }
  }
}

template <typename Key, class Comparator>
auto SkipList<Key, Comparator>::FindLast() const -> SkipNode * {
  SkipNode *x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    SkipNode *next = x->Next(level);
    if (next == nullptr) {
      if (level == 0) {
        return x;
      } else {
        level--;
      }
    } else {
      x = next;
    }
  }
}

template <typename Key, class Comparator>
auto SkipList<Key, Comparator>::FindLessThan(const Key &key) const
    -> SkipNode * {
  SkipNode *x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_(x->key, key) < 0);
    SkipNode *next = x->Next(level);
    if (next == nullptr || compare_(next->key, key) >= 0) {
      if (level == 0) {
        return x;
      } else {
        level--;
      }
    } else {
      x = next;
    }
  }
}

struct Comparator {
  int operator()(const unsigned long &a, const unsigned long &b) const {
    if (a < b) {
      return -1;
    } else if (a > b) {
      return +1;
    } else {
      return 0;
    }
  }
};

template class SkipList<unsigned long, Tskydb::Comparator>;

}  // namespace Tskydb
