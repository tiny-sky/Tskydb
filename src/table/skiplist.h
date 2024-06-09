#pragma once

#include <assert.h>
#include <atomic>

#include "arena.h"
#include "util/macros.h"
#include "util/random.h"

namespace Tskydb {

// Format of an Key is concatenation of:
//  key_size     : varint32 of internal_key.size()
//  key bytes    : char[internal_key.size()]
//  tag          : uint64((sequence << 8) | type)
//  value_size   : varint32 of value.size()
//  value bytes  : char[value.size()]

template <typename Key, class Comparator>
struct SkipNode {
  explicit SkipNode(const Key &k) : key(k) {}

  const Key key;

  auto Next(int n) const -> SkipNode * {
    return next_[n].load(std::memory_order_acquire);
  }

  void SetNext(int n, SkipNode *x) {
    next_[n].store(x, std::memory_order_release);
  }

  // No-barrier variants that can be safely used in a few locations.
  SkipNode *NoBarrier_Next(int n) {
    assert(n >= 0);
    return next_[n].load(std::memory_order_relaxed);
  }
  void NoBarrier_SetNext(int n, SkipNode *x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_relaxed);
  }

 private:
  std::atomic<SkipNode *> next_[0];
};

template <typename Key, class Comparator>
class SkipList {
  using SkipNode = SkipNode<Key, Comparator>;

 public:
  explicit SkipList(Comparator cmp, Arena *arena);

  DISALLOW_COPY_AND_MOVE(SkipList);

  // Insert key into the list.
  void Insert(const Key &key);

  // Find key in SkipList
  auto lookup(const Key &key) const -> bool;

  class Iterator {
   public:
    inline explicit Iterator(const SkipList *list) {
      list_ = list;
      node_ = nullptr;
    }

    // Returns true iff the iterator is positioned at a valid node.
    inline auto Valid() const -> bool { return node_ != nullptr; }

    // Returns the key at the current position.
    inline auto key() const -> const Key & { return node_->key; }

    // Advances to the next position.
    inline void Next() { node_ = node_->Next(0); }

    inline void Prev() {
      assert(Valid());
      node_ = list_->FindLessThan(node_->key);
      if (node_ == list_->head_) {
        node_ = nullptr;
      }
    }

    inline void Seek(const Key &target) {
      node_ = list_->FindGreaterOrEqual(target, nullptr);
    }

    inline void SeekToFirst() { node_ = list_->head_->Next(0); }

    inline void SeekToLast() {
      node_ = list_->FindLast();
      if (node_ == list_->head_) {
        node_ = nullptr;
      }
    }

   private:
    const SkipList *list_;
    SkipNode *node_;
  };

 private:
  const static int MaxHeight = 12;

  inline int GetMaxHeight() const {
    return max_height_.load(std::memory_order_relaxed);
  }

  auto FindGreaterOrEqual(const Key &key, SkipNode **prev) const -> SkipNode *;

  auto FindLast() const -> SkipNode *;

  auto FindLessThan(const Key &key) const -> SkipNode *;

  auto NewNode(const Key &key, int height) -> SkipNode *;

  auto RandomHeight() -> int;

  Comparator const compare_;

  // Arena used for allocations of nodes
  Arena *const arena_;

  SkipNode *const head_;

  // Height of the entire list
  std::atomic<int> max_height_;

  Tskydb::Random rnd_;
};

}  // namespace Tskydb
