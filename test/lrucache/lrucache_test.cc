#include <gtest/gtest.h>

#include "lrucache/lrucache.h"
#include "util/encoding.h"

namespace Tskydb {

static std::string EncodeKey(int k) {
  std::string result;
  PutFixed32(&result, k);
  return result;
}
static void *EncodeValue(uintptr_t v) { return reinterpret_cast<void *>(v); }
static int DecodeValue(void *v) { return reinterpret_cast<uintptr_t>(v); }

class LRUCacheTest : public testing::Test {
 public:
  static constexpr int CacheSize = 1000;
  LRUCache *cache_;

  LRUCacheTest() { cache_ = new LRUCache(CacheSize); }

  ~LRUCacheTest() { delete cache_; }

  int Lookup(int key) {
    LRUNode *node = cache_->Lookup(EncodeKey(key));
    const int r = (node == nullptr) ? -1 : DecodeValue(node->value);
    if (node != nullptr) {
      cache_->Release(node);
    }
    return r;
  }

  void Insert(int key, int value, int charge = 1) {
    cache_->Release(cache_->Insert(EncodeKey(key), charge, EncodeValue(value)));
  }

  auto InsertAndReturnHandle(int key, int value, int charge = 1) {
    return cache_->Insert(EncodeKey(key), charge, EncodeValue(value));
  }

  void Erase(int key) { cache_->Erase(EncodeKey(key)); }
};

TEST_F(LRUCacheTest, simple) {
  ASSERT_EQ(-1, Lookup(100));

  Insert(100, 101);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(-1, Lookup(200));
  ASSERT_EQ(-1, Lookup(300));

  Insert(200, 201);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1, Lookup(300));

  Insert(100, 102);
  ASSERT_EQ(102, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1, Lookup(300));
}

TEST_F(LRUCacheTest, Erase) {
  Erase(200);

  Insert(100, 101);
  Insert(200, 201);
  Erase(100);
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_EQ(201, Lookup(200));

  Erase(100);
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
}

TEST_F(LRUCacheTest, EntriesArePinned) {
  Insert(100, 101);
  auto h1 = cache_->Lookup(EncodeKey(100));
  ASSERT_EQ(2, h1->refs);
  ASSERT_EQ(101, DecodeValue(h1->value));

  Insert(100, 102);
  auto h2 = cache_->Lookup(EncodeKey(100));
  ASSERT_EQ(2, h2->refs);
  ASSERT_EQ(102, DecodeValue(h2->value));

  cache_->Release(h1);

  cache_->Release(h2);

  Erase(100);
  ASSERT_EQ(-1, Lookup(100));
}

TEST_F(LRUCacheTest, EvictionPolicy) {
  Insert(100, 101);
  Insert(200, 201);
  Insert(300, 301);
  auto *h = cache_->Lookup(EncodeKey(300));

  for (int i = 0; i < CacheSize + 100; i++) {
    Insert(1000 + i, 2000 + i);
    ASSERT_EQ(2000 + i, Lookup(1000 + i));
    ASSERT_EQ(101, Lookup(100));
  }
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(-1, Lookup(200));
  ASSERT_EQ(301, Lookup(300));
  cache_->Release(h);
}

TEST_F(LRUCacheTest, UseExceedsCacheSize) {
  std::vector<LRUNode*> h;
  for (int i = 0; i < CacheSize + 100; i++) {
    h.push_back(InsertAndReturnHandle(1000 + i, 2000 + i));
  }

  // Check that all the entries can be found in the cache.
  for (int i = 0; i < static_cast<int>(h.size()); i++) {
    ASSERT_EQ(2000 + i, Lookup(1000 + i));
  }

  for (int i = 0; i < static_cast<int>(h.size()); i++) {
    cache_->Release(h[i]);
  }
}

TEST_F(LRUCacheTest, HeavyEntries) {
  // Add a bunch of light and heavy entries and then count the combined
  // size of items still in the cache, which must be approximately the
  // same as the total capacity.
  const int kLight = 1;
  const int kHeavy = 10;
  int added = 0;
  int index = 0;
  while (added < 2 * CacheSize) {
    const int weight = (index & 1) ? kLight : kHeavy;
    Insert(index, 1000 + index, weight);
    added += weight;
    index++;
  }

  int cached_weight = 0;
  for (int i = 0; i < index; i++) {
    const int weight = (i & 1 ? kLight : kHeavy);
    int r = Lookup(i);
    if (r >= 0) {
      cached_weight += weight;
      ASSERT_EQ(1000 + i, r);
    }
  }
  ASSERT_LE(cached_weight, CacheSize + CacheSize / 10);
}

}  // namespace Tskydb
