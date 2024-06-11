#pragma once

#include "common/filter_policy.h"

#include "util/hash.h"
#include "util/slice.h"

#include <string>

namespace Tskydb {
static uint32_t BloomHash(const Slice &key) {
  return Hash(key.data(), key.size(), 0xbc9f1d34);
}

class BloomFilter : public FilterPolicy {
 public:
  explicit BloomFilter(int bits_per_key) : bits_per_key_(bits_per_key) {
    k_ = static_cast<size_t>(bits_per_key_ * 0.69);  // K = In2 * (m/n)
    if (k_ < 1) k_ = 1;
    if (k_ > 30) k_ = 30;
  }

  void CreateFilter(const Slice *key, int n, std::string *dst) const override {
    size_t bits = n * bits_per_key_;

    if (bits < 64) bits = 64;

    size_t bytes = (bits + 7) / 8;
    bits = bytes * 8;

    const size_t size = dst->size();
    dst->resize(size + bytes, 0);
    dst->push_back(static_cast<char>(k_));
    char *array = &(*dst)[size];

    for (int i = 0; i < n; i++) {
      uint32_t h = BloomHash(key[i]);
      const uint32_t delta = (h >> 17) | (h << 15);
      for (size_t j = 0; j < k_; j++) {
        const uint32_t bitpos = h % bits;
        array[bitpos / 8] |= (1 << (bitpos % 8));
        h += delta;
      }
    }
  }

  bool KeyMayMatch(const Slice &key, const Slice &bloom_filter) const override {
    const size_t len = bloom_filter.size();
    if (len < 2) return false;

    const char *array = bloom_filter.data();
    const size_t bits = (len - 1) * 8;

    const size_t k = array[len - 1];
    if (k > 30) {
      return true;
    }

    uint32_t h = BloomHash(key);
    const uint32_t delta = (h >> 17) | (h << 15);
    for (size_t j = 0; j < k; j++) {
      const uint32_t bitpos = h % bits;
      if ((array[bitpos / 8] & (1 << (bitpos % 8))) == 0) return false;
      h += delta;
    }
    return true;
  }

 private:
  size_t bits_per_key_;
  size_t k_;
};

const FilterPolicy *NewBloomFilterPolicy(int bits_per_key) {
  return new BloomFilter(bits_per_key);
}
}  // namespace Tskydb
