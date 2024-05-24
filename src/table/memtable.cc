#pragma once

#include <string>

#include "memtable.h"
#include "common/iterator.h"
#include "util/slice.h"
#include "util/encoding.h"

namespace Tskydb {
class MemTableIterator : public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table *table) : iter_(table) {}

  MemTableIterator(const MemTableIterator &) = delete;
  MemTableIterator &operator=(const MemTableIterator &) = delete;

  ~MemTableIterator() override = default;

  bool Valid() const override { return iter_.Valid(); }
  void Seek(const Slice &k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override { iter_.Next(); }
  void Prev() override { iter_.Prev(); }
  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
  Slice value() const override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  Status status() const override { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;  // For passing to EncodeKey
};

}  // namespace Tskydb
