#pragma once

#include "skiplist.h"
#include "util/macros.h"
#include "db/keyformat.h"

namespace Tskydb {

class InternalKeyComparator;

class MemTable {
 public:
  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  explicit MemTable(const InternalKeyComparator &comparator);

  DISALLOW_COPY_AND_MOVE(MemTable);

  // Increase reference count.
  void Ref() { ++refs_; }

  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/format.{h,cc} module.
  Iterator *NewIterator();

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  void Add(uint64_t seq, ValueType type, const Slice &key,
           const Slice &value);

  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
  bool Get(const LookupKey &key, std::string *value, Status *s);

 private:
  friend class MemTableIterator;

  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator &c) : comparator(c) {}
    int operator()(const char *a, const char *b) const;
  };

  typedef SkipList<const char *, KeyComparator> Table;

  ~MemTable();

  KeyComparator comparator_;
  int refs_;
  Arena arena_;
  Table table_;
};
}  // namespace Tskydb
