#pragma once

#include <cstdint>

#include "table/memtable.h"
#include "util/slice.h"
#include "util/status.h"

namespace Tskydb {

class Slice;
class MenTable;

class WriteBatch {
 public:
  class Handler {
   public:
    virtual ~Handler();
    virtual void Put(const Slice &key, const Slice &value) = 0;
    virtual void Delete(const Slice &key) = 0;
  };

  WriteBatch();

  // Intentionally copyable.
  WriteBatch(const WriteBatch &) = default;
  WriteBatch &operator=(const WriteBatch &) = default;

  ~WriteBatch();

  // Store the mapping "key->value" in the database.
  void Put(const Slice &key, const Slice &value);

  // If the database contains a mapping for "key", erase it.  Else do nothing.
  void Delete(const Slice &key);

  // Clear all updates buffered in this batch.
  void Clear();

  // The size of the database changes caused by this batch.
  //
  // This number is tied to implementation details, and may change across
  // releases. It is intended for LevelDB usage metrics.
  size_t ApproximateSize() const;

  // Copies the operations in "source" to this batch.
  //
  // This runs in O(source size) time. However, the constant factor is better
  // than calling Iterate() over the source batch with a Handler that replicates
  // the operations into this batch.
  void Append(const WriteBatch &source);

  // Support for iterating over the contents of a batch.
  Status Iterate(Handler *handler) const;

 private:
  friend class WriteBatchInternal;

 /*
  * | sequence_number |    count     |                   data                            |
  * |    fixed64      |   fixed32    |  ValueType  | key_size | key | value_size | value |
  */
  std::string rep_;
};

// WriteBatchInternal provides static methods for manipulating a
// WriteBatch that we don't want in the public WriteBatch interface.
class WriteBatchInternal {
 public:
  // Return the number of entries in the batch.
  static int Count(const WriteBatch *batch);

  // Set the count for the number of entries in the batch.
  static void SetCount(WriteBatch *batch, int n);

  // Return the sequence number for the start of this batch.
  static SequenceNumber Sequence(const WriteBatch *batch);

  // Store the specified number as the sequence number for the start of
  // this batch.
  static void SetSequence(WriteBatch *batch, SequenceNumber seq);

  static Slice Contents(const WriteBatch *batch) { return Slice(batch->rep_); }

  static size_t ByteSize(const WriteBatch *batch) { return batch->rep_.size(); }

  static void SetContents(WriteBatch *batch, const Slice &contents);

  static Status InsertInto(const WriteBatch *batch, MemTable *memtable);

  static void Append(WriteBatch *dst, const WriteBatch *src);
};
}  // namespace Tskydb
