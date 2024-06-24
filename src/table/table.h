#pragma once

#include <cstdint>

#include "common/iterator.h"
#include "db/options.h"
#include "env.h"
#include "util/macros.h"
#include "util/status.h"

namespace Tskydb {

class Footer;

class Table {
 public:
  // Attempt to open the table that is stored in bytes [0..file_size)
  // of "file", and read the metadata entries necessary to allow
  // retrieving data from the table.
  //
  // If successful, returns ok and sets "*table" to the newly opened
  // table.  The client should delete "*table" when no longer needed.
  // If there was an error while initializing the table, sets "*table"
  // to nullptr and returns a non-ok status.  Does not take ownership of
  // "*source", but the client must ensure that "source" remains live
  // for the duration of the returned table's lifetime.
  //
  // *file must remain live while this Table is in use.
  static Status Open(const Options &options, RandomAccessFile *file,
                     uint64_t file_size, Table **table);

  DISALLOW_COPY(Table);

  ~Table();

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  Iterator *NewIterator(const ReadOptions &) const;

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file).  The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  uint64_t ApproximateOffsetOf(const Slice &key) const;

 private:
  friend class TableCache;
  struct Rep;

  static Iterator *BlockReader(void *, const ReadOptions &, const Slice &);

  explicit Table(Rep *rep) : rep_(rep) {}

  // Calls (*handle_result)(arg, ...) with the entry found after a call
  // to Seek(key).  May not make such a call if filter policy says
  // that key is not present.
  Status InternalGet(const ReadOptions &options, const Slice &k, void *arg,
                     void (*handle_result)(void *, const Slice &,
                                           const Slice &));

  void ReadMeta(const Footer &footer);
  void ReadFilter(const Slice &filter_handle_value);

  Rep *const rep_;
};
}  // namespace Tskydb
