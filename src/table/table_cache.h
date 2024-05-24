#pragma once

#include <string>
#include <cstdint>

#include "options.h"
#include "util/macros.h"

namespace Tskydb {

class TableCache {
  TableCache(const std::string &dbname, const Options &options, int enties);

  DISALLOW_COPY(TableCache);

  ~TableCache();

  // Return an iterator for the specified file number (the corresponding
  // file length must be exactly "file_size" bytes).  If "tableptr" is
  // non-null, also sets "*tableptr" to point to the Table object
  // underlying the returned iterator, or to nullptr if no Table object
  // underlies the returned iterator.  The returned "*tableptr" object is owned
  // by the cache and should not be deleted, and is valid for as long as the
  // returned iterator is live.
  Iterator *NewIterator(const ReadOptions &options, uint64_t file_number,
                        uint64_t file_size, Table **tableptr = nullptr);
};
}  // namespace Tskydb
