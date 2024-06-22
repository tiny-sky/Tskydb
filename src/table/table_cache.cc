#include "table_cache.h"

#include "common/iterator.h"
#include "filename.h"
#include "lrucache.h"
#include "options.h"
#include "util/encoding.h"

namespace Tskydb {

struct TableAndFile {
  RandomAccessFile *file;
  Table *table;
};

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

static void UnrefEntry(void *arg1, void *arg2) {
  LRUCache *cache = reinterpret_cast<LRUCache *>(arg1);
  LRUNode *h = reinterpret_cast<LRUNode *>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string &dbname, const Options &options,
                       int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(new LRUCache(entries)) {}

TableCache::~TableCache() { delete cache_; }

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             LRUCache::Handle **handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);

  // cache miss
  if (*handle == nullptr) {
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile *file = nullptr;
    Table *table = nullptr;
    s = env_->NewRandomAccessFile(fname, &file);
  }
}

Iterator *TableCache::NewIterator(const ReadOptions &options,
                                  uint64_t file_number, uint64_t file_size,
                                  Table **tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  LRUCache::Handle *handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table *table = reinterpret_cast<TableAndFile *>(cache_->Value(handle))->table;
  Iterator *result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);  // ?
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}
}  // namespace Tskydb
