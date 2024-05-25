#include "table_cache.h"

#include "util/encoding.h"

namespace Tskydb {

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf,sizeof(buf)));
}
}  // namespace Tskydb
