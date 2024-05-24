#pragma once

#include "table.h"

#include <memory>

#include "options.h"
#include "filter_block.h"
#include "util/status.h"
#include "env.h"

namespace Tskydb {

struct Table::Rep {
  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;

  std::unique_ptr<FilterBlockReader> filter;
};
}  // namespace Tskydb
