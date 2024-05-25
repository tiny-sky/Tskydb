#pragma once

#include <cstddef>

#include "env.h"
#include "util/format.h"
#include "util/macros.h"
#include "util/slice.h"
#include "util/status.h"
#include "util/crc32c.h"

namespace Tskydb {

namespace crc32c = leveldb::crc32c;

using namespace Tskydb::log;

class Wal {

 public:
  explicit Wal(WritableFile *dest);

  Wal(WritableFile *dest, uint64_t dest_length);

  DISALLOW_COPY(Wal);

  ~Wal();

  Status AddRecord(const Slice &slice);

 private:
  Status EmitPhysicalRecord(RecordType type, const char *ptr, size_t length);

  WritableFile *dest_;
  int block_offset_;

  uint32_t type_crc_[kMaxRecordType + 1];
};

}  // namespace Tskydb
