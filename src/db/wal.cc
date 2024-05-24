#include "wal.h"

#include <assert.h>

#include "util/crc32c.h"
#include "util/encoding.h"
#include "writablefile.h"

namespace Tskydb {

static void InitTypeCrc(uint32_t *type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Wal::Wal(WritableFile *dest) : dest_(dest), block_offset_(0) {
  InitTypeCrc(type_crc_);
}

Wal::Wal(WritableFile *dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}

Wal::~Wal() = default;

Status Wal::AddRecord(const Slice &slice) {
  const char *ptr = slice.data();
  size_t left = slice.size();

  Status s;
  bool begin = true;
  do {
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    if (leftover < kHeaderSize) {
      if (leftover > 0) {
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      block_offset_ = 0;
    }

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = kFullType;
    } else if (begin) {
      type = kFirstType;
    } else if (end) {
      type = kLastType;
    } else {
      type = kMiddleType;
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.Isok() && left > 0);
  return s;
}

Status Wal::EmitPhysicalRecord(RecordType t, const char *ptr, size_t length) {
  char buf[kHeaderSize];
  buf[4] = static_cast<char>(length & 0xff);
  buf[5] = static_cast<char>(length >> 8);
  buf[6] = static_cast<char>(t);

  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
  crc = crc32c::Mask(crc);
  EncodeFixed32(buf, crc);

  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.Isok()) {
    s = dest_->Append(Slice(ptr, length));
    if (s.Isok()) {
      s = dest_->Flush();
    }
  }
  block_offset_ += kHeaderSize + length;
  return s;
}
}  // namespace Tskydb
