#pragma once

#include "db/env.h"
#include "db/options.h"
#include "util/macros.h"
#include "vtable_format.h"

namespace Tskydb {
namespace wisckey {
class VFileIterator {
 public:
  const uint64_t kMinReadaheadSize = 4 << 10;
  const uint64_t kMaxReadaheadSize = 256 << 10;
  VFileIterator(std::unique_ptr<RandomAccessFile> &&file, uint64_t file_name,
                uint64_t file_size, Options *options);

  DISALLOW_COPY_AND_MOVE(VFileIterator);

  ~VFileIterator();

  Status status() const;
  Slice key() const;
  Slice value() const;

  bool Valid() const;

  bool Init();
  void Next();

  void SeekToFirst();

  void IterateForPrev(uint64_t offset);

  VIndex GetVtableIndex();

 private:
  void GetVtableRecord();

  // file info
  const std::unique_ptr<RandomAccessFile> file_;
  const uint64_t file_number_;
  const uint64_t file_size_;
  Options *options_;

  bool init_{false};
  uint64_t total_blocks_size_{0};

  // Iterator status
  Status status_;
  bool valid_{false};

  // currently record
  VRecord cur_record_;
  uint64_t cur_record_offset_;
  uint64_t cur_record_size_;

  Decoder decoder_;
  uint64_t iterate_offset_{0};
  std::vector<char> buffer_;
  std::string uncompressed_;
};
}  // namespace wisckey

}  // namespace Tskydb
