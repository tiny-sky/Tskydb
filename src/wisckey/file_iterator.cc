#include "file_iterator.h"

#include "db/env.h"
#include "util/macros.h"
#include "vtable_format.h"

namespace Tskydb {
namespace wisckey {

VFileIterator::VFileIterator(std::unique_ptr<RandomAccessFile> &&file,
                             uint64_t file_name, uint64_t file_size,
                             Options *options)
    : file_(std::move(file)),
      file_number_(file_name),
      file_size_(file_size),
      options_(options),
      decoder_(options_->compression) {}

VFileIterator::~VFileIterator() {}

Status VFileIterator::status() const { return status_; }
Slice VFileIterator::key() const { return cur_record_.key; }
Slice VFileIterator::value() const { return cur_record_.value; }

bool VFileIterator::Valid() const { valid_ &&status_.ok(); }

bool VFileIterator::Init() {
  char buf[VFileFooter::kEncodedLength];
  Slice slice;
  status_ = file_->Read(file_size_ - VFileFooter::kEncodedLength,
                        VFileFooter::kEncodedLength, &slice, buf);
  if (!status_.ok()) return false;
  VFileFooter file_footer;
  status_ = file_footer.DecodeFrom(&slice);
  total_blocks_size_ = file_size_ - VFileFooter::kEncodedLength -
                       file_footer.meta_index_handle.size();
  init_ = true;
  return true;
}

void VFileIterator::Next() {
  assert(init_);
  GetVtableRecord();
}

void VFileIterator::SeekToFirst() {
  if (!init_ && !Init()) return;
  status_ = Status::OK();
  iterate_offset_ = 0;
  GetVtableRecord();
}

void VFileIterator::IterateForPrev(uint64_t offset) {
  if (!init_ && !Init()) return;

  status_ = Status::OK();

  if (offset >= total_blocks_size_) {
    iterate_offset_ = offset;
    status_ = Status::InvalidArgument("Out of bound");
    return;
  }

  uint64_t total_length = 0;
  Slice header_buffer;
  char buffer[kHeaderSize];
  bool found = false;
  for (iterate_offset_ = 0; iterate_offset_ < offset;
       iterate_offset_ += total_length) {
    status_ = file_->Read(iterate_offset_, kHeaderSize, &header_buffer, buffer);
    if (!status_.ok()) {
      valid_ = false;
      return;
    }
    status_ = decoder_.DecodeHeader(&header_buffer);
    if (!status_.ok()) {
      valid_ = false;
      return;
    }
    total_length = kHeaderSize + decoder_.GetRecordSize();
    if (iterate_offset_ + total_length > offset) {
      found = true;
      break;
    }
  }

  if (found) {
    iterate_offset_ -= total_length;
    valid_ = true;
  } else {
    valid_ = false;
  }
}

void VFileIterator::GetVtableRecord() {
  if (iterate_offset_ >= total_blocks_size_) {
    valid_ = false;
    return;
  }

  Slice header_buffer;
  char buffer[kHeaderSize];
  status_ = file_->Read(iterate_offset_, kHeaderSize, &header_buffer, buffer);

  if (!status_.ok()) return;
  status_ = decoder_.DecodeHeader(&header_buffer);
  if (!status_.ok()) return;

  Slice record_slice;
  auto record_size = decoder_.GetRecordSize();
  buffer_.reserve(record_size);
  status_ = file_->Read(iterate_offset_ + kHeaderSize, record_size,
                        &record_slice, buffer_.data());
  if (status_.ok()) {
    status_ =
        decoder_.DecodeRecord(&record_slice, &cur_record_, &uncompressed_);
  }
  if (!status_.ok()) return;

  cur_record_offset_ = iterate_offset_;
  cur_record_size_ = kHeaderSize + record_size;
  iterate_offset_ += cur_record_size_;
  valid_ = true;
}

VIndex VFileIterator::GetVtableIndex() {
  VIndex index;
  index.file_number = file_number_;
  index.Vtable_handle.offset = cur_record_offset_;
  index.Vtable_handle.size = cur_record_size_;
  return index;
}

}  // namespace wisckey

}  // namespace Tskydb
