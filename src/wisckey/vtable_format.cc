#include "vtable_format.h"
#include "table_builder.h"

#include "util/crc32c.h"
#include "util/encoding.h"

namespace Tskydb {

namespace wisckey {

bool GetChar(Slice *src, unsigned char *value) {
  if (src->size() < 1) return false;
  *value = *src->data();
  src->remove_prefix(1);
  return true;
}

void VRecord::EncodeTo(std::string *dst) const {
  PutLengthPrefixedSlice(dst, key);
  PutLengthPrefixedSlice(dst, value);
}

Status VRecord::DecodeFrom(Slice *src) {
  if (!GetLengthPrefixedSlice(src, &key) ||
      !GetLengthPrefixedSlice(src, &value)) {
    return Status::Corruption("BlobRecord");
  }
  return Status::OK();
}

bool operator==(const VRecord &lhs, const VRecord &rhs) {
  return lhs.key == rhs.key && lhs.value == rhs.value;
}

void Encoder::EncodeRecord(const VRecord &record) {
  record_buffer_.clear();
  compressed_buffer_.clear();

  record.EncodeTo(&record_buffer_);
  compressor_.Compress(record_buffer_, &compressed_buffer_);

  EncodeFixed32(header_ + 4, static_cast<uint32_t>(compressed_buffer_.size()));
  header_[8] = compressor_.GetType();

  uint32_t crc = leveldb::crc32c::Value(header_ + 4, sizeof(header_) - 4);
  crc = leveldb::crc32c::Extend(crc, compressed_buffer_.data(),
                                compressed_buffer_.size());
  EncodeFixed32(header_, crc);
}

Status Decoder::DecodeHeader(Slice *src) {
  if (!GetFixed32(src, &crc_)) {
    return Status::Corruption("BlobHeader");
  }
  header_crc_ = leveldb::crc32c::Value(src->data(), kBlobHeaderSize - 4);

  unsigned char compression;
  if (!GetFixed32(src, &record_size_) || !GetChar(src, &compression)) {
    return Status::Corruption("BlobHeader");
  }
  compression_ = static_cast<CompressionType>(compression);

  return Status::OK();
}

Status Decoder::DecodeRecord(Slice *src, VRecord *record, std::string *buffer) {
  Slice input(src->data(), record_size_);
  src->remove_prefix(record_size_);
  uint32_t crc =
      leveldb::crc32c::Extend(header_crc_, input.data(), input.size());
  if (crc != crc_) {
    return Status::Corruption("BlobRecord", "checksum mismatch");
  }

  if (compression_ == kNoCompression) {
    return DecodeInto(input, record);
  }

  compressor_.Decompress(input.ToString(), buffer);

  return DecodeInto(*buffer, record);
}

void VHandle::EncodeTo(std::string *dst) const {
  PutVarint64(dst, offset);
  PutVarint64(dst, size);
}

Status VHandle::DecodeFrom(Slice *src) {
  if (!GetVarint64(src, &offset) || !GetVarint64(src, &size)) {
    return Status::Corruption("BlobHandle");
  }
  return Status::OK();
}

bool operator==(const VHandle &lhs, const VHandle &rhs) {
  return lhs.offset == rhs.offset && lhs.size == rhs.size;
}

void VIndex::EncodeTo(std::string *dst) const {
  dst->push_back(kBlobRecord);
  PutVarint64(dst, file_number);
  Vtable_handle.EncodeTo(dst);
}

Status VIndex::DecodeFrom(Slice *src) {
  unsigned char type;
  if (!GetChar(src, &type) || type != kBlobRecord ||
      !GetVarint64(src, &file_number)) {
    return Status::Corruption("Index");
  }
  Status s = Vtable_handle.DecodeFrom(src);
  if (!s.ok()) {
    return Status::Corruption("Index", s.ToString());
  }
  return s;
}

bool operator==(const VIndex &lhs, const VIndex &rhs) {
  return (lhs.file_number == rhs.file_number &&
          lhs.Vtable_handle == rhs.Vtable_handle);
}

void VFileMeta::EncodeTo(std::string *dst) const {
  PutVarint64(dst, file_number_);
  PutVarint64(dst, file_size_);
}

Status VFileMeta::DecodeFrom(Slice *src) {
  if (!GetVarint64(src, &file_number_) || !GetVarint64(src, &file_size_)) {
    return Status::Corruption("FileMeta Decode failed");
  }
  return Status::OK();
}

bool operator==(const VFileMeta &lhs, const VFileMeta &rhs) {
  return (lhs.file_number_ == rhs.file_number_ &&
          lhs.file_size_ == rhs.file_size_);
}

void VFileMeta::AddDiscardableSize(uint64_t _discardable_size) {
  assert(_discardable_size < file_size_);
  discardable_size_ += _discardable_size;
  assert(discardable_size_ < file_size_);
}

double VFileMeta::GetDiscardableRatio() const {
  return static_cast<double>(discardable_size_) /
         static_cast<double>(file_size_);
}

void VFileMeta::FileStateTransit(const FileEvent &event) {
  switch (event) {
    case FileEvent::kFlushCompleted:
      assert(state_ == FileState::kPendingLSM ||
             state_ == FileState::kPendingGC || state_ == FileState::kNormal ||
             state_ == FileState::kBeingGC);
      if (state_ == FileState::kPendingLSM) state_ = FileState::kNormal;
      break;
    case FileEvent::kGCCompleted:
      assert(state_ == FileState::kPendingGC || state_ == FileState::kBeingGC);
      state_ = FileState::kNormal;
      break;
    case FileEvent::kCompactionCompleted:
      assert(state_ == FileState::kPendingLSM);
      state_ = FileState::kNormal;
      break;
    case FileEvent::kGCBegin:
      assert(state_ == FileState::kNormal);
      state_ = FileState::kBeingGC;
      break;
    case FileEvent::kGCOutput:
      assert(state_ == FileState::kInit);
      state_ = FileState::kPendingGC;
      break;
    case FileEvent::kFlushOrCompactionOutput:
      assert(state_ == FileState::kInit);
      state_ = FileState::kPendingLSM;
      break;
    case FileEvent::kDbRestart:
      assert(state_ == FileState::kInit);
      state_ = FileState::kNormal;
      break;
    default:
      fprintf(stderr,
              "Unknown file event[%d], file number[%lu], file state[%d]",
              static_cast<int>(event), static_cast<std::size_t>(file_number_),
              static_cast<int>(state_));
      abort();
  }
}

void VFileFooter::EncodeTo(std::string *dst) const {
  auto size = dst->size();
  meta_index_handle.EncodeTo(dst);
  // Add padding to make a fixed size footer.
  dst->resize(size + kEncodedLength - 12);
  PutFixed64(dst, kMagicNumber);
  Slice encoded(dst->data() + size, dst->size() - size);
  PutFixed32(dst, leveldb::crc32c::Value(encoded.data(), encoded.size()));
}

Status VFileFooter::DecodeFrom(Slice *src) {
  auto data = src->data();
  Status s = meta_index_handle.DecodeFrom(src);
  if (!s.ok()) {
    return Status::Corruption("FileFooter", s.ToString());
  }
  // Remove padding.
  src->remove_prefix(data + kEncodedLength - 12 - src->data());
  uint64_t magic_number = 0;
  if (!GetFixed64(src, &magic_number) || magic_number != kMagicNumber) {
    return Status::Corruption("FileFooter", "magic number");
  }
  Slice decoded(data, src->data() - data);
  uint32_t checksum = 0;
  if (!GetFixed32(src, &checksum) ||
      leveldb::crc32c::Value(decoded.data(), decoded.size()) != checksum) {
    return Status::Corruption("FileFooter", "checksum");
  }
  return Status::OK();
}

bool operator==(const VFileFooter &lhs, const VFileFooter &rhs) {
  return (lhs.meta_index_handle.offset() == rhs.meta_index_handle.offset() &&
          lhs.meta_index_handle.size() == rhs.meta_index_handle.size());
}

}  // namespace wisckey

}  // namespace Tskydb
