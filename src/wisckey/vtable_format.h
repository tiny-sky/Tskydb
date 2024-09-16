#pragma once

#include <cstdint>
#include <string>

#include <table/table_format.h>
#include <util/slice.h>
#include <util/status.h>
#include <util/compressor.h>

namespace Tskydb {

namespace wisckey {
enum class FileEvent {
  kInit,
  kFlushCompleted,
  kCompactionCompleted,
  kGCCompleted,
  kGCBegin,
  kGCOutput,
  kFlushOrCompactionOutput,
  kDbRestart,
};

enum class FileState {
  kInit,  // file never at this state
  kNormal,
  kPendingLSM,  // waiting keys adding to LSM
  kBeingGC,     // being gced
  kPendingGC,   // output of gc, waiting gc finish and keys adding to LSM
};

// vtable header format:
//
// crc          : fixed32
// size         : fixed32
// compression  : char
const uint64_t kHeaderSize = 9;

// vtable record format:
//
// key          : varint64 length + length bytes
// value        : varint64 length + length bytes
struct VRecord {
  Slice key;
  Slice value;

  void EncodeTo(std::string *dst) const;
  Status DecodeFrom(Slice *src);

  friend bool operator==(const VRecord &lhs, const VRecord &rhs);
};

class Encoder {
 public:
  Encoder(CompressionType type) : compressor_(type) {}

  void EncodeRecord(const VRecord &record);

  Slice GetHeader() const { return Slice(header_, sizeof(header_)); }
  Slice GetRecord() const { return record_; }

  size_t GetEncodedSize() const { return sizeof(header_) + record_.size(); }

 private:
  char header_[kHeaderSize];
  Slice record_;
  std::string record_buffer_;
  std::string compressed_buffer_;
  Compressor compressor_;
};

class Decoder {
 public:
  Decoder(CompressionType type) : compressor_(type) {}

  Status DecodeHeader(Slice *src);
  Status DecodeRecord(Slice *src, VRecord *record, std::string *buffer);

  size_t GetRecordSize() const { return record_size_; }

 private:
  uint32_t crc_{0};
  uint32_t header_crc_{0};
  uint32_t record_size_{0};
  CompressionType compression_{kNoCompression};
  Compressor compressor_;
};

// Vtable handle format:
//
// offset       : varint64
// size         : varint64
struct VHandle {
  uint64_t offset{0};
  uint64_t size{0};

  void EncodeTo(std::string *dst) const;
  Status DecodeFrom(Slice *src);

  friend bool operator==(const VHandle &lhs, const VHandle &rhs);
};

// Vtable index format:
//
// type         : char
// file_number_  : varint64
// blob_handle  : varint64 offset + varint64 size
struct VIndex {
  enum Type : unsigned char {
    kBlobRecord = 1,
  };
  uint64_t file_number{0};
  VHandle Vtable_handle;

  void EncodeTo(std::string *dst) const;
  Status DecodeFrom(Slice *src);

  friend bool operator==(const VIndex &lhs, const VIndex &rhs);
};

// Vtable file meta format:
//
// file_number_      : varint64
// file_size_        : varint64
class VFileMeta {
 public:
  VFileMeta() = default;
  VFileMeta(uint64_t _file_number, uint64_t _file_size)
      : file_number_(_file_number), file_size_(_file_size) {}

  friend bool operator==(const VFileMeta &lhs, const VFileMeta &rhs);

  void EncodeTo(std::string *dst) const;
  Status DecodeFrom(Slice *src);

  uint64_t file_number() const { return file_number_; }
  uint64_t file_size() const { return file_size_; }
  FileState file_state() const { return state_; }
  uint64_t discardable_size() const { return discardable_size_; }

  void FileStateTransit(const FileEvent &event);

  void AddDiscardableSize(uint64_t _discardable_size);
  double GetDiscardableRatio() const;

 private:
  // Persistent field
  uint64_t file_number_{0};
  uint64_t file_size_{0};

  // Not persistent field
  FileState state_{FileState::kInit};

  uint64_t discardable_size_{0};
};

// Vtable file footer format:
//
// meta_index_handle    : varint64 offset + varint64 size
// <padding>            : [... kEncodedLength - 12] bytes
// magic_number         : fixed64
// checksum             : fixed32
struct VFileFooter {
  static const uint64_t kMagicNumber{0x2045346560706835ull};
  static const uint64_t kEncodedLength{BlockHandle::kMaxEncodedLength + 8 + 4};

  BlockHandle meta_index_handle{BlockHandle::NullBlockHandle()};

  void EncodeTo(std::string *dst) const;
  Status DecodeFrom(Slice *src);

  friend bool operator==(const VFileFooter &lhs,
                         const VFileFooter &rhs);
};

// A convenient template to decode a const slice.
template <typename T>
Status DecodeInto(const Slice &src, T *target) {
  auto tmp = src;
  auto s = target->DecodeFrom(&tmp);
  if (s.ok() && !tmp.empty()) {
    s = Status::Corruption(Slice());
  }
  return s;
}

}  // namespace wisckey

}  // namespace Tskydb
