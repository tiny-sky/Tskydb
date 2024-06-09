#pragma once

#include <unistd.h>

#include <cstdint>
#include <string>

#include "slice.h"

namespace Tskydb {

enum class Endian {
  LITTLE = __ORDER_LITTLE_ENDIAN__,
  BIG = __ORDER_BIG_ENDIAN__,
  NATIVE = __BYTE_ORDER__,
};

// Determine the size segment used by this machine

constexpr inline bool IsLittleEndian() {
  return Endian::NATIVE == Endian::LITTLE;
}

constexpr inline bool IsBigEndian() { return Endian::NATIVE == Endian::BIG; }

constexpr inline uint32_t BitSwap(uint32_t x) { return __builtin_bswap32(x); }

constexpr inline uint64_t BitSwap(uint64_t x) { return __builtin_bswap64(x); }

// perform data compression
template <typename T>
constexpr char *EncodeFixed(char *buf, T value) {
  if constexpr (IsLittleEndian()) {
    value = BitSwap(value);
  }
  __builtin_memcpy(buf, &value, sizeof(value));
  return buf + sizeof(value);
}

inline char *EncodeFixed32(char *buf, uint32_t value) {
  return EncodeFixed<uint32_t>(buf, value);
}
inline char *EncodeFixed64(char *buf, uint64_t value) {
  return EncodeFixed<uint64_t>(buf, value);
}

// Decompress data
template <typename T>
constexpr T DecodeFixed(const char *ptr) {
  T value = 0;

  __builtin_memcpy(&value, ptr, sizeof(value));

  return IsLittleEndian() ? BitSwap(value) : value;
}

inline uint32_t DecodeFixed32(const char *ptr) {
  return DecodeFixed<uint32_t>(ptr);
}
inline uint64_t DecodeFixed64(const char *ptr) {
  return DecodeFixed<uint64_t>(ptr);
}

template <typename T>
void PutFixed(std::string *dst, T value) {
  char buf[sizeof(value)];
  EncodeFixed(buf, value);
  dst->append(buf, sizeof(buf));
}

inline void PutFixed32(std::string *dst, uint32_t value) {
  PutFixed<uint32_t>(dst, value);
}
inline void PutFixed64(std::string *dst, uint64_t value) {
  PutFixed<uint64_t>(dst, value);
}

template <typename T>
bool GetFixed(Slice *input, T *value) {
  if (input.size() < sizeof(T)) return false;
  *value = DecodeFixed<T>(input.data());
  input->remove_prefixe(sizeof(T));
  return true;
}

inline bool GetFixed32(Slice *input, uint32_t *value) {
  return GetFixed<uint32_t>(input, value);
}
inline bool GetFixed64(Slice *input, uint64_t *value) {
  return GetFixed<uint64_t>(input, value);
}

char *EncodeVarint32(char *dst, uint32_t v);

void PutVarint32(std::string *dst, uint32_t v);
bool GetVarint32(Slice *input, uint32_t *value);

int VarintLength(uint64_t v);
const char *GetVarint32Ptr(const char *p, const char *limit, uint32_t *value);
const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* v);

bool GetLengthPrefixedSlice(Slice* input, Slice* result);
void PutLengthPrefixedSlice(std::string* dst, const Slice& value);

}  // namespace Tskydb
