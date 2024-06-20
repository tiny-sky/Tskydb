#include <cstdint>

#include "encoding.h"

namespace Tskydb {

char *EncodeVarint32(char *dst, uint32_t v) {
  // Operate on characters as unsigneds
  auto *ptr = reinterpret_cast<unsigned char *>(dst);
  do {
    *ptr = 0x80 | v;
    v >>= 7, ++ptr;
  } while (v != 0);
  *(ptr - 1) &= 0x7F;  // Remove the 0X80 flag
  return reinterpret_cast<char *>(ptr);
}

void PutVarint32(std::string *dst, uint32_t v) {
  char buf[5];
  char *ptr = EncodeVarint32(buf, v);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

char *EncodeVarint64(char *dst, uint64_t v) {
  static const int B = 128;
  uint8_t *ptr = reinterpret_cast<uint8_t *>(dst);
  while (v >= B) {
    *(ptr++) = v | B;
    v >>= 7;
  }
  *(ptr++) = static_cast<uint8_t>(v);
  return reinterpret_cast<char *>(ptr);
}

void PutVarint64(std::string *dst, uint64_t v) {
  char buf[10];
  char *ptr = EncodeVarint64(buf, v);
  dst->append(buf, ptr - buf);
}

void PutLengthPrefixedSlice(std::string *dst, const Slice &value) {
  PutVarint32(dst, value.size());
  dst->append(value.data(), value.size());
}

int VarintLength(uint64_t v) {
  int len = 1;
  while (v >= 128) {
    v >>= 7;
    len++;
  }
  return len;
}

const char *GetVarint32PtrFallback(const char *p, const char *limit,
                                   uint32_t *value) {
  uint32_t result = 0;
  for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
    uint32_t byte = static_cast<unsigned char>(*p);
    p++;
    if (byte & 0x80) {
      // More bytes are present
      result |= ((byte & 0x7F) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return p;
    }
  }
  return nullptr;
}

const char *GetVarint32Ptr(const char *p, const char *limit, uint32_t *value) {
  if (p < limit) {
    uint32_t result = static_cast<unsigned char>(*p);
    if ((result & 0x80) == 0) {
      *value = result;
      return p + 1;
    }
  }
  return GetVarint32PtrFallback(p, limit, value);
}

bool GetVarint32(Slice *input, uint32_t *value) {
  const char *p = input->data();
  const char *limit = p + input->size();
  const char *q = GetVarint32Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    *input = Slice(q, static_cast<size_t>(limit - q));
    return true;
  }
}

bool GetVarint64(Slice *input, uint64_t *value) {
  const char *p = input->data();
  const char *limit = p + input->size();
  const char *q = GetVarint64Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    *input = Slice(q, limit - q);
    return true;
  }
}

bool GetLengthPrefixedSlice(Slice *input, Slice *result) {
  uint32_t len;
  if (GetVarint32(input, &len) && input->size() >= len) {
    *result = Slice(input->data(), len);
    input->remove_prefix(len);
    return true;
  } else {
    return false;
  }
}
}  // namespace Tskydb
