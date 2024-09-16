#include "compressor.h"

#include <snappy.h>
#include <zstd.h>
#include <cassert>
#include <string>

namespace Tskydb {

// Snappy 压缩实现
bool Compressor::CompressSnappy(const std::string &input, std::string *output) {
  size_t length = input.size();
  output->resize(snappy::MaxCompressedLength(length));
  size_t outlen;
  snappy::RawCompress(input.data(), length, &(*output)[0], &outlen);
  output->resize(outlen);
  return true;
}

// Snappy 解压实现
bool Compressor::DecompressSnappy(const std::string &input,
                                  std::string *output) {
  size_t uncompressed_length;
  if (!snappy::GetUncompressedLength(input.data(), input.size(),
                                     &uncompressed_length)) {
    return false;
  }
  output->resize(uncompressed_length);
  return snappy::RawUncompress(input.data(), input.size(), &(*output)[0]);
}

// ZSTD 压缩实现
bool Compressor::CompressZSTD(const std::string &input, std::string *output) {
  size_t compressed_size = ZSTD_compressBound(input.size());
  output->resize(compressed_size);
  size_t outlen = ZSTD_compress(&(*output)[0], compressed_size, input.data(),
                                input.size(), 1);
  if (ZSTD_isError(outlen)) {
    return false;
  }
  output->resize(outlen);
  return true;
}

// ZSTD 解压实现
bool Compressor::DecompressZSTD(const std::string &input, std::string *output) {
  unsigned long long uncompressed_size =
      ZSTD_getFrameContentSize(input.data(), input.size());
  if (uncompressed_size == ZSTD_CONTENTSIZE_ERROR ||
      uncompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
    return false;
  }
  output->resize(uncompressed_size);
  size_t result = ZSTD_decompress(&(*output)[0], uncompressed_size,
                                  input.data(), input.size());
  if (ZSTD_isError(result)) {
    return false;
  }
  return true;
}
}  // namespace Tskydb
