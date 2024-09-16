#pragma once

#include <string>

namespace Tskydb {

enum CompressionType {
  // NOTE: do not change the values of existing entries, as these are
  // part of the persistent format on disk.
  kNoCompression = 0x0,
  kSnappyCompression = 0x1,
  kZstdCompression = 0x2,
};

class Compressor {
 public:
  Compressor(CompressionType type) : type_(type) {}

  // 压缩接口，返回压缩后的数据
  bool Compress(const std::string &input, std::string *output) {
    switch (type_) {
      case CompressionType::kSnappyCompression:
        return CompressSnappy(input, output);
      case CompressionType::kZstdCompression:
        return CompressZSTD(input, output);
      case CompressionType::kNoCompression:
        *output = input;
        return true;
      default:
        return false;
    }
  }

  // 解压接口，返回解压后的数据
  bool Decompress(const std::string &input, std::string *output) {
    switch (type_) {
      case CompressionType::kSnappyCompression:
        return DecompressSnappy(input, output);
      case CompressionType::kZstdCompression:
        return DecompressZSTD(input, output);
      case CompressionType::kNoCompression:
        *output = input;
        return true;
      default:
        return false;
    }
  }

  CompressionType GetType() { return type_; }

 private:
  bool CompressSnappy(const std::string &input, std::string *output);
  bool DecompressSnappy(const std::string &input, std::string *output);

  bool CompressZSTD(const std::string &input, std::string *output);
  bool DecompressZSTD(const std::string &input, std::string *output);

  CompressionType type_;
};
}  // namespace Tskydb
