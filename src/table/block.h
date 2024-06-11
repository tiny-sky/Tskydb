#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/iterator.h"

namespace Tskydb {

struct BlockContents;
class Comparator;

class Block {
 public:
  // Initialize the block with the specified contents.
  explicit Block(const BlockContents &contents);

  Block(const Block &) = delete;
  Block &operator=(const Block &) = delete;

  ~Block();

  size_t size() const { return size_; }
  Iterator *NewIterator(const Comparator *comparator);

 private:
  class Iter;

  uint32_t NumRestarts() const;

  // +-------------------+---------------------+----------------------+
  // | Data              | Restart Points      | Num Restarts         |
  // +-------------------+---------------------+----------------------+
  const char *data_;
  size_t size_;
  uint32_t restart_offset_;  // Offset in data_ of restart array
  bool owned_;               // Block owns data_[]
};

struct Options;

class BlockBuilder {
 public:
  explicit BlockBuilder(const Options *options);

  DISALLOW_COPY(BlockBuilder);

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // key is larger than any previously added key
  void Add(const Slice &key, const Slice &value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  size_t CurrentSizeEstimate() const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return buffer_.empty(); }

 private:
  const Options *options_;
  std::string buffer_;              // Destination buffer
  std::vector<uint32_t> restarts_;  // Restart points
  int counter_;                     // Distance from restart point
  bool finished_;                   // Has Finish() been called?
  std::string last_key_;
};

}  // namespace Tskydb
