#pragma once

#include "db/env.h"
#include "db/options.h"
#include "util/macros.h"
#include "util/slice.h"
#include "block.h"

namespace Tskydb {

class TableBuilder {
 public:
  // Create a builder that will store the contents of the table it is
  // building in *file.  Does not close the file.  It is up to the
  // caller to close the file after calling Finish().
  TableBuilder(const Options &options, WritableFile *file);

  DISALLOW_COPY(TableBuilder);

  ~TableBuilder();

  // Add key,value to the table being constructed.
  void Add(const Slice &key, const Slice &value);

  // flush any buffered key/value pairs to file.
  void Flush();

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  Status Finish();

  // Return non-ok iff some error has been detected.
  Status status() const;

  // Number of calls to Add() so far.
  uint64_t NumEntries() const;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  uint64_t FileSize() const;

  // Abandon
  void Close();

 private:
  bool ok() const { return status().ok(); }
  void WriteBlock(BlockBuilder *block, BlockHandle *handle);
  void WriteRawBlock(const Slice &block_contents, BlockHandle *handle);

  struct Rep;
  Rep *rep_;
};
}  // namespace Tskydb
