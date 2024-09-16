#pragma once

#include "db/env.h"
#include "db/options.h"

#include "vtable_format.h"

namespace Tskydb {
namespace wisckey {

class VFileBuilder {
 public:
  VFileBuilder(const Options &options, WriteableFileWriter *file)
      : options_(options), file_(file), encoder_(options_.compression) {}

  // Adds the record to the file and points the handle to it.
  void Add(const VRecord &record, VHandle *handle);

  // Returns non-ok iff some error has been detected.
  Status status() const { return status_; }

  // Finishes building the table.
  // REQUIRES: Finish(), Abandon() have not been called.
  Status Finish();

  // Abandons building the table. If the caller is not going to call
  // Finish(), it must call Abandon() before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called.
  void Close();

 private:
  bool ok() const { return status().ok(); }

  Options options_;
  WriteableFileWriter *file_;

  Status status_;
  Encoder encoder_;
};
}  // namespace wisckey

}  // namespace Tskydb
