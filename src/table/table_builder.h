#pragma once

#include "db/env.h"
#include "db/options.h"
#include "util/macros.h"
#include "util/slice.h"

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

 private:
  struct Rep;
  Rep *rep_;
};
}  // namespace Tskydb
