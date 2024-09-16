#pragma once

#include <memory>

#include "db/options.h"
#include "vfile_builder.h"
#include "table/table_builder.h"
#include "util/status.h"

#include "file_manager.h"

namespace Tskydb {
namespace wisckey {

class WKTableBuilder {
 public:
  WKTableBuilder(const Options &options,
                 std::unique_ptr<TableBuilder> base_builder,
                 std::shared_ptr<VFileManager> vtable_manager)
      : base_builder_(std::move(base_builder)),
        vtable_manager_(vtable_manager) {}

  void Add(const Slice &key, const Slice &value);

  Status status() const;

  Status Finish();

  void Close();

  uint64_t NumEntries() const;

  uint64_t FileSize() const;

 private:
  bool ok() const { return status().ok(); }

  void AddVTable(const Slice &key, const Slice &value, std::string *index_value);

  Status status_;
  Options options_;
  std::unique_ptr<TableBuilder> base_builder_;
  std::unique_ptr<VFileHandle> vtable_handle_;
  std::shared_ptr<VFileManager> vtable_manager_;
  std::unique_ptr<VFileBuilder> vtable_builder_;
};
}  // namespace wisckey

}  // namespace Tskydb
