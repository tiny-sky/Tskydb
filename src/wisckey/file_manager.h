#pragma once

#include <memory>
#include <string>

#include <db/db.h>
#include <db/env.h>

#include "vtable_format.h"

namespace Tskydb {
namespace wisckey {

class VFileHandle {
 public:
  VFileHandle(uint64_t number, const std::string &name,
              std::unique_ptr<WriteableFileWriter> file)
      : number_(number), name_(name), file_(std::move(file)) {}

  uint64_t GetNumber() const { return number_; }

  const std::string &GetName() const { return name_; }

  WriteableFileWriter *GetFile() const { return file_.get(); }

 private:
  friend class VFileManager;

  uint64_t number_;
  std::string name_;
  std::unique_ptr<WriteableFileWriter> file_;
};

class VFileManager {
 public:
  VFileManager(DB *db) : db_(db) {}

  Status NewFile(std::unique_ptr<VFileHandle> *handle);

  Status FinishFile(std::shared_ptr<VFileMeta> file,
                    std::unique_ptr<VFileHandle> &&handle);

  Status DeleteFile(std::unique_ptr<VFileHandle> &&handle);

  Status BatchFinishFiles(
      const std::vector<std::pair<std::shared_ptr<VFileMeta>,
                                  std::unique_ptr<VFileHandle>>> &files);

  Status BatchDeleteFiles(
      const std::vector<std::unique_ptr<VFileHandle>> &handles);

 private:
  friend class FileHandle;

  DB *db_;
};
}  // namespace wisckey
}  // namespace Tskydb
