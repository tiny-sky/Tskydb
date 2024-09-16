#include "file_manager.h"
#include "db/filename.h"

namespace Tskydb {
namespace wisckey {

Status VFileManager::NewFile(std::unique_ptr<VFileHandle> *handle) {
  uint64_t number = db_->versions_->NewFileNumber();
  std::string name = VtableName(db_->dbname_, number);

  Status s;
  WritableFile *file;
  s = db_->env_->NewWritableFile(name, &file);
  if (!s.ok()) return s;

  handle->reset(new VFileHandle(number, name,
                                std::make_unique<WriteableFileWriter>(file)));
  {
    std::lock_guard<std::mutex> l(db_->mutex_);
    db_->pending_outputs_.insert(number);
  }
  return s;
}

Status VFileManager::FinishFile(std::shared_ptr<VFileMeta> file,
                                std::unique_ptr<VFileHandle> &&handle) {
  std::vector<
      std::pair<std::shared_ptr<VFileMeta>, std::unique_ptr<VFileHandle>>>
      tmp;
  tmp.emplace_back(std::make_pair(file, std::move(handle)));
  return BatchFinishFiles(tmp);
}

Status VFileManager::DeleteFile(std::unique_ptr<VFileHandle> &&handle) {
  std::vector<std::unique_ptr<VFileHandle>> tmp;
  tmp.emplace_back(std::move(handle));
  return BatchDeleteFiles(tmp);
}

Status VFileManager::BatchFinishFiles(
    const std::vector<std::pair<std::shared_ptr<VFileMeta>,
                                std::unique_ptr<VFileHandle>>> &files) {
  Status s;
  VersionEdit edit;
  // edit.SetColumnFamilyID(cf_id);
  for (auto &file : files) {
    s = file.second->GetFile()->Sync();
    if (s.ok()) {
      s = file.second->GetFile()->Close();
    }
    if (!s.ok()) return s;

    edit.AddVFile(file.first);
  }

  {
    std::lock_guard<std::mutex> lock(db_->mutex_);
    s = db_->versions_->LogAndApply(&edit, &db_->mutex_);
    for (const auto &file : files)
      db_->pending_outputs_.erase(file.second->GetNumber());
  }
  return s;
}

Status VFileManager::BatchDeleteFiles(
    const std::vector<std::unique_ptr<VFileHandle>> &handles) {
  Status s;
  for (auto &handle : handles) s = db_->env_->RemoveFile(handle->GetName());
  {
    std::lock_guard<std::mutex> lock(db_->mutex_);
    for (const auto &handle : handles)
      db_->pending_outputs_.erase(handle->GetNumber());
  }
  return s;
}

}  // namespace wisckey

}  // namespace Tskydb
