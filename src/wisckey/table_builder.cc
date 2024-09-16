#pragma once

#include "table_builder.h"
#include "db/keyformat.h"

#include "vfile_builder.h"

namespace Tskydb {
namespace wisckey {

void WKTableBuilder::Add(const Slice &key, const Slice &value) {
  if (!ok()) return;

  ParsedInternalKey ikey;
  if (!ParseInternalKey(key, &ikey)) {
    status_ = Status::Corruption(Slice());
    return;
  }

  if (ikey.type != kTypeValue || value.size() < options_.min_vtable_size) {
    base_builder_->Add(key, value);
    return;
  }

  std::string index_value;
  AddVTable(ikey.user_key, value, &index_value);
  if (!ok()) return;

  ikey.type = kTypeVtableIndex;
  std::string index_key;
  AppendInternalKey(&index_key, ikey);
  base_builder_->Add(index_key, index_value);
}

void WKTableBuilder::AddVTable(const Slice &key, const Slice &value,
                               std::string *index_value) {
  if (!ok()) return;

  if (!vtable_builder_) {
    status_ = vtable_manager_->NewFile(&vtable_handle_);
    if (!ok()) return;
    vtable_builder_.reset(
        new VFileBuilder(options_, vtable_handle_->GetFile()));
  }

  VIndex index;
  VRecord record;
  record.key = key;
  record.value = value;
  index.file_number = vtable_handle_->GetNumber();
  vtable_builder_->Add(record, &index.Vtable_handle);
  if (ok()) {
    index.EncodeTo(index_value);
  }
}

Status WKTableBuilder::status() const {
  Status s = status_;
  if (s.ok()) {
    s = base_builder_->status();
  }
  if (s.ok() && vtable_builder_) {
    s = vtable_builder_->status();
  }
  return s;
}

Status WKTableBuilder::Finish() {
  base_builder_->Finish();
  if (vtable_builder_) {
    vtable_builder_->Finish();
    if (ok()) {
      std::shared_ptr<VFileMeta> file =
          std::make_shared<VFileMeta>(vtable_handle_->GetNumber(),
                                      vtable_handle_->GetFile()->GetFileSize());
      file->FileStateTransit(FileEvent::kFlushOrCompactionOutput);
      status_ = vtable_manager_->FinishFile(file, std::move(vtable_handle_));
    } else {
      status_ = vtable_manager_->DeleteFile(std::move(vtable_handle_));
    }
  }
  return status();
}

void WKTableBuilder::Close() {
  base_builder_->Close();
  if (vtable_builder_) {
    vtable_builder_->Close();
    status_ = vtable_manager_->DeleteFile(std::move(vtable_handle_));
  }
}

uint64_t WKTableBuilder::NumEntries() const {
  return base_builder_->NumEntries();
}

uint64_t WKTableBuilder::FileSize() const { return base_builder_->FileSize(); }

}  // namespace wisckey

}  // namespace Tskydb
