#include "vfile_builder.h"

namespace Tskydb {
namespace wisckey {

void VFileBuilder::Add(const VRecord &record, VHandle *handle) {
  if (!ok()) return;

  encoder_.EncodeRecord(record);
  handle->offset = file_->GetFileSize();
  handle->size = encoder_.GetEncodedSize();

  status_ = file_->Append(encoder_.GetHeader());
  if (ok()) {
    status_ = file_->Append(encoder_.GetRecord());
  }
}

Status VFileBuilder::Finish() {
  if (!ok()) return status();

  std::string buffer;
  VFileFooter footer;
  footer.EncodeTo(&buffer);

  status_ = file_->Append(buffer);
  if (ok()) {
    status_ = file_->Flush();
  }
  return status();
}

void VFileBuilder::Close() {}

}  // namespace wisckey

}  // namespace Tskydb
