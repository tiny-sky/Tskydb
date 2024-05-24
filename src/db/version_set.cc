#include <assert.h>
#include "version_set.h"

namespace Tskydb {

void Version::Ref() { ++refs_; }

void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}
}  // namespace Tskydb
