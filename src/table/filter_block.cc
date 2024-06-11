#include "filter_block.h"

#include "common/filter_policy.h"
#include "util/encoding.h"

namespace Tskydb {

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy *policy)
    : policy_(policy) {}

void FilterBlockBuilder::AddKey(const Slice &key) {
  Slice k = key;
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Restore the real key from key_ and start_
  start_.push_back(keys_.size());
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    const char *base = keys_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  filter_offsets_.push_back(result_.size());
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

Slice FilterBlockBuilder::Finish() {
  // PUT
  // [filter 0]
  // [filter N-1]
  if (!start_.empty()) {
    GenerateFilter();
  }

  // PUT
  // [offset of filter 0]
  // [offset of filter N - 1]
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }

  // PUT
  // [offset of beginning of offset array]
  // lg(base)
  PutFixed32(&result_, array_offset);
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}
}  // namespace Tskydb
