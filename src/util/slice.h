#pragma once

#include <assert.h>
#include <cstring>
#include <string>

namespace Tskydb {

class Slice {
 public:
  // Construct Slice
  Slice() : data_(""), size_(0) {}

  Slice(const char *d, size_t n) : data_(d), size_(n) {}

  Slice(const std::string &s) : data_(s.data()), size_(s.size()) {}

  Slice(const char *s) : data_(s), size_(strlen(s)) {}

  Slice(const Slice &) = default;
  Slice &operator=(const Slice &) = default;

  // Get Slice
  const char *data() const { return data_; }
  size_t size() const { return size_; }
  bool empty() const { return size_ == 0; }

  char operator[](size_t n) const {
    assert(n < size());
    return data_[n];
  }

  // Revise
  void clear() {
    data_ = "";
    size_ = 0;
  }

  std::string ToString() const { return std::string(data_, size_); }

  int compare(const Slice &b) const;

 private:
  const char *data_;
  size_t size_;
};

// Compare
inline bool operator==(const Slice &a, const Slice &b) {
  return (a.size() == b.size()) && (memcmp(a.data(), b.data(), a.size()) == 0);
}

inline bool operator!=(const Slice &a, const Slice &b) { return !(a == b); }

inline int Slice::compare(const Slice &b) const {
  const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
  int r = memcmp(data_, b.data(), min_len);
  if (r == 0) {
    if (size_ < b.size_)
      r = -1;
    else if (size_ > b.size_)
      r = 1;
  }
  return r;
}

};  // namespace Tskydb