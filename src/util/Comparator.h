#pragma once

#include "slice.h"

namespace Tskydb {

class Comparator {
 public:
    Comparator() = default;
   ~Comparator() = default;

  auto Compare(const Slice &a, const Slice &b) const  -> int {
    return a.compare(b);
  }
};
}  // namespace Tskydb
