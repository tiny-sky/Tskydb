#pragma once

namespace Tskydb {
#define DISALLOW_COPY(cname)                                      \
  cname(const cname &) = delete;                     /* NOLINT */ \
  auto operator=(const cname &) -> cname & = delete; /* NOLINT */

#define DISALLOW_MOVE(cname)                                 \
  cname(cname &&) = delete;                     /* NOLINT */ \
  auto operator=(cname &&) -> cname & = delete; /* NOLINT */

#define DISALLOW_COPY_AND_MOVE(cname) \
  DISALLOW_COPY(cname);               \
  DISALLOW_MOVE(cname);
}  // namespace Tskydb
