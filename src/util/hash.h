#pragma once

#include <cstddef>
#include <cstdint>

namespace Tskydb {

uint32_t Hash(const char *data, size_t n, uint32_t seed);

};  // namespace Tskydb
