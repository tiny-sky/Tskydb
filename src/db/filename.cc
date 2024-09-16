// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "filename.h"

#include <cassert>
#include <cstdio>
#include <limits>

#include "env.h"
#include "util/slice.h"
#include "util/status.h"

namespace Tskydb {

static Status DoWriteStringToFile(Env *env, const Slice &data,
                                  const std::string &fname, bool should_sync) {
  WritableFile *file;
  Status s = env->NewWritableFile(fname, &file);
  if (!s.ok()) {
    return s;
  }
  s = file->Append(data);
  if (s.ok() && should_sync) {
    s = file->Sync();
  }
  if (s.ok()) {
    s = file->Close();
  }
  delete file;  // Will auto-close if we did not close above
  if (!s.ok()) {
    env->RemoveFile(fname);
  }
  return s;
}

bool ConsumeDecimalNumber(Slice *in, uint64_t *val) {
  // Constants that will be optimized away.
  constexpr const uint64_t kMaxUint64 = std::numeric_limits<uint64_t>::max();
  constexpr const char kLastDigitOfMaxUint64 =
      '0' + static_cast<char>(kMaxUint64 % 10);

  uint64_t value = 0;

  // reinterpret_cast-ing from char* to uint8_t* to avoid signedness.
  const uint8_t *start = reinterpret_cast<const uint8_t *>(in->data());

  const uint8_t *end = start + in->size();
  const uint8_t *current = start;
  for (; current != end; ++current) {
    const uint8_t ch = *current;
    if (ch < '0' || ch > '9') break;

    // Overflow check.
    // kMaxUint64 / 10 is also constant and will be optimized away.
    if (value > kMaxUint64 / 10 ||
        (value == kMaxUint64 / 10 && ch > kLastDigitOfMaxUint64)) {
      return false;
    }

    value = (value * 10) + (ch - '0');
  }

  *val = value;
  const size_t digits_consumed = current - start;
  in->remove_prefix(digits_consumed);
  return digits_consumed != 0;
}

Status WriteStringToFileSync(Env *env, const Slice &data,
                             const std::string &fname) {
  return DoWriteStringToFile(env, data, fname, true);
}

static std::string MakeFileName(const std::string &dbname, uint64_t number,
                                const char *suffix) {
  char buf[100];
  std::snprintf(buf, sizeof(buf), "/%06llu.%s",
                static_cast<unsigned long long>(number), suffix);
  return dbname + buf;
}

std::string LogFileName(const std::string &dbname, uint64_t number) {
  assert(number > 0);
  return MakeFileName(dbname, number, "log");
}

std::string VtableName(const std::string &dbname, uint64_t number) {
  return MakeFileName(dbname, number, "vtb");
}

std::string TableFileName(const std::string &dbname, uint64_t number) {
  assert(number > 0);
  return MakeFileName(dbname, number, "ldb");
}

std::string SSTTableFileName(const std::string &dbname, uint64_t number) {
  assert(number > 0);
  return MakeFileName(dbname, number, "sst");
}

std::string DescriptorFileName(const std::string &dbname, uint64_t number) {
  assert(number > 0);
  char buf[100];
  std::snprintf(buf, sizeof(buf), "/MANIFEST-%06llu",
                static_cast<unsigned long long>(number));
  return dbname + buf;
}

std::string CurrentFileName(const std::string &dbname) {
  return dbname + "/CURRENT";
}

std::string LockFileName(const std::string &dbname) { return dbname + "/LOCK"; }

std::string TempFileName(const std::string &dbname, uint64_t number) {
  assert(number > 0);
  return MakeFileName(dbname, number, "dbtmp");
}

std::string InfoLogFileName(const std::string &dbname) {
  return dbname + "/LOG";
}

// Return the name of the old info log file for "dbname".
std::string OldInfoLogFileName(const std::string &dbname) {
  return dbname + "/LOG.old";
}

// Owned filenames have the form:
//    dbname/CURRENT
//    dbname/LOCK
//    dbname/LOG
//    dbname/LOG.old
//    dbname/MANIFEST-[0-9]+
//    dbname/[0-9]+.(log|sst|ldb)
bool ParseFileName(const std::string &filename, uint64_t *number,
                   FileType *type) {
  Slice rest(filename);
  if (rest == "CURRENT") {
    *number = 0;
    *type = kCurrentFile;
  } else if (rest == "LOCK") {
    *number = 0;
    *type = kDBLockFile;
  } else if (rest == "LOG" || rest == "LOG.old") {
    *number = 0;
    *type = kInfoLogFile;
  } else if (rest.starts_with("MANIFEST-")) {
    rest.remove_prefix(strlen("MANIFEST-"));
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    if (!rest.empty()) {
      return false;
    }
    *type = kDescriptorFile;
    *number = num;
  } else {
    // Avoid strtoull() to keep filename format independent of the
    // current locale
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    Slice suffix = rest;
    if (suffix == Slice(".log")) {
      *type = kLogFile;
    } else if (suffix == Slice(".sst") || suffix == Slice(".ldb")) {
      *type = kTableFile;
    } else if (suffix == Slice(".dbtmp")) {
      *type = kTempFile;
    } else {
      return false;
    }
    *number = num;
  }
  return true;
}

Status SetCurrentFile(Env *env, const std::string &dbname,
                      uint64_t descriptor_number) {
  // Remove leading "dbname/" and add newline to manifest file name
  std::string manifest = DescriptorFileName(dbname, descriptor_number);
  Slice contents = manifest;
  assert(contents.starts_with(dbname + "/"));
  contents.remove_prefix(dbname.size() + 1);
  std::string tmp = TempFileName(dbname, descriptor_number);
  Status s = WriteStringToFileSync(env, contents.ToString() + "\n", tmp);
  if (s.ok()) {
    s = env->RenameFile(tmp, CurrentFileName(dbname));
  }
  if (!s.ok()) {
    env->RemoveFile(tmp);
  }
  return s;
}

}  // namespace Tskydb
