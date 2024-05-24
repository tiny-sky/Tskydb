#pragma once

#include <mutex>
#include <string>

#include "options.h"
#include "util/macros.h"
#include "version_edit.h"

namespace Tskydb {

class TableCache;
class InternalKeyComparator;

class Version {
 public:
  struct Stats {
    FileMetaData *file;
    int file_level;
  };

  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  Status Get(const ReadOptions &, const LookupKey &key, std::string *val,
             Stats *stats);

  // Adds "stats" into the current state.  Returns true if a new
  // compaction may need to be triggered, false otherwise.
  bool UpdateStats(const Stats &stats);

  void Ref();
  void Unref();

 private:
  VersionSet *vset_;  // VersionSet to which this Version belongs
  Version *next_;     // Next version in linked list
  Version *prev_;     // Previous version in linked list
  int refs_;          // Number of live refs to this version

  // List of files per level
  std::vector<FileMetaData *> files_[config::kNumLevels];
};

class VersionSet {
 public:
  VersionSet(const std::string &dbname, const Options *options,
             std::unique_ptr<TableCache> table_cache,
             const InternalKeyComparator *cmp);

  DISALLOW_COPY(VersionSet);

  ~VersionSet();

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  Status LogAndApply(VersionEdit *edit, std::mutex *mu);

  // Recover the last saved descriptor from persistent storage.
  Status Recover(bool *save_manifest);

  // Return the current version.
  Version *current() const { return current_; }

  // Allocate and return a new file number
  uint64_t NewFileNumber() { return next_file_number_++; }

  // Return the last sequence number.
  uint64_t LastSequence() const { return last_sequence_; }

 private:
  friend class Version;

  Env *const env_;
  const std::string dbname_;
  const Options *const options_;
  TableCache *const table_cache_;
  const InternalKeyComparator icmp_;
  uint64_t next_file_number_;
  uint64_t manifest_file_number_;
  uint64_t last_sequence_;
  uint64_t log_number_;
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

  Version dummy_versions_;  // Head of circular doubly-linked list of versions.
  Version *current_;

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  std::string compact_pointer_[config::kNumLevels];
};
}  // namespace Tskydb
