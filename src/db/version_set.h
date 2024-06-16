#pragma once

#include <mutex>
#include <string>

#include "common/iterator.h"
#include "options.h"
#include "util/macros.h"
#include "version_edit.h"

namespace Tskydb {

class TableCache;
class Compaction;
class InternalKeyComparator;

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
bool SomeFileOverlapsRange(const InternalKeyComparator &icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData *> &files,
                           const Slice *smallest_user_key,
                           const Slice *largest_user_key);

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

  // Return the level at which we should place a new memtable compaction
  // result that covers the range [smallest_user_key,largest_user_key].
  int PickLevelForMemTableOutput(const Slice &smallest_user_key,
                                 const Slice &largest_user_key);

  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==nullptr represents a key smaller than all the DB's keys.
  // largest_user_key==nullptr represents a key largest than all the DB's keys.
  bool OverlapInLevel(int level, const Slice *smallest_user_key,
                      const Slice *largest_user_key);

  void GetOverlappingInputs(
      int level,
      const InternalKey *begin,  // nullptr means before all keys
      const InternalKey *end,    // nullptr means after all keys
      std::vector<FileMetaData *> *inputs);

  void Ref();
  void Unref();

 private:
  friend class VersionSet;
  friend class Compaction;

  class LevelFileNumIterator;

  explicit Version(VersionSet* vset)
      : vset_(vset),
        next_(this),
        prev_(this),
        refs_(0),
        file_to_compact_(nullptr),
        file_to_compact_level_(-1),
        compaction_score_(-1),
        compaction_level_(-1) {}

  VersionSet *vset_;  // VersionSet to which this Version belongs
  Version *next_;     // Next version in linked list
  Version *prev_;     // Previous version in linked list
  int refs_;          // Number of live refs to this version

  // List of files per level
  std::vector<FileMetaData *> files_[config::kNumLevels];

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize().
  double compaction_score_;
  int compaction_level_;

  // Next file to compact based on seek stats.
  FileMetaData *file_to_compact_;
  int file_to_compact_level_;
};

class VersionSet {
 public:
  VersionSet(const std::string &dbname, const Options *options,
             TableCache *table_cache, const InternalKeyComparator *cmp);

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

  // Pick level and inputs for a new compaction.
  // Returns nullptr if there is no compaction to be done.
  Compaction *PickCompaction();

  // For GC
  // Add all files listed in any live version to *live.
  void AddLiveFiles(std::set<uint64_t> *live);

  // Return the current version.
  Version *current() const { return current_; }

  // Return the current log file number.
  uint64_t LogNumber() const { return log_number_; }

  // ?
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  // Return the current manifest file number
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // Allocate and return a new file number
  uint64_t NewFileNumber() { return next_file_number_++; }

  // Return the last sequence number.
  uint64_t LastSequence() const { return last_sequence_; }

  TableCache *GetTableCahe() const { return table_cache_.get(); }

  // Returns true iff some level needs a compaction.
  bool NeedsCompaction() const {
    Version *v = current_;
    return (v->compaction_score_ >= 1) || (v->file_to_compact_ != nullptr);
  }

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  Iterator *MakeInputIterator(Compaction *c);

 private:
  class Builder;

  friend class Version;
  friend class Compaction;

  // Stores the minimal range that covers all entries in inputs in
  // smallest, largest.
  void GetRange(const std::vector<FileMetaData *> &inputs,
                InternalKey *smallest, InternalKey *largest);

  // Stores the minimal range that covers all entries in inputs1 and inputs2
  // in *smallest, *largest.
  void GetRange2(const std::vector<FileMetaData *> &inputs1,
                 const std::vector<FileMetaData *> &inputs2,
                 InternalKey *smallest, InternalKey *largest);

  // optimization : Add more files to input[0]
  //
  // input[0] : level
  // input[1] : level + 1
  //
  // Try to add more input[0]
  // without changing the input[1] layer
  void SetupOtherInputs(Compaction *c);

  Env *const env_;
  const std::string dbname_;
  const Options *const options_;
  std::unique_ptr<TableCache> table_cache_;
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

// A Compaction encapsulates information about a compaction.
class Compaction {
 public:
  ~Compaction();

  // Return the level that is being compacted.  Inputs from "level"
  // and "level+1" will be merged to produce a set of "level+1" files.
  int level() const { return level_; }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  VersionEdit *edit() { return &edit_; }

  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  FileMetaData *input(int which, int i) const { return inputs_[which][i]; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  bool IsTrivialMove() const;

 private:
  friend class Version;
  friend class VersionSet;

  Compaction(const Options *options, int level);

  // State used to check for number of overlapping grandparent files
  // (parent == level_ + 1, grandparent == level_ + 2)
  std::vector<FileMetaData *> grandparents_;

  int level_;  // current compaction level
  Version *input_version_;
  VersionEdit edit_;

  // Each compaction reads inputs from "level_" and "level_+1"
  std::vector<FileMetaData *> inputs_[2];  // The two sets of inputs
};
}  // namespace Tskydb
