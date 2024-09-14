#pragma once

#include <mutex>
#include <string>

#include "common/iterator.h"
#include "options.h"
#include "util/macros.h"
#include "version_edit.h"

namespace Tskydb {

class Wal;
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
  struct GetStats {
    FileMetaData *seek_file;
    int seek_file_level;
  };

  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  Status Get(const ReadOptions &, const LookupKey &key, std::string *val,
             GetStats *stats);

  // Adds "stats" into the current state.  Returns true if a new
  // compaction may need to be triggered, false otherwise.
  bool UpdateStats(const GetStats &stats);

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

  // Call func(arg, level, f) for every file that overlaps user_key in
  // order from newest to oldest.  If an invocation of func returns
  // false, makes no more calls.
  //
  // REQUIRES: user portion of internal_key == user_key.
  void ForEachOverlapping(Slice user_key, Slice internal_key, void *arg,
                          bool (*func)(void *, int, FileMetaData *));

  explicit Version(VersionSet *vset)
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

  void ReuseFileNumber(uint64_t file_number) {
    if (next_file_number_ == file_number + 1) {
      next_file_number_ = file_number;
    }
  }

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  Iterator *MakeInputIterator(Compaction *c);

 private:
  class Builder;

  friend class Version;
  friend class Compaction;

  void AppendVersion(Version *v);

  // Choose best_level
  // Calculate compaction_score_
  void Finalize(Version *v);

  // Save current contents to *log
  Status WriteSnapshot(Wal *log);

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

  WritableFile *descriptor_file_;
  Wal *descriptor_log_;
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

  int num_input_files(int which) const { return inputs_[which].size(); }

  // Maximum size of files to build during this compaction.
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  bool IsTrivialMove() const;

  // Add all inputs to this compaction as delete operations to *edit.
  void AddInputDeletions(VersionEdit *edit);

  // Detect the number of bytes overlapped with the grandparent layer
  // When it is greater than MaxGrandParentOverlapBytes, it returns true
  // means that a new output file needs to be replaced.
  bool ShouldStopBefore(const Slice &internal_key);

  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
  bool IsBaseLevelForKey(const Slice &user_key);

 private:
  friend class Version;
  friend class VersionSet;

  Compaction(const Options *options, int level);

  // State used to check for number of overlapping grandparent files
  // (parent == level_ + 1, grandparent == level_ + 2)
  std::vector<FileMetaData *> grandparents_;

  int level_;  // current compaction level
  uint64_t max_output_file_size_;
  Version *input_version_;
  VersionEdit edit_;

  // For ShouldStopBefore
  // State used to check for number of overlapping grandparent files
  // (parent == level_ + 1, grandparent == level_ + 2)
  std::vector<FileMetaData *> grandparents_;
  size_t grandparent_index_;  // Index in grandparent_starts_
  bool seen_key_;             // Some output key has been seen
  int64_t overlapped_bytes_;  // Bytes of overlap between current output
                              // and grandparent files

  // Each compaction reads inputs from "level_" and "level_+1"
  std::vector<FileMetaData *> inputs_[2];  // The two sets of inputs

  // State for implementing IsBaseLevelForKey
  // level_ptrs_ holds indices into input_version_->levels_: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).
  size_t level_ptrs_[config::kNumLevels];
};
}  // namespace Tskydb
