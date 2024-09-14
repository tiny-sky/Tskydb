#pragma once

#include <deque>
#include <memory>
#include <mutex>
#include <set>
#include <string>

#include "options.h"
#include "table/memtable.h"
#include "table/table_cache.h"
#include "util/format.h"
#include "util/macros.h"
#include "util/status.h"
#include "version_edit.h"
#include "version_set.h"
#include "wal.h"
#include "write_batch.h"

#include <leveldb/filter_policy.h>

namespace Tskydb {
class DB {
 public:
  DB(const Options &options, const std::string &dbname);

  DISALLOW_COPY(DB);

  ~DB();

  // Open the database with the specified "name".
  // Stores a pointer to a heap-allocated database in *dbptr and returns
  // OK on success.
  // Stores nullptr in *dbptr and returns a non-OK status on error.
  // Caller should delete *dbptr when it is no longer needed.
  static Status Open(const Options &, const std::string &name, DB **dbptr);

  // Set the database entry for "key" to "value".  Returns OK on success,
  // and a non-OK status on error.
  Status Put(const WriteOptions &options, const Slice &key, const Slice &value);

  // Remove the database entry (if any) for "key".  Returns OK on
  // success, and a non-OK status on error.  It is not an error if "key"
  // did not exist in the database.
  Status Delete(const WriteOptions &options, const Slice &key);

  // Apply the specified updates to the database.
  // Returns OK on success, non-OK on failure.
  Status Write(const WriteOptions &options, WriteBatch *batch);

  // If the database contains an entry for "key" store the
  // corresponding value in *value and return OK.
  //
  // If there is no entry for "key" leave *value unchanged and return
  // a status for which Status::IsNotFound() returns true.
  //
  // May return some other Status on an error.
  Status Get(const ReadOptions &options, const Slice &key, std::string *value);

 private:
  struct Writer;
  struct CompactionState;

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  struct CompactionStats {
    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

    void Add(const CompactionStats &c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }

    int64_t micros;
    int64_t bytes_read;
    int64_t bytes_written;
  };

  // Loop to confirm the status of database
  // before writing data to the write-ahead log file
  // including whether the MemTable has reached the maximum capacity
  // and whether the number of files in Level-0 has reached a certain threshold.
  Status MakeMemoryToWrite(bool force);

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit *edit, bool *save_manifest);

  // First writer must have a non-null batch
  WriteBatch *BuildBatchGroup(Writer **last_writer);

  // Delete any unneeded files and stale in-memory entries.
  void RemoveObsoleteFiles();

  // Compaction
  // =================================
  // Core compression process
  Status DoCompactionWork(CompactionState *compact);
  // Background Compression operations
  void BackgroundCompaction();
  // Possibly schedule compression operations
  void MaybeScheduleCompaction();
  // Compact the in-memory write buffer to disk.
  void CompactMemTable();
  // immutable -> sstable level 0
  Status WriteLevel0Table(MemTable *mem, VersionEdit *edit, Version *base);
  // Complete the table construction through builder
  Status FinishCompactionOutputFile(CompactionState *compact, Iterator *input);
  // Create a Table File and builder helper function
  Status OpenCompactionOutputFile(CompactionState *compact);
  // Compress level file to level + 1 file
  // Delete level file
  // Added to level + 1
  Status InstallCompactionResults(CompactionState *compact);

  // Background Work
  // =================================
  static void BGWork(void *db);
  void BackgroundCall();
  void RecordBackgroundError(const Status &s);

  // BuildTable
  // =================================
  Status BuildTable(const std::string &dbname, Env *env, const Options &options,
                    TableCache *table_cache, Iterator *iter,
                    FileMetaData *meta);

  // Helper
  // =================================
  const Comparator *user_comparator() const {
    return internal_comparator_.user_comparator();
  }

  // Constant after construction
  Env *const env_;
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_;
  const std::string dbname_;

  TableCache *const table_cache_;

  std::mutex mutex_;
  MemTable *mem_;
  MemTable *imm_;
  std::atomic<bool> has_imm_;

  WritableFile *logfile_;
  uint64_t logfile_number_;
  std::unique_ptr<Wal> log_;

  // For GC
  // Set of table files to protect from deletion
  std::set<uint64_t> pending_outputs_;

  // Queue of writers.
  std::deque<Writer *> writers_;
  std::unique_ptr<WriteBatch> tmp_batch_;

  SequenceNumber last_sequence_;

  std::unique_ptr<VersionSet> versions_;

  SnapshotList snapshots_;

  // Close the database
  std::atomic<bool> shutting_down_;
  // Background task completion signal
  CondVar background_work_finished_signal_;
  // background error
  Status bg_error_;
  // Has a background compaction been scheduled or is running?
  bool background_compaction_scheduled_;

  // Other
  CompactionStats stats_[config::kNumLevels];
};
}  // namespace Tskydb
