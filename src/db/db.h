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

  // Loop to confirm the status of database
  // before writing data to the write-ahead log file
  // including whether the MemTable has reached the maximum capacity
  // and whether the number of files in Level-0 has reached a certain threshold.
  Status MakeMemoryToWrite(bool force) { return Status::OK(); };

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit *edit, bool *save_manifest);

  // First writer must have a non-null batch
  WriteBatch *BuildBatchGroup(Writer **last_writer);

  // Delete any unneeded files and stale in-memory entries.
  void RemoveObsoleteFiles();

  // Compression operations
  void BackgroundCompaction();

  // Possibly schedule compression operations
  void MaybeScheduleCompaction();

  // background
  static void BGWork(void *db);
  void BackgroundCall();

  // Constant after construction
  Env *const env_;
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_;
  const std::string dbname_;

  std::mutex mutex_;
  MemTable *mem_;
  MemTable *imm_;

  std::unique_ptr<WritableFile> logfile_;
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

  // Close the database
  std::atomic<bool> shutting_down_;
  // Background task completion signal
  CondVar background_work_finished_signal_;

  // Has a background compaction been scheduled or is running?
  bool background_compaction_scheduled_;
};
}  // namespace Tskydb
