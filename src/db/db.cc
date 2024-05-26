#include "db.h"

#include <assert.h>
#include <mutex>

#include "filename.h"
#include "options.h"
#include "snapshot.h"
#include "util/status.h"
#include "util/sync.h"
#include "version_edit.h"
#include "write_batch.h"

namespace Tskydb {

const int kNumNonTableCacheFiles = 10;

struct DB::Writer {
  explicit Writer(std::mutex *mu)
      : batch(nullptr), sync(false), done(false), cv(mu) {}

  Status status;
  WriteBatch *batch;
  bool sync;
  bool done;
  CondVar cv;
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T *ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

Options SanitizeOptions(const std::string &dbname, const Options &src) {
  Options result = src;
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);
}

static int TableCacheSize(const Options &sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

// =====================================================
//

DB::DB(const Options &options, const std::string &dbname)
    : env_(options.env),
      internal_comparator_(options.comparator),
      internal_filter_policy_(options.filter_policy),
      options_(SanitizeOptions(dbname, options)),
      dbname_(dbname),
      background_work_finished_signal_(&mutex_),
      mem_(nullptr),
      imm_(nullptr) {
  tmp_batch_ = std::make_unique<WriteBatch>();
  auto table_cache_ =
      std::make_unique<TableCache>(dbname_, options_, TableCacheSize(options_));
  versions_ = std::make_unique<VersionSet>(dbname, &options, table_cache_,
                                           &internal_comparator_);
}

DB::~DB() {
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
}

void DB::RemoveObsoleteFiles() {
  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  // Get the files in the directory
  // identify the version by file name
  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);

  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete;
  for (std::string &filename : filenames) {
    if (ParseFileName(filename, &number, &type)) {
      bool keep = false;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  number == versions_->PrevLogNumber());
          break;
        case kDescriptorFile:
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        files_to_delete.push_back(std::move(filename));
        if (type == kTableFile) {
          versions_->GetTableCahe()->Evict(number);
        }
      }
    }

    mutex_.unlock();
    for (const std::string &filename : files_to_delete) {
      env_->RemoveFile(dbname_ + '/' + filename);
    }
    mutex_.lock();
  }
}

void DB::MaybeScheduleCompaction() {
  if (background_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
  } else if (imm_ == nullptr && !versions_->NeedsCompaction()) {
    // No work to be done
  } else {
    background_compaction_scheduled_ = true;
    env_->Schedule(&DB::BGWork, this);
  }
}

void DB::BGWork(void *db) { reinterpret_cast<DB *>(db)->BackgroundCall(); }

void DB::BackgroundCall() {
  MutexLock l(&mutex_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else {
    BackgroundCompaction();
  }

  background_compaction_scheduled_ = false;
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll();
}

void DB::BackgroundCompaction() {
  if (imm_ != nullptr) {
    CompactMemTable();
    return;
  }

  auto c = std::move(versions_->PickCompaction());
  
}

Status DB::Put(const WriteOptions &opt, const Slice &key, const Slice &value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions &opt, const Slice &key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

Status DB::Open(const Options &options, const std::string &dbname, DB **dbptr) {
  *dbptr = nullptr;

  DB *impl = new DB(options, dbname);
  impl->mutex_.lock();
  VersionEdit edit;

  // This will restore the descriptor from edit
  // any changes added to the *edit.
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);  // TODO

  // Create new log file
  if (s.ok() && impl->mem_ == nullptr) {
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile *lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = std::make_unique<WritableFile>(*lfile);
      impl->logfile_number_ = new_log_number;
      impl->log_ = std::make_unique<Wal>(lfile);
      impl->mem_ = new MemTable(
          impl->internal_comparator_);  // Is it possible to use shared_ptr ?
      impl->mem_->Ref();
    }
  }

  // save_manifest mean ?
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }

  // Clean old data and perform necessary compression
  if (s.ok()) {
    impl->RemoveObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.unlock();

  // Returns a pointer to the database
  if (s.ok()) {
    assert(impl->mem_ != nullptr);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Status DB::Write(const WriteOptions &options, WriteBatch *updates) {
  Writer w(&mutex_);
  w.batch = updates;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  Status status = MakeMemoryToWrite(updates == nullptr);
  uint64_t last_sequence = versions_->LastSequence();
  Writer *last_writer = &w;
  if (status.ok() && updates != nullptr) {
    WriteBatch *write_batch = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(write_batch);

    {
      mutex_.unlock();
      status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(write_batch, mem_);
      }
      mutex_.lock();
    }
    if (write_batch == tmp_batch_.get()) tmp_batch_->Clear();
  }

  while (true) {
    Writer *ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }
  return status;
}

Status DB::Get(const ReadOptions &options, const Slice &key,
               std::string *value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;

  if (options.snapshot != nullptr) {
    snapshot = options.snapshot->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable *mem = mem_;
  MemTable *imm = imm_;
  Version *current = versions_->current();
  mem->Ref();
  if (imm != nullptr) imm->Ref();
  current->Ref();

  // When data needs to access the current version
  // Reduce the allowed_seeks for file
  // compress the file when allowed_seeks <= 0
  bool have_stat_update = false;
  Version::Stats stats;

  // First find value from memtable
  // Then find from immutable
  // Finally, find from the current version
  {
    mutex_.unlock();
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {
      // Done
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    mutex_.lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != nullptr) imm->Unref();
  current->Unref();
  return s;
}

WriteBatch *DB::BuildBatchGroup(Writer **last_writer) {
  Writer *first = writers_.front();
  WriteBatch *result = first->batch;

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer *>::iterator iter = writers_.begin();
  ++iter;
  for (; iter != writers_.end(); ++iter) {
    Writer *w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_.get();
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

Status DB::Recover(VersionEdit *edit, bool *save_manifest) {
  return Status::OK();
}
}  // namespace Tskydb
