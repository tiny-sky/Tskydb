#include "db.h"

#include <assert.h>
#include <mutex>

#include "filename.h"
#include "options.h"
#include "snapshot.h"
#include "table_builder.h"
#include "util/status.h"
#include "util/sync.h"
#include "version_edit.h"
#include "write_batch.h"

#include <leveldb/cache.h>
#include <leveldb/options.h>

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

struct DB::CompactionState {
  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };

  Output *current_output() { return &outputs[outputs.size() - 1]; }

  Compaction *const compaction;

  explicit CompactionState(Compaction *c)
      : compaction(c),
        smallest_snapshot(0),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {}

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile *outfile;
  TableBuilder *builder;

  uint64_t total_bytes;
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T *ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

Options SanitizeOptions(const std::string &dbname,
                        const InternalKeyComparator *icmp,
                        const InternalFilterPolicy *ipolicy,
                        const Options &src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);

  // TODO : Added log

  if (result.block_cache == nullptr) {
    result.block_cache = new LRUCache(8 << 20);
  }
  return result;
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
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, options)),
      owns_cache_(options_.block_cache != options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      background_work_finished_signal_(&mutex_),
      mem_(nullptr),
      imm_(nullptr) {
  tmp_batch_ = std::make_unique<WriteBatch>();
  versions_ = std::make_unique<VersionSet>(dbname, &options, table_cache_,
                                           &internal_comparator_);
}

DB::~DB() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    shutting_down_.store(true, std::memory_order_release);
    while (background_compaction_scheduled_) {
      background_work_finished_signal_.Wait();
    }
  }

  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
  delete logfile_;
  delete table_cache_;
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

void DB::RemoveObsoleteFiles() {
  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

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
          table_cache_->Evict(number);
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
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
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
  assert(background_compaction_scheduled_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  background_compaction_scheduled_ = false;

  // optimization : reschedule compaction if needed
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll();
}

void DB::RecordBackgroundError(const Status &s) {
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
  }
}

void DB::BackgroundCompaction() {
  if (imm_ != nullptr) {
    CompactMemTable();
    return;
  }

  auto c = std::move(versions_->PickCompaction());

  Status status;
  if (c == nullptr) {
    // Noting to do
  } else if (c->IsTrivialMove()) {
    // optimization : Move file to next level
    FileMetaData *f = c->input(0, 0);
    c->edit()->RemoveFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest,
                       f->largest);
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
  } else {
    CompactionState *compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    RemoveObsoleteFiles();
  }
}

void DB::CompactMemTable() {
  assert(imm_ != nullptr);

  VersionEdit edit;
  Version *base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.store(false, std::memory_order_release);
    RemoveObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

Status DB::WriteLevel0Table(MemTable *imm, VersionEdit *edit, Version *base) {
  const uint64_t start_micros = env_->NowMicros();

  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(
      meta.number);  // Protect current file Avoid compaction
  Iterator *iter = imm->NewIterator();

  Status s;
  {
    mutex_.unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.lock();
  }

  delete iter;
  pending_outputs_.erase(meta.number);

  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != nullptr) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest,
                  meta.largest);
  }

  // main ?
  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

Status DB::FinishCompactionOutputFile(CompactionState *compact,
                                      Iterator *input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Close();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator *iter =
        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
    s = iter->status();
    delete iter;
  }
  return s;
}

Status DB::OpenCompactionOutputFile(CompactionState *compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number;
  {
    mutex_.lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status DB::InstallCompactionResults(CompactionState *compact) {
  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output &out = compact->outputs[i];
    compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                         out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

Status BuildTable(const std::string &dbname, Env *env, const Options &options,
                  TableCache *table_cache, Iterator *iter, FileMetaData *meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string filename = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile *file;
    s = env->NewWritableFile(filename, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder *builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());
    Slice key;
    for (; iter->Valid(); iter->Next()) {
      key = iter->key();
      builder->Add(key, iter->value());
    }
    if (!key.empty()) {
      meta->largest.DecodeFrom(key);
    }

    // Complete the construction of SSTable
    s = builder->Finish();
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator *it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->RemoveFile(filename);
  }
  return s;
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

  // Create new Wal file
  if (s.ok() && impl->mem_ == nullptr) {
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile *lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = std::make_unique<Wal>(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
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
  impl->mutex_.unlock();
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
  Version::GetStats stats;

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

Status DB::MakeMemoryToWrite(bool force) {
  Status s;

  while (true) {
    // leveldb :延迟写操作：当内存表快满时，会延迟写操作以减缓系统的负担。

    if (!force &&
        (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // 当前 memtable 还有空间，继续写入
      break;
    } else if (imm_ != nullptr) {
      // 旧的 memtable 正在压缩，等待后台任务完成
      background_work_finished_signal_.Wait();
    } else {
      // 创建新的 memtable 并切换日志文件
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile *lfile = nullptr;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        versions_->ReuseFileNumber(new_log_number);
        break;
      }

      delete logfile_;
      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_.reset(new Wal(lfile));
      imm_ = mem_;
      has_imm_.store(true, std::memory_order_release);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false;
      MaybeScheduleCompaction();
    }
  }
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

Status DB::DoCompactionWork(CompactionState *compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;

  // If there is no snapshot
  // you can safely delete all key-value pairs older than the current latest
  // sequence number. When snapshot exists you need to keep all data after the
  // oldest snapshot.
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  Iterator *input = versions_->MakeInputIterator(compact->compaction);

  // Release mutex while we're actually doing the compaction work
  mutex_.unlock();

  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {
    // Prioritize immutable compaction work
    if (has_imm_.load(std::memory_order_relaxed)) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.lock();
      if (imm_ != nullptr) {
        CompactMemTable();
        // Wake up MakeRoomForWrite() if necessary.
        background_work_finished_signal_.SignalAll();
      }
      mutex_.unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key();
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
              0) {
        // Reassign
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Outdated data
        drop = true;
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }

    if (!drop) {
      if (compact->builder == nullptr) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  //
  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != nullptr) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = nullptr;

  //
  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  return status;
}

Status BuildTable(const std::string &dbname, Env *env, const Options &options,
                  TableCache *table_cache, Iterator *iter, FileMetaData *meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  // Create a file
  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile *file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    // Inserting Data
    TableBuilder *builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());
    Slice key;
    for (; iter->Valid(); iter->Next()) {
      key = iter->key();
      builder->Add(key, iter->value());
    }
    if (!key.empty()) {
      meta->largest.DecodeFrom(key);
    }

    // Finish and check for builder
    s = builder->Finish();
    if (s.ok()) {
      meta->file_size = builder->FileSize();
    }
    delete builder;

    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      Iterator *it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
  } else {
    env->RemoveFile(fname);
  }
  return s;
}
}  // namespace Tskydb
