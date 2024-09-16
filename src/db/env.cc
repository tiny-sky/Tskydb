#include "env.h"

#include <assert.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <string>
#include <thread>

namespace Tskydb {

Status PosixError(const std::string &context, int error_number) {
  if (error_number == ENOENT) {
    return Status::NotFound(context, std::strerror(error_number));
  } else {
    return Status::IOError(context, std::strerror(error_number));
  }
}

Status Env::RemoveFile(const std::string &filename) {
  if (::unlink(filename.c_str()) != 0) {
    return PosixError(filename, errno);
  }
  return Status::OK();
}

Status Env::GetFileSize(const std::string &filename, uint64_t *size) {
  struct ::stat file_stat;
  if (::stat(filename.c_str(), &file_stat) != 0) {
    *size = 0;
    return PosixError(filename, errno);
  }
  *size = file_stat.st_size;
  return Status::OK();
}

Status Env::NewWritableFile(const std::string &filename,
                            WritableFile **result) {
  int fd = ::open(filename.c_str(), O_TRUNC | O_WRONLY | O_CREAT, 0644);
  if (fd < 0) {
    *result = nullptr;
    return PosixError(filename, errno);
  }

  *result = new WritableFile(filename, fd);
  return Status::OK();
}

Status Env::NewRandomAccessFile(const std::string &filename,
                                RandomAccessFile **result) {
  *result = nullptr;
  int fd = ::open(filename.c_str(), O_RDONLY);
  if (fd < 0) {
    return PosixError(filename, errno);
  }

  size_t file_size;
  Status status = GetFileSize(filename, &file_size);
  if (status.ok()) {
    void *mmap_base = ::mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (mmap_base != MAP_FAILED) {
      *result = new MmapReadableFile(
          filename, reinterpret_cast<char *>(mmap_base), file_size);
    } else {
      status = PosixError(filename, errno);
    }
  }
  ::close(fd);
  return status;
}

Status Env::RenameFile(const std::string &from, const std::string &to) {
  if (std::rename(from.c_str(), to.c_str()) != 0) {
    return PosixError(from, errno);
  }
  return Status::OK();
}

void Env::BackgroundThreadMain() {
  while (true) {
    bg_work_mutex_.lock();

    while (bg_work_queue_.empty()) {
      bg_work_cv_.Wait();
    }

    assert(!bg_work_queue_.empty());
    auto bg_work_func = bg_work_queue_.front().func;
    void *bg_work_arg = bg_work_queue_.front().arg;
    bg_work_queue_.pop();

    bg_work_mutex_.unlock();
    bg_work_func(bg_work_arg);
  }
}

void Env::Schedule(BackgroundWorkFunc bg_work_function, void *bg_work_arg) {
  bg_work_mutex_.lock();

  // Start the background thread, if we haven't done so already.
  if (!started_background_thread_) {
    started_background_thread_ = true;
    std::thread(&BackgroundThreadMain).detach();
  }

  // If the queue is empty, the background thread may be waiting for work.
  if (bg_work_queue_.empty()) {
    bg_work_cv_.Signal();
  }

  bg_work_queue_.emplace(std::move(bg_work_function), bg_work_arg);
  bg_work_mutex_.unlock();
}

Status Env::GetChildren(const std::string &directory_path,
                        std::vector<std::string> *result) {
  result->clear();
  ::DIR *dir = ::opendir(directory_path.c_str());
  if (dir == nullptr) {
    return PosixError(directory_path, errno);
  }
  struct ::dirent *entry;
  while ((entry = ::readdir(dir)) != nullptr) {
    result->emplace_back(entry->d_name);
  }
  ::closedir(dir);
  return Status::OK();
}

uint64_t Env::NowMicros() {
  static constexpr uint64_t kUsecondsPerSecond = 1000000;
  struct ::timeval tv;
  ::gettimeofday(&tv, nullptr);
  return static_cast<uint64_t>(tv.tv_sec) * kUsecondsPerSecond + tv.tv_usec;
}

WriteableFileWriter::WriteableFileWriter(std::unique_ptr<WritableFile> &&file)
    : writable_file_(std::move(file)), filesize_(0) {}

Status WriteableFileWriter::Append(const Slice &data) {
  Status status = writable_file_->Append(data);
  if (status.ok()) {
    filesize_ += data.size();
  }

  return status;
}

Status WriteableFileWriter::Close() { return writable_file_->Close(); }

Status WriteableFileWriter::Flush() { return writable_file_->Flush(); }

Status WriteableFileWriter::Sync() { return writable_file_->Sync(); }

WritableFile::WritableFile(std::string filename, int fd)
    : pos_(0),
      fd_(fd),
      is_manifest_(IsManifest(filename)),
      filename_(std::move(filename)),
      dirname_(Dirname(filename)) {}

WritableFile::~WritableFile() {
  if (fd_ >= 0) {
    Close();
  }
}

Status WritableFile::Append(const Slice &data) {
  size_t write_size = data.size();
  const char *write_data = data.data();

  size_t copy_size = std::min(write_size, kWritableFileBufferSize - pos_);
  std::memcpy(buf_ + pos_, write_data, copy_size);
  write_size -= copy_size;
  pos_ += copy_size;
  if (write_size == 0) {
    return Status::OK();
  }

  Status status = FlushBuffer();
  if (!status.ok()) {
    return status;
  }

  if (write_size < kWritableFileBufferSize) {
    std::memcpy(buf_, write_data, write_size);
    pos_ = write_size;
    return Status::OK();
  }
  return WriteUnbuffered(write_data, write_size);
}

Status WritableFile::Close() {
  Status status = FlushBuffer();
  const int close_result = ::close(fd_);
  if (close_result < 0 && status.ok()) {
    status = PosixError(filename_, errno);
  }
  fd_ = -1;
  return status;
}

Status WritableFile::Flush() { return FlushBuffer(); }

Status WritableFile::Sync() {
  Status status = SyncDirIfManifest();
  if (!status.ok()) {
    return status;
  }

  status = FlushBuffer();
  if (!status.ok()) {
    return status;
  }

  return SyncFd(fd_, filename_);
}

Status WritableFile::FlushBuffer() {
  Status status = WriteUnbuffered(buf_, pos_);
  pos_ = 0;
  return status;
}

Status WritableFile::WriteUnbuffered(const char *data, size_t size) {
  while (size > 0) {
    ssize_t write_result = ::write(fd_, data, size);
    if (write_result < 0) {
      if (errno == EINTR) {
        continue;  // Retry
      }
      return PosixError(filename_, errno);
    }
    data += write_result;
    size -= write_result;
  }
  return Status::OK();
}

Status WritableFile::SyncDirIfManifest() {
  Status status;
  if (!is_manifest_) {
    return status;
  }

  int fd = ::open(dirname_.c_str(), O_RDONLY);
  if (fd < 0) {
    status = PosixError(dirname_, errno);
  } else {
    status = SyncFd(fd, dirname_);
    ::close(fd);
  }
  return status;
}

Status WritableFile::SyncFd(int fd, const std::string &fd_path) {
  bool sync_success = ::fsync(fd) == 0;
  if (sync_success) {
    return Status::OK();
  }
  return PosixError(fd_path, errno);
}

SequentialFile::SequentialFile(std::string filename, int fd)
    : fd_(fd), filename_(std::move(filename)) {}

SequentialFile::~SequentialFile() { close(fd_); }

Status SequentialFile::Read(size_t n, Slice *result, char *scratch) {
  Status status;
  while (true) {
    ::ssize_t read_size = ::read(fd_, scratch, n);
    if (read_size < 0) {  // Read error.
      if (errno == EINTR) {
        continue;  // Retry
      }
      status = PosixError(filename_, errno);
      break;
    }
    *result = Slice(scratch, read_size);
    break;
  }
  return status;
}

Status SequentialFile::Skip(uint64_t n) {
  if (::lseek(fd_, n, SEEK_CUR) == static_cast<off_t>(-1)) {
    return PosixError(filename_, errno);
  }
  return Status::OK();
}

class PosixRandomAccessFile final : public RandomAccessFile {
 public:
  PosixRandomAccessFile(std::string filename, int fd)
      : filename_(std::move(filename)), fd_(fd) {}

 private:
  const int fd_;  // -1 if has_permanent_fd_ is false.
  const std::string filename_;
};

class MmapReadableFile : public RandomAccessFile {
 public:
  MmapReadableFile(std::string filename, char *mmap_base, size_t length)
      : mmap_base_(mmap_base),
        length_(length),
        filename_(std::move(filename)) {}

  ~MmapReadableFile() override {
    ::munmap(static_cast<void *>(mmap_base_), length_);
  }

  Status Read(uint64_t offset, size_t n, Slice *result,
              char *scratch) const override {
    if (offset + n > length_) {
      *result = Slice();
      return PosixError(filename_, EINVAL);
    }

    *result = Slice(mmap_base_ + offset, n);
    return Status::OK();
  }

 private:
  char *const mmap_base_;
  const size_t length_;
  const std::string filename_;
};

}  // namespace Tskydb
