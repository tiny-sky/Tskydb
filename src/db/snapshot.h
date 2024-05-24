#pragma once

#include <assert.h>
#include <list>
#include <memory>

#include "util/format.h"

namespace Tskydb {

class Snapshot {
 public:
  Snapshot(SequenceNumber sequence_number)
      : sequence_number_(sequence_number) {}

  SequenceNumber sequence_number() const { return sequence_number_; }

 private:
  friend class SnapshotList;

  const SequenceNumber sequence_number_;
};

class SnapshotList {
 public:
  SnapshotList() {}

  bool empty() const { return snapshots_.empty(); }

  const Snapshot *oldest() const {
    assert(!empty());
    return snapshots_.front().get();
  }
  const Snapshot *newest() const {
    assert(!empty());
    return snapshots_.back().get();
  }

  const Snapshot *New(SequenceNumber sequence_number) {
    assert(empty() || newest()->sequence_number_ <= sequence_number);

    std::unique_ptr<Snapshot> snapshot =
        std::make_unique<Snapshot>(sequence_number);
    snapshots_.emplace_back(std::move(snapshot));
    return snapshot.get();
  }

  void Delete(const Snapshot *snapshot) {
    for (auto it = snapshots_.begin(); it != snapshots_.end(); ++it) {
      if (it->get() == snapshot) {
        snapshots_.erase(it);
        return;
      }
    }
  }

 private:
  std::list<std::unique_ptr<Snapshot>> snapshots_;
};
}  // namespace Tskydb
