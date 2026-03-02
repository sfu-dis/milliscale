#pragma once

#include <mutex>
#include <chrono>
#include "sampler.h"
#include "sm-config.h"
#include "sm-thread.h"
#include "../util/mcs-lock.h"

namespace ermia {

namespace pcommit {

static const uint64_t DIRTY_FLAG = uint64_t{1} << 63;
// non-durable csn mask
static const uint64_t NDCSN_MASK = (uint64_t{1} << 63) - 1;

extern uint64_t *_tls_durable_csn CACHE_ALIGNED;
extern uint64_t *_tls_non_durable_csn CACHE_ALIGNED;
extern std::atomic<uint64_t> global_upto_csn;

struct commit_queue {
  struct Entry {
    uint64_t csn;
    bool is_local_log;
    uint64_t max_dependent_csn;
    bool dequeued;
    std::chrono::steady_clock::time_point start_time;
    Entry() : csn(0), is_local_log(false), max_dependent_csn(0), dequeued(false) {}
  };

  Entry *queue;
  volatile uint32_t start;
  volatile uint32_t end;
  uint64_t total_latency_ns;
  uint32_t length;
  // mcs_lock lock; // tail lock
  std::mutex lock;

  commit_queue()
      : start(0), end(0), total_latency_ns(0),
        length(config::pcommit_queue_length) {
    queue = new Entry[length];
  }
  ~commit_queue() { delete[] queue; }
  void push_back(uint64_t csn, uint64_t max_dependent_csn, bool is_local_log);
  void extend();
  uint32_t size() {
    if (end >= start) {
      return end - start;
    } else {
      return end + length - start;
    }
  }
};

struct tls_committer {
  // Same as log id and thread id
  uint32_t id;
  commit_queue *_commit_queue CACHE_ALIGNED;
  ReservoirSampler<uint64_t> sampler;
  uint64_t max_latency_ns;
  uint64_t min_latency_ns;
  uint64_t dequeue_count;

  tls_committer() {}
  ~tls_committer() {}

  inline uint32_t get_queue_size() { return _commit_queue->size(); }

  inline uint64_t get_latency_ns() { return _commit_queue->total_latency_ns; }

  // The log will be marked as dirty if the first transaction in a buffer gets a
  // csn but haven't enqueue to the pcommit queue
  inline void set_dirty_flag() {
    volatile_write(_tls_non_durable_csn[id],
                   _tls_non_durable_csn[id] | DIRTY_FLAG);
  }

  // Set tls durable csn of this thread
  inline void set_tls_durable_csn(uint64_t csn) {
    volatile_write(_tls_durable_csn[id], csn);
  }

  // Set tls non-durable csn of this thread
  // Also clean the dirty bit and switch the empty bit
  inline void set_tls_non_durable_csn(uint64_t csn) {
    volatile_write(_tls_non_durable_csn[id], csn);
  }

  // Initialize a tls_committer object
  void initialize(uint32_t id);

  // Given a target CSN, return a dequeue csn, every thing before this csn is
  // durable
  uint64_t get_dequeue_csn(uint64_t target_csn);

  // Enqueue commit queue of this thread
  inline void enqueue_committed_xct(uint64_t csn, uint64_t max_dependent_csn, bool is_local_log) {
    _commit_queue->push_back(csn, max_dependent_csn, is_local_log);
  }

  // Dequeue commit queue of this thread
  void dequeue_committed_xcts(bool old_target=false, bool try_lock=false);

  // Extend commit queue
  inline void extend_queue() { _commit_queue->extend(); }

  void reset_latency() {
    _commit_queue->total_latency_ns = 0;
    max_latency_ns = 0;
    min_latency_ns = std::numeric_limits<uint64_t>::max();
    dequeue_count = 0;
    sampler.clear();
  }

  uint64_t get_pct(double pct) { return sampler.get_pct(pct); }

  const std::vector<uint64_t> &get_sampling_latencies() const {
    return sampler.result();
  }

  const uint64_t get_max_latency_ns() const { return max_latency_ns; }

  const uint64_t get_min_latency_ns() const { return min_latency_ns; }

  bool is_empty() { return is_empty(volatile_read(_tls_non_durable_csn[id])); }

  static bool is_dirty(uint64_t csn) { return csn & DIRTY_FLAG; }

  static bool is_empty(uint64_t csn) { return csn == NDCSN_MASK; }

  static uint64_t get_tls_non_durable_csn(uint64_t csn) {
    return csn & NDCSN_MASK;
  }
};

} // namespace pcommit

} // namespace ermia
