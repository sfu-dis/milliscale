#include <atomic>

#include "../engine.h"
#include "../macros.h"
#include "pcommit.h"
#include "sm-common.h"

namespace ermia {

namespace pcommit {

// tls_committer-local durable CSNs - belongs to tls_committer
// but stored here together
uint64_t *_tls_durable_csn =
    (uint64_t *)malloc(sizeof(uint64_t) * config::MAX_THREADS);

// tls_committer-local non-durable CSNs - belongs to tls_committer
// but stored here together
uint64_t *_tls_non_durable_csn =
    (uint64_t *)malloc(sizeof(uint64_t) * config::MAX_THREADS);

std::atomic<uint64_t> global_upto_csn {0};

// It is a single producer, multiple consumer queue
// no need to lock during enqueue (unless extend)
void commit_queue::push_back(uint64_t csn, uint64_t max_dependent_csn, bool is_local_log) {
  if ((end + 1) % length == start) {
    extend();
  }
  auto start_time = std::chrono::steady_clock::now();
  queue[end].csn = csn;
  queue[end].is_local_log = is_local_log;
  queue[end].max_dependent_csn = max_dependent_csn;
  queue[end].dequeued = false;
  queue[end].start_time = start_time;
  end = (end + 1) % length;
}

void commit_queue::extend() {
  // CRITICAL_SECTION(cs, lock);
  lock.lock();
  Entry *new_queue = new Entry[length * 2];
  if (end >= start) {
    memcpy(new_queue, queue, sizeof(Entry) * length);
  } else {
    size_t first_part = length - start;
    size_t second_part = end;
    memcpy(new_queue, queue + start, first_part * sizeof(Entry));
    memcpy(new_queue + first_part, queue, second_part * sizeof(Entry));
    start = 0;
    end = first_part + end;
  }
  length = length * 2;
  delete[] queue;
  queue = new_queue;
  lock.unlock();
}

void tls_committer::initialize(uint32_t id) {
  this->id = id;
  max_latency_ns = 0;
  min_latency_ns = std::numeric_limits<uint64_t>::max();
  dequeue_count = 0;
  _commit_queue = new commit_queue();
}

// The target csn can be set to
uint64_t tls_committer::get_dequeue_csn(uint64_t target_csn) {

  for (uint32_t i = 0; i < ermia::dlog::tlogs.size(); i++) {
  retry:
    uint64_t csn = volatile_read(_tls_non_durable_csn[i]);
    if (!tls_committer::is_empty(csn)) {
      target_csn = std::min(target_csn, csn & NDCSN_MASK);
    } else if (tls_committer::is_dirty(csn)) {
      // wait until it is not dirty (unlikely)
      goto retry;
    }
    // if it is not dirty and it is empty, then skip it
  }

  return target_csn;
}

void tls_committer::dequeue_committed_xcts(bool old_target, bool try_lock) {
  // XXX(tzwang): enter the CS early to obtain get a consistent view of end_time and entry.start_time.
  // Previously we did this after getting end_time, and occassionally there will be cases where
  // end_time is less than start_time. This can happen if a newer entry is pushed onto the queue
  // before the dequeuing thread enters this critical section.
  // CRITICAL_SECTION(cs, _commit_queue->lock);
  if (try_lock) {
    if (!_commit_queue->lock.try_lock()) {
      return;
    }
  } else {
    _commit_queue->lock.lock();
  }

  uint64_t upto_csn;
  if (old_target){
    upto_csn = global_upto_csn.load();
  } else {
    // XXX(jiatangz): Read only workload problem is solved when enqueue the read only transaction
    // using tls durable csn can adapt the case that there isn't a global current csn (like GSN case)
    // upto_csn = get_dequeue_csn(dlog::current_csn.load(std::memory_order_relaxed));
    upto_csn = get_dequeue_csn(_tls_durable_csn[id] + 1);
    uint64_t old_csn = global_upto_csn.load(std::memory_order_acquire);
    while (old_csn < upto_csn && !global_upto_csn.compare_exchange_strong(
        old_csn, upto_csn,
        std::memory_order_release,
        std::memory_order_acquire)) {
    }
  }
  
  auto end_time = std::chrono::steady_clock::now();
  uint32_t start = _commit_queue->start;
  uint32_t end = _commit_queue->end;
  uint32_t dequeue = 0;

  bool dequeueing = true;
  if (ermia::config::optimize_dequeue){
    if (ermia::config::dependency_aware){
      for (uint32_t idx = start; idx != end; idx = (idx + 1) % _commit_queue->length) {
        auto &entry = _commit_queue->queue[idx];

        if (volatile_read(entry.csn) > _tls_durable_csn[id]) {
          break;
        }

        if (volatile_read(entry.csn) < upto_csn || entry.is_local_log || entry.dequeued) {
          // dequeue
          dequeue += !(entry.dequeued);
          if (volatile_read(entry.dequeued) == false) {
            if (end_time <= entry.start_time) {
              end_time = std::chrono::steady_clock::now();
            }
            LOG_IF(FATAL, end_time <= entry.start_time);
            uint64_t latency = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - entry.start_time).count();

            sampler.process(latency);
            max_latency_ns = std::max(max_latency_ns, latency);
            min_latency_ns = std::min(min_latency_ns, latency);
            _commit_queue->total_latency_ns += latency;
          }
        } else {
          break;
        }
      }
      _commit_queue->start = start = (start + dequeue) % _commit_queue->length;
      dequeue_count += dequeue;
      dequeue = 0;
    }
    
    uint32_t size = _commit_queue->size();
    // New algorithm: Pop the element that have csn <= tls_durable_csn && dependent csn < upto csn
    // This is not the best approach, a better way is find the last index (i) csn <= tls_durable_csn, 
    // then remove the items in queue[start, i] and move remain items to the end of queue[start, i]  
    for (uint32_t idx = start; idx != end; idx = (idx + 1) % _commit_queue->length) {
      auto &entry = _commit_queue->queue[idx];
      if (volatile_read(entry.csn) > _tls_durable_csn[id]) {
        break;
      }
      if (volatile_read(entry.max_dependent_csn) < upto_csn) {
        // can dequeue this
        dequeue += dequeueing;
        if (volatile_read(entry.dequeued) == false) {
          if (end_time <= entry.start_time) {
            end_time = std::chrono::steady_clock::now();
          }
          LOG_IF(FATAL, end_time <= entry.start_time);
          uint64_t latency = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - entry.start_time).count();

          sampler.process(latency);
          max_latency_ns = std::max(max_latency_ns, latency);
          min_latency_ns = std::min(min_latency_ns, latency);
          _commit_queue->total_latency_ns += latency;
          if (!dequeueing) {
            volatile_write(entry.dequeued, true);
          }
        }
      }
    }
  } else { // Old algorithm
    for (uint32_t idx = start; idx != end; idx = (idx + 1) % _commit_queue->length) {
      auto &entry = _commit_queue->queue[idx];
      if (volatile_read(entry.csn) >= upto_csn) {
        break;
      }
      if (end_time <= entry.start_time) {
        end_time = std::chrono::steady_clock::now();
      }
      LOG_IF(FATAL, end_time <= entry.start_time);
      uint64_t latency = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - entry.start_time).count();
      sampler.process(latency);
      max_latency_ns = std::max(max_latency_ns, latency);
      min_latency_ns = std::min(min_latency_ns, latency);
      _commit_queue->total_latency_ns += latency;
      dequeue ++;
    }
  }
  _commit_queue->start = (start + dequeue) % _commit_queue->length;
  dequeue_count += dequeue;
  _commit_queue->lock.unlock();
}

} // namespace pcommit

} // namespace ermia
