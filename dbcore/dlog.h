#pragma once

/* A high-performance log manager.
 *
 * By default this is used as an append-only log, but users can be creative and
 * use it to represent heap regions via third-party lambda plugins.
 *
 * The basic design is a distributed log consisting of multiple log files, each
 * of which owns a dedicated log buffer.
 */
#include <atomic>
#include <vector>

#include <fcntl.h>
#include <liburing.h>
#include <numa.h>
#include <stdio.h>
#include <string.h>
#include <shared_mutex>

#include <aws/core/Aws.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

#include <boost/context/continuation.hpp>
#include <curl/curl.h>
#include <glog/logging.h>

#include "../util/spinlock.h"
#include "dlog-defs.h"
#include "pcommit.h"
#include "task.h"

namespace ermia {

namespace dlog {

constexpr uint64_t INVALID_CSN = ermia::pcommit::NDCSN_MASK;

extern std::atomic<uint64_t> current_csn;

extern std::vector<uint64_t> thread_begin_csns;

uint64_t get_min_thread_begin_csn();

void flush_all();

void dequeue_committed_xcts();

// Commit daemon function - only useful when dedicated commit thread is enabled
void commit_daemon();

void wakeup_commit_daemon();

void initialize();
void uninitialize();

#define SEGMENT_FILE_NAME_BUFSZ sizeof("tlog-01234567-01234567")

struct s3_meta_data {
  Aws::S3::S3Client *client;
  std::string *bucket_name;

  // lazy init
  CURLM *multi_handle = nullptr;

  char *buff;
  char *key;
  uint64_t size;
  uint64_t offset;
  bool should_yield;
};

// A segment of the log, i.e., a file
struct segment {
  // File descriptor for the underlying file (-1 if using S3 backend)
  int fd;

  char name[SEGMENT_FILE_NAME_BUFSZ];

  // The (global) beginning address this segment covers
  uint64_t start_offset;

  // Amount of data that has been written
  std::atomic<uint64_t> size;

  // Amount of data that has been written and pending for flush
  uint64_t expected_size;

  // Number of appends applied to this segment, also used as OCC version
  // Using MSB as dirty bit
  std::atomic<uint64_t> append_count;

  const static uint64_t DIRTY_BIT = 1ULL << 63;
  // ctor and dtor
  segment(int dfd, const char *segname, bool dio);
  ~segment();
};

// A thread/transaction-local log which consists of one or multiple segments. No
// CC because this is per thread, and it is expected that no more than one
// transaction will be using the log at the same time.
struct tls_log {
  // Directory where the segment files should be created. Ignored under S3.
  const char *dir;

  // Which NUMA node is this log supposed to run on?
  // This affect where the log buffers are allocated.
  int numa_node;

  enum class IOState {
    Idle = 0,
    Preparing = 1,
    Flushing = 2
  };
  std::atomic<IOState> iostate;

  // Log buffer size in bytes
  uint64_t logbuf_size;

  // Two log buffers (double buffering)
  char *logbuf[3];

  // number of time issue flush
  uint64_t flush_count;

  uint64_t last_flush_size;
  segment* last_segment;

  // Last csn for each log buffer
  uint64_t last_csns[2];

  // First csn for each log buffer
  uint64_t first_csns[2];

  // The log buffer accepting new writes
  char *active_logbuf;

  // Offset of the first available byte in the log buffer
  uint32_t logbuf_offset;

  // Durable LSN for this log
  tlog_lsn durable_lsn;

  // Current LSN for this log
  tlog_lsn current_lsn;

  // Segment size
  uint64_t segment_size;

  // All segments belonging to this partition. The last one
  // segments[segments.size()-1] is the currently open segment
  std::vector<segment*> segments;

  // io_uring structures
  struct io_uring ring;

  // Increase when allocate log block, decrease when populate log block
  // Only can flush when holes == 0
  std::atomic<uint8_t> holes;

  // Committer
  pcommit::tls_committer tcommitter;

  // Lock
  std::mutex lock;
  std::atomic<uint32_t> result_count;
  // Metadata for S3
  s3_meta_data write_metadata[3];
  CoroTask write_coro_task[3];
  ThreadTask write_thread_task[3];

  // ID of this log; can be seen as 'partition ID' -
  // caller/user should make sure this is unique
  uint32_t id;

  std::chrono::steady_clock::time_point last_open_new_logbuf;

  uint64_t fill_buf_tot_duration_us{0};
  uint64_t fill_buf_nb_times{0};

  // Get the currently open segment
  inline segment *current_segment() { return segments[segments.size() - 1]; }

  // Do flush when doing enqueue commits
  void enqueue_flush();

  // Issue an async I/O to flush the current active log buffer
  void issue_flush(const char *buf, uint64_t size);

  // Poll for log I/O completion and dequeue accordingly. We only allow one
  // active I/O at any time (io_uring requests may come back out of order).
  // @peek_once: whether to check for completion only once (true) or spin until
  // completion (false). Returns true if some I/O completed.
  bool poll_flush(bool peek_once = false);

  // Create a new segment when the current segment is about to exceed the max
  // segment size.
  void create_segment();

  // Dummy ctor and dtor. The user must use initialize/uninitialize() to make
  // sure we capture the proper parameters set in ermia::config which may get
  // initialized/created after tls_logs are created.
  tls_log() {}
  ~tls_log() {}

  // Initialize/uninitialize this tls-log object
  void initialize(const char *log_dir, uint32_t log_id, uint32_t node,
                  uint64_t logbuf_mb, uint64_t max_segment_mb);
  void uninitialize();

  inline uint32_t get_id() { return id; }
  inline segment *get_segment(uint32_t segnum) { 
    return segments[segnum];
  }

  inline pcommit::tls_committer *get_committer() { return &tcommitter; }

  inline uint64_t get_latency_ns() { return tcommitter.get_latency_ns(); }

  // Allocate a log block in-place on the log buffer
  log_block *allocate_log_block(uint32_t payload_size, uint64_t *out_cur_lsn,
                                uint64_t *out_seg_num, transaction *txn);

  // Enqueue commit queue
  void enqueue_committed_xct(uint64_t csn, uint64_t max_dependent_csn, bool is_local_log);

  // Dequeue commit queue
  inline void dequeue_committed_xcts(bool try_lock) { tcommitter.dequeue_committed_xcts(false, try_lock); }

  inline uint32_t get_commit_queue_size() {
    return tcommitter.get_queue_size();
  }

  // Last flush
  void last_flush();

  inline void switch_log_buffers() {
    uint32_t logbuf_idx = (active_logbuf == logbuf[0]) ? 1 : 0;
    active_logbuf = logbuf[logbuf_idx];
    first_csns[logbuf_idx] = INVALID_CSN; // first csn set to invalid
    logbuf_offset = 0;
  }

  void issue_read(int fd, char *buf, uint64_t size, uint64_t offset,
                  void *user_data, const char *segment_name = nullptr);

  bool peek_only(void *user_data, uint32_t read_size, bool from_s3=false);

  void peek_tid(int &tid, int &ret_val);

  uint64_t align_up_flush_size(uint64_t size);

  void reset_flushcount() { flush_count = 0; }

  void reset_latency() { tcommitter.reset_latency(); reset_logbuf_fill_latency(); }

  void reset_logbuf_fill_latency() {
    last_open_new_logbuf = std::chrono::steady_clock::time_point{};
    fill_buf_tot_duration_us = 0;
    fill_buf_nb_times = 0;
  }

  uint64_t get_pct(double pct) { return tcommitter.get_pct(pct); }

  const std::vector<uint64_t> &get_sampling_latencies() const {
    return tcommitter.get_sampling_latencies();
  }

  const uint64_t get_max_latency_ns() const {
    return tcommitter.get_max_latency_ns();
  }

  const uint64_t get_min_latency_ns() const {
    return tcommitter.get_min_latency_ns();
  }

  // Give the payload of txn, check it's position in the buffer
  // return true if the txn is the first transaction in the buffer
  // else return false
  const bool is_buffer_head(uint64_t payload_size) const {
    if (payload_size == 0) {
      return false;
    }
    uint32_t alloc_size = payload_size + sizeof(log_block);
    return (!logbuf_offset) || (alloc_size + logbuf_offset > logbuf_size);
  }

  void set_first_csn(uint64_t block_csn) {
    if (active_logbuf == logbuf[0]) {
      first_csns[0] = block_csn;
    } else {
      first_csns[1] = block_csn;
    }
  }

  void set_last_csn(uint64_t block_csn) {
    if (active_logbuf == logbuf[0]) {
      last_csns[0] = block_csn;
    } else {
      last_csns[1] = block_csn;
    }
  }
};

extern std::vector<tls_log *> tlogs;

} // namespace dlog

} // namespace ermia
