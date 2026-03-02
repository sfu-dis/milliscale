#include <atomic>
#include <dirent.h>
#include <vector>

#include <zstd.h>

#include "../engine.h"
#include "../macros.h"
#include "dlog.h"
#include "s3-client.h"
#include "sm-common.h"
#include "sm-config.h"
#include "sm-thread.h"

// io_uring code based off of examples from
// https://unixism.net/loti/tutorial/index.html

namespace ermia {

namespace dlog {

// Segment file name template: tlog-id-segnum
#define SEGMENT_FILE_NAME_FMT "tlog-%08x-%08x"
#define SEGMENT_FILE_NAME_BUFSZ sizeof("tlog-01234567-01234567")
#define GENERAL_BUCKET_OBJ_NAME_BUFSZ sizeof("tlog-01234567-01234567-01234567")

thread_local char segment_name_buf[SEGMENT_FILE_NAME_BUFSZ];
// Store object name for general bucket
thread_local char general_bucket_name[GENERAL_BUCKET_OBJ_NAME_BUFSZ];

std::vector<tls_log *> tlogs;

std::atomic<uint64_t> current_csn(1);

std::vector<uint64_t> thread_begin_csns;

std::mutex tls_log_lock;

thread_local struct io_uring tls_read_ring;

std::thread *pcommit_thread;
std::condition_variable pcommit_daemon_cond;
std::mutex pcommit_daemon_lock;
std::atomic<bool> pcommit_daemon_has_work(false);

thread_local CoroTask read_coro_task;
thread_local ThreadTask read_thread_task;
thread_local s3_meta_data read_metadata;

uint64_t get_min_thread_begin_csn() {
  uint64_t min_global_csn = ~uint64_t{0};
  for (auto b : dlog::thread_begin_csns) {
    if (b > 0 && b < min_global_csn) {
      min_global_csn = b;
    }
  }
  return min_global_csn;
}

void flush_all() {
  // Flush rest blocks
  for (auto &tlog : tlogs) {
    tlog->last_flush();
  }
}

void dequeue_committed_xcts() {
  for (auto &tlog : tlogs) {
    tlog->dequeue_committed_xcts(true);
  }
}

void wakeup_commit_daemon() {
  pcommit_daemon_lock.lock();
  pcommit_daemon_has_work = true;
  pcommit_daemon_lock.unlock();
  pcommit_daemon_cond.notify_all();
}

void commit_daemon() {
  auto timeout = std::chrono::milliseconds(ermia::config::pcommit_timeout_ms);
  while (!ermia::config::IsShutdown()) {
    if (pcommit_daemon_has_work) {
      dequeue_committed_xcts();
    } else {
      std::unique_lock<std::mutex> lock(pcommit_daemon_lock);
      pcommit_daemon_cond.wait_for(lock, timeout);
      pcommit_daemon_has_work = false;
    }
  }
}

void initialize() {
  uint32_t max_threads =
      std::max(ermia::config::loaders, ermia::config::worker_threads);
  for (uint32_t i = 0; i < max_threads; i++) {
    dlog::tls_log *tlog = new dlog::tls_log();
    dlog::tlogs.push_back(tlog);
    tlog->initialize(config::log_dir.c_str(), dlog::tlogs.size() - 1,
                     numa_node_of_cpu(sched_getcpu()), config::log_buffer_kb,
                     config::log_segment_mb);
    dlog::thread_begin_csns.push_back(0);
    tlog->tcommitter.set_tls_non_durable_csn(INVALID_CSN);
  }

  if (ermia::config::pcommit_thread) {
    pcommit_thread = new std::thread(commit_daemon);
  }
}

void uninitialize() {
  if (ermia::config::pcommit_thread) {
    pcommit_thread->join();
    delete pcommit_thread;
  }
}

void send_s3_read_request(void *p) {
  auto data = (s3_meta_data *)p;
  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(*data->bucket_name);
  request.SetKey(data->key);
  char offset_str[128];
  sprintf(offset_str, "bytes=%ld-%ld", data->offset,
          data->offset + data->size - 1);
  request.SetRange(offset_str);

  auto object_outcome = data->client->GetObject(request);
  LOG_IF(FATAL, !object_outcome.IsSuccess())
      << "Failed to download object from S3: "
      << object_outcome.GetError().GetMessage();
  auto &object_stream = object_outcome.GetResult().GetBody();
  object_stream.read(data->buff, data->size);
}

void init_task() {
thread_local bool task_inited = false;
  if (!task_inited) {
    if (ermia::config::flusher_thread) {
      read_thread_task.init(send_s3_read_request);
      read_thread_task.set_affinity(sched_getcpu());
    } else {
      read_coro_task.init(send_s3_read_request);
    }
    read_metadata.client = new Aws::S3::S3Client();
    read_metadata.bucket_name = &ermia::config::s3_bucket_names[0];
    task_inited = true;
  }
}

void send_s3_write_request(void *p) {
  auto data = (s3_meta_data *)p;
  bool success;
  uint64_t n_retries = 0;
retry:
  Aws::S3::Model::PutObjectRequest request;
  request.SetBucket(*data->bucket_name);
  request.SetKey(data->key);
  auto buffer = const_cast<unsigned char *>(
      reinterpret_cast<const unsigned char *>(data->buff));
  auto psb = Aws::MakeShared<Aws::Utils::Stream::PreallocatedStreamBuf>(
      "upload", buffer, data->size);
  auto buffer_stream = Aws::MakeShared<Aws::IOStream>("upload", psb.get());

  request.SetBody(buffer_stream);
  request.SetContentLength(data->size);
  if (data->offset > 0) {
    request.SetWriteOffsetBytes(data->offset);
  }

  // if first time, use default data->should_yield, if second time, then set
  // data->should_yield to false
  data->should_yield = data->should_yield && (n_retries == 0);
  auto put_object_outcome = data->client->PutObject(request);
  success = put_object_outcome.IsSuccess();

  if (!success && n_retries < 5) {
    n_retries++;
    goto retry;
  }

  LOG_IF(FATAL, !success) << put_object_outcome.GetError().GetMessage()
                          << std::endl;
}

struct custom_stack_allocator {
  boost::context::stack_context allocate() {
    boost::context::stack_context sctx;
    sctx.size = 64 * 1024;
    char *custom_stack = new char[sctx.size];
    sctx.sp = custom_stack + sctx.size;
    return sctx;
  }

  void deallocate(boost::context::stack_context &sctx) {}
};

void tls_log::initialize(const char *log_dir, uint32_t log_id, uint32_t node,
                         uint64_t logbuf_kb, uint64_t max_segment_mb) {
  std::lock_guard<std::mutex> lock(tls_log_lock);
  dir = log_dir;
  id = log_id;
  numa_node = node;
  iostate = IOState::Idle;
  logbuf_size = logbuf_kb * ermia::config::KB;
  int pmret = posix_memalign((void **)&logbuf[0], PAGE_SIZE, logbuf_size * 3);
  LOG_IF(FATAL, pmret) << "Unable to allocate log buffers";
  logbuf[1] = logbuf[0] + logbuf_size;
  logbuf[2] = logbuf[1] + logbuf_size;

  segment_size = max_segment_mb * ermia::config::MB;
  LOG_IF(FATAL, segment_size > SEGMENT_MAX_SIZE)
      << "Unable to allocate log buffer";
  holes = 0;
  logbuf_offset = 0;
  active_logbuf = logbuf[0];

  durable_lsn = 0;
  current_lsn = 0;
  flush_count = 0;

  // Create a new segment
  segments.reserve(
      16); // Each log will have at most 16 segments (base on fat_ptr)
  create_segment();
  current_segment()->start_offset = current_lsn;

  DLOG(INFO) << "Log " << id << ": new segment " << segments.size() - 1
             << ", start lsn " << current_lsn;

  // Initialize io_uring
  int ret = io_uring_queue_init(2, &ring, 0);
  LOG_IF(FATAL, ret != 0) << "Error setting up io_uring: " << strerror(ret);

  // Initialize committer
  tcommitter.initialize(log_id);

  if (ermia::config::enable_s3) {

    Aws::SDKOptions options;
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Off;
    Aws::InitAPI(options);
    if (!config::sync_io && config::flusher_thread == 0 &&
        !config::default_flusher) {
      Aws::Http::SetHttpClientFactory(
          Aws::MakeShared<MyHttpClientFactory>("CustomHttpClient"));
    }
    auto *client = new Aws::S3::S3Client();

    // initialize sender context
    for (uint32_t i = 0; i < ermia::config::s3_bucket_names.size(); ++i) {
      write_metadata[i].client = client;
      write_metadata[i].bucket_name = &ermia::config::s3_bucket_names[i];
    }
    if (ermia::config::default_flusher) {
      result_count = 0;
    } else if (ermia::config::flusher_thread) {
      for (uint32_t i = 0; i < ermia::config::s3_bucket_names.size(); ++i) {
        write_thread_task[i].init(send_s3_write_request);
      }
    } else {
      for (uint32_t i = 0; i < ermia::config::s3_bucket_names.size(); ++i) {
        write_coro_task[i].init(send_s3_write_request);
      }
    }
  }
}

void tls_log::uninitialize() {
  std::lock_guard<std::mutex> lg(lock);
  if (logbuf_offset) {
    uint64_t aligned_size = align_up_flush_size(logbuf_offset);
    current_lsn += (aligned_size - logbuf_offset);
    logbuf_offset = aligned_size;
    issue_flush(active_logbuf, logbuf_offset);
    poll_flush();
  }
  io_uring_queue_exit(&ring);
}

void tls_log::enqueue_flush() {
  std::lock_guard<std::mutex> lg(lock);
  if (iostate == IOState::Flushing) {
    poll_flush();
    iostate = IOState::Idle;
  }

  if (logbuf_offset) {
    uint64_t aligned_size = align_up_flush_size(logbuf_offset);
    current_lsn += (aligned_size - logbuf_offset);
    logbuf_offset = aligned_size;
    issue_flush(active_logbuf, logbuf_offset);
    if (ermia::config::sync_io) {
      poll_flush();
      iostate = IOState::Idle;
    }
  }
}

void tls_log::last_flush() {
  std::lock_guard<std::mutex> lg(lock);
  if (iostate == IOState::Flushing) {
    poll_flush();
    iostate = IOState::Idle;
  }

  if (logbuf_offset) {
    uint64_t aligned_size = align_up_flush_size(logbuf_offset);
    current_lsn += (aligned_size - logbuf_offset);
    logbuf_offset = aligned_size;
    issue_flush(active_logbuf, logbuf_offset);
    poll_flush();
    iostate = IOState::Idle;
  } else { // increase csn if no write transactions in the buffer
    tcommitter.set_tls_durable_csn(current_csn - 1);
    tcommitter.set_tls_non_durable_csn(INVALID_CSN);
  }
  if (ermia::config::is_general_bucket) {
    create_segment();
    current_segment()->start_offset = current_lsn;
  }
}

// TODO(tzwang) - fd should correspond to the actual segment
void tls_log::issue_read(int fd, char *buf, uint64_t size, uint64_t offset,
                         void *user_data, const char *segment_name) {
  if (ermia::config::enable_uring && fd > -1) {
    thread_local bool initialized = false;
    if (unlikely(!initialized)) {
      // Initialize the tls io_uring
      int ret = io_uring_queue_init(1024, &tls_read_ring, 0);
      LOG_IF(FATAL, ret != 0) << "Error setting up io_uring: " << strerror(ret);
      initialized = true;
    }

    struct io_uring_sqe *sqe = io_uring_get_sqe(&tls_read_ring);
    LOG_IF(FATAL, !sqe);

    io_uring_prep_read(sqe, fd, buf, size, offset);
    io_uring_sqe_set_data(sqe, user_data);
    int nsubmitted = io_uring_submit(&tls_read_ring);
    LOG_IF(FATAL, nsubmitted != 1);
  } else if (ermia::config::enable_s3) {
    char *key = const_cast<char *>(segment_name);
    uint64_t real_offset = offset;
    // Only allocate on stack when should_yield = false
    char name[GENERAL_BUCKET_OBJ_NAME_BUFSZ];
    if (ermia::config::is_general_bucket) {
      snprintf(name, GENERAL_BUCKET_OBJ_NAME_BUFSZ, "%s-%08lx", key,
               offset / logbuf_size);
      key = name;
      real_offset = offset % logbuf_size;
    }
    read_metadata.should_yield = false;
    read_metadata.key = key;
    read_metadata.buff = const_cast<char *>(buf);
    read_metadata.size = size;
    read_metadata.offset = real_offset;
    // TODO: default flusher
    init_task();
    if (ermia::config::flusher_thread) {
      read_thread_task.start(&read_metadata);
    } else {
      read_coro_task.start(&read_metadata);
    }
  }
}

void tls_log::peek_tid(int &tid, int &ret_val) {
  struct io_uring_cqe *cqe;
  int ret = io_uring_peek_cqe(&tls_read_ring, &cqe);
  if (ret < 0) {
    if (ret == -EAGAIN) {
      // Nothing yet - caller should retry later
      return;
    } else {
      LOG(FATAL) << strerror(ret);
    }
  }

  tid = *(int *)cqe->user_data;
  ret_val = cqe->res;
  io_uring_cqe_seen(&tls_read_ring, cqe);
}

bool tls_log::peek_only(void *user_data, uint32_t read_size, bool from_s3) {
  if (from_s3) {
    if (ermia::config::flusher_thread) {
      return read_thread_task.check() == TaskState::Finished;
    } else {
      return read_coro_task.check() == TaskState::Finished;
    }
  }

  struct io_uring_cqe *cqe;
  int ret = io_uring_peek_cqe(&tls_read_ring, &cqe);

  if (ret < 0) {
    if (ret == -EAGAIN) {
      // Nothing yet - caller should retry later
      return false;
    } else {
      LOG(FATAL) << strerror(ret);
    }
  }

  if (*(int *)cqe->user_data != *(int *)user_data) {
    return false;
  }

  ALWAYS_ASSERT(cqe->res == read_size);
  io_uring_cqe_seen(&tls_read_ring, cqe);
  return true;
}

void tls_log::issue_flush(const char *buf, uint64_t size) {
  if (iostate == IOState::Flushing) {
    poll_flush();
    iostate = IOState::Idle;
  }

  iostate = IOState::Flushing;
  last_flush_size = size;
  last_segment = current_segment();

  if (ermia::config::log_compress) {
    size = align_up(ZSTD_compress(logbuf[2], logbuf_size, buf, size, 1),
                    PAGE_SIZE);
    buf = logbuf[2];
  }

  // wait for others to populate log block
  while (holes) {
  };

  if (ermia::config::enable_s3) {
    char *key = current_segment()->name;
    uint64_t offset = current_segment()->size;
    if (ermia::config::is_general_bucket) {
      snprintf(general_bucket_name, GENERAL_BUCKET_OBJ_NAME_BUFSZ, "%s-%08lx",
               key, current_segment()->append_count.load());
      key = general_bucket_name;
      offset = 0;
      ASSERT(current_segment()->size % logbuf_size == 0);
      // ASSERT(current_segment()->size / logbuf_size ==
      // current_segment()->append_count);
    }
    for (uint32_t i = 0; i < ermia::config::s3_bucket_names.size(); ++i) {
      // put necessary informations into metadata
      write_metadata[i].should_yield = !ermia::config::sync_io;
      write_metadata[i].key = key;
      write_metadata[i].buff = const_cast<char *>(buf);
      write_metadata[i].size = size;
      write_metadata[i].offset = offset;
      if (ermia::config::default_flusher) {
        auto data = &write_metadata[i];
        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(*data->bucket_name);
        request.SetKey(data->key);

        auto buffer = const_cast<unsigned char *>(
            reinterpret_cast<const unsigned char *>(data->buff));
        auto psb = Aws::MakeShared<Aws::Utils::Stream::PreallocatedStreamBuf>(
            "upload", buffer, data->size);
        auto buffer_stream =
            Aws::MakeShared<Aws::IOStream>("upload", psb.get());

        request.SetBody(buffer_stream);
        request.SetContentLength(data->size);
        if (data->offset > 0) {
          request.SetWriteOffsetBytes(data->offset);
        }
        result_count = 0;
        data->client->PutObjectAsync(
            request,
            [this](const Aws::S3::S3Client *client,
                   const Aws::S3::Model::PutObjectRequest &request,
                   const Aws::S3::Model::PutObjectOutcome &outcome,
                   const std::shared_ptr<const Aws::Client::AsyncCallerContext>
                       &) {
              if (!outcome.IsSuccess()) {
                LOG(ERROR) << "S3 PutObjectAsync failed: "
                           << outcome.GetError().GetMessage();
              } else {
                result_count++;
              }
            });
      } else if (ermia::config::flusher_thread) {
        write_thread_task[i].start(&write_metadata[i]);
      } else {
        write_coro_task[i].start(&write_metadata[i]);
      }
    }
  }
  if (ermia::config::enable_uring) {
    // Issue an async I/O to flush the buffer into the current open segment
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    LOG_IF(FATAL, !sqe);

    io_uring_prep_write(sqe, current_segment()->fd, buf, size,
                        current_segment()->size);

    // Encode data size which is useful upon completion (to add to durable_lsn)
    // Must be set after io_uring_prep_write (which sets user_data to 0)
    sqe->user_data = size;

    int nsubmitted = io_uring_submit(&ring);
    LOG_IF(FATAL, nsubmitted != 1);
  }

  current_segment()->append_count.fetch_add(1 + segment::DIRTY_BIT,
                                            std::memory_order_release);
  current_segment()->expected_size += size;
  switch_log_buffers();
  current_segment()->append_count.fetch_add(segment::DIRTY_BIT,
                                            std::memory_order_release);
  flush_count ++;
}

bool tls_log::poll_flush(bool peek_once) {
  if (config::enable_s3) {
    if (config::default_flusher) {
      while (true) {
        if (result_count == 0 && peek_once) {
          return false;
        }
        if (result_count > 0) {
          break;
        }
      }
    } else {
      while (true) {
        bool success = true;
        for (uint32_t i = 0; i < ermia::config::s3_bucket_names.size(); ++i) {
          auto state = ermia::config::flusher_thread
                           ? write_thread_task[i].check()
                           : write_coro_task[i].check();
          success &= (state == TaskState::Finished);
        }
        if (!success) {
          if (peek_once) {
            return false;
          }
        } else {
          break;
        }
      }
    }
  }
  if (config::enable_uring) {
    struct io_uring_cqe *cqe = nullptr;
    while (true) {
      int ret = io_uring_peek_cqe(&ring, &cqe);
      if (ret < 0) {
        if (ret == -EAGAIN) {
          // Nothing yet
          if (peek_once) {
            return false;
          } else {
            continue;
          }
        } else {
          LOG(FATAL) << strerror(ret);
        }
      }
      break;
    }
    LOG_IF(FATAL, cqe->res < 0)
        << "Error in async operation: " << strerror(-cqe->res);
    uint64_t size = cqe->user_data;
    io_uring_cqe_seen(&ring, cqe);
  }

  durable_lsn += last_flush_size;
  last_segment->size.fetch_add(last_flush_size, std::memory_order_release);

  // get last tls durable csn
  uint64_t last_tls_durable_csn =
      (active_logbuf == logbuf[0]) ? last_csns[1] : last_csns[0];

  // Get the current non durable csn (or INVALID CSN)
  uint64_t current_non_durable_csn =
      (active_logbuf == logbuf[0]) ? first_csns[0] : first_csns[1];

  // Set the tls non-durable csn and durable csn
  tcommitter.set_tls_durable_csn(last_tls_durable_csn);
  tcommitter.set_tls_non_durable_csn(current_non_durable_csn);

  if (ermia::config::pcommit_thread) {
    dlog::wakeup_commit_daemon();
  } else {
    dlog::dequeue_committed_xcts();
  }
  return true;
}

void tls_log::create_segment() {
  size_t n = snprintf(segment_name_buf, sizeof(segment_name_buf),
                      SEGMENT_FILE_NAME_FMT, id, (unsigned int)segments.size());
  int dfd = -1;
  if (ermia::config::enable_uring) {
    DIR *logdir = opendir(dir);
    ALWAYS_ASSERT(logdir);
    dfd = dirfd(logdir);
  }
  segments.push_back(
      new segment(dfd, segment_name_buf, ermia::config::log_direct_io));

  // TODO(jiatangz): recycle old segment
}

uint64_t tls_log::align_up_flush_size(uint64_t size) {
  if (ermia::config::is_general_bucket) {
    return logbuf_size;
  }
  // Pad to PAGE_SIZE boundary if dio is enabled
  if (ermia::config::log_direct_io) {
    size = align_up(size, PAGE_SIZE);
    LOG_IF(FATAL, size > logbuf_size)
        << "Aligned log buffer data size > log buffer size";
  }
  return size;
}

log_block *tls_log::allocate_log_block(uint32_t payload_size,
                                       uint64_t *out_cur_lsn,
                                       uint64_t *out_seg_num,
                                       transaction *txn) {
  uint32_t alloc_size = payload_size + sizeof(log_block);
  LOG_IF(FATAL, alloc_size > logbuf_size) << "Total size too big";
  txn->xc->logid = id;
  // Read only, enqueue csn
  // We can assume this is already durable
  // So it's end csn is useless, just make sure all it's dependency is committed
  if (payload_size == 0) {
    txn->xc->end = txn->xc->begin - 1;
    if (ermia::config::pcommit) {
      std::lock_guard<std::mutex> lg(lock);
      enqueue_committed_xct(txn->xc->end, txn->max_dependent_csn, txn->is_local_log);
    }
    return nullptr;
  }

  std::lock_guard<std::mutex> lg(lock);

  bool is_first_txn = is_buffer_head(payload_size);
  if (tcommitter.is_empty() && is_first_txn) {
    tcommitter.set_dirty_flag();
    txn->xc->end = dlog::current_csn.fetch_add(1);
    enqueue_committed_xct(txn->xc->end, txn->max_dependent_csn, txn->is_local_log);
    tcommitter.set_tls_non_durable_csn(txn->xc->end);
  } else {
    txn->xc->end = dlog::current_csn.fetch_add(1);
    enqueue_committed_xct(txn->xc->end, txn->max_dependent_csn, txn->is_local_log);
  }
  auto block_csn = txn->xc->end;

  // If this allocated log block would span across segments, we need a new
  // segment.
  bool create_new_segment = false;
  if (alloc_size + logbuf_offset + current_segment()->expected_size >
      segment_size) {
    create_new_segment = true;
  }

  // If the allocated size exceeds the available space in the active logbuf,
  // or we need to create a new segment for this log block,
  // flush the active logbuf, and switch to the other logbuf.
  if (alloc_size + logbuf_offset > logbuf_size || create_new_segment) {
    if (logbuf_offset) {
      if (last_open_new_logbuf != std::chrono::steady_clock::time_point{}) {
        auto end = std::chrono::steady_clock::now();
        ++fill_buf_nb_times;
        fill_buf_tot_duration_us += std::chrono::duration_cast<std::chrono::microseconds>(end - last_open_new_logbuf).count();
      }
      // Pad to 4K boundary if dio is enabled
      uint64_t aligned_size = align_up_flush_size(logbuf_offset);
      current_lsn += (aligned_size - logbuf_offset);
      logbuf_offset = aligned_size;
      issue_flush(active_logbuf, logbuf_offset);
      // after flush, we will switch log buffer, which will reset current first
      // csn
      set_first_csn(block_csn); // set first csn
      if (ermia::config::sync_io) {
        poll_flush();
        iostate = IOState::Idle;
      }
      last_open_new_logbuf = std::chrono::steady_clock::now();
    }
    if (create_new_segment ||
        (!ermia::config::is_general_bucket && // for onezone bucket, need to
                                              // create new segment
         current_segment()->append_count >= ermia::config::max_appends)) {
      create_segment();
      current_segment()->start_offset = current_lsn;
      // TODO(jiatang): chkpt, for S3 Express Onezone, issue copy then delete if data is large enough
    }
  }

  log_block *lb = (log_block *)(active_logbuf + logbuf_offset);
  logbuf_offset += alloc_size;
  if (out_cur_lsn) {
    *out_cur_lsn = current_lsn;
  }
  current_lsn += alloc_size;

  if (out_seg_num) {
    *out_seg_num = segments.size() - 1;
  }

  new (lb) log_block(payload_size);

  if (iostate == IOState::Flushing) {
    if (poll_flush(true)) {
      iostate = IOState::Idle;
    }
  }

  set_last_csn(block_csn);
  // CSN must be made available here - after this the caller will start to
  // use the block to populate individual log records into the block, and
  // each log record needs to carry a CSN so that during recovery/read op
  // the newly instantiated record can directly be filled out with its CSN
  lb->csn = block_csn;
  holes ++;
  return lb;
}

void tls_log::enqueue_committed_xct(uint64_t csn, uint64_t max_dependent_csn, bool is_local_log) {
  tcommitter._commit_queue->push_back(csn, max_dependent_csn, is_local_log);
}

segment::segment(int dfd, const char *segname, bool dio)
    : size(0), expected_size(0), append_count(0) {
  // [dio] ignore if storing in S3
  if (!ermia::config::enable_uring) {
    fd = -1;
  } else {
    int flags = dio ? O_DIRECT : O_SYNC;
    flags |= (O_RDWR | O_CREAT | O_TRUNC);
    fd = openat(dfd, segname, flags, 0644);
    LOG_IF(FATAL, fd < 0);
  }
  strcpy(name, segname);
}

segment::~segment() {
  if (fd >= 0) {
    close(fd);
  }
}

} // namespace dlog

} // namespace ermia
