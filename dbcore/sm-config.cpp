#include "sm-config.h"
#include "../macros.h"
#include "sm-thread.h"
#include <iostream>
#include <numa.h>
#include <unistd.h>

namespace ermia {
namespace config {

uint32_t arena_size_mb = 8;
bool threadpool = true;
bool tls_alloc = true;
std::string benchmark("");
uint32_t worker_threads = 0;
uint32_t io_threads = 0;
uint32_t remote_threads = 0;
uint32_t benchmark_seconds = 30;
uint64_t benchmark_transactions = 0;
bool parallel_loading = false;
bool retry_aborted_transactions = false;
bool quick_bench_start = false;
int backoff_aborted_transactions = 0;
int numa_nodes = 0;
bool enable_gc = false;
bool gc_scavenge = false;
std::string tmpfs_dir("/dev/shm");
int enable_safesnap = 0;
uint64_t ssn_read_opt_threshold = SSN_READ_OPT_DISABLED;
uint64_t log_buffer_kb = 8;
uint64_t log_segment_mb = 16384;
std::string log_dir("");
bool null_log_device = false;
bool enable_s3 = false;
bool default_flusher = false;
uint32_t optimize_dequeue = 1;
uint64_t ssd_buffer_gb = 0;
uint64_t buffer_hit_rate = 0;
bool enable_uring = true;
std::vector<std::string> s3_bucket_names;
bool is_general_bucket = false;
bool dependency_aware = false;
uint64_t max_appends = ~0;
bool sync_io = false;
bool truncate_at_bench_start = false;
bool htt_is_on = true;
bool physical_workers_only = true;
bool physical_io_workers_only = true;
bool print_cpu_util = false;
bool enable_perf = false;
std::string perf_record_event("");
uint64_t node_memory_gb = 12;
int recovery_warm_up_policy = WARM_UP_NONE;
bool pcommit = false;
uint32_t pcommit_queue_length = 50000;
uint32_t pcommit_timeout_ms = 1000;
bool pcommit_thread = false;
bool log_key_for_update = false;
bool enable_chkpt = 0;
uint64_t chkpt_interval = 50;
bool phantom_prot = 0;
double cycles_per_byte = 0;
uint32_t state = kStateLoading;
uint32_t threads = 0;
bool index_probe_only = false;
bool numa_spread = false;
bool kStateRunning = false;
bool iouring_read_log = true;
bool log_direct_io = true;
bool log_compress = true;
uint64_t fetch_cold_tx_interval = 0;
uint32_t loaders = 0;

uint32_t flusher_thread = 0;
uint32_t n_combine_log = 1;

// debug
bool finish_loading = false;

void init() {
  ALWAYS_ASSERT(threads);
  // Here [threads] refers to worker threads, so use the number of physical
  // cores to calculate # of numa nodes
  if (numa_spread) {
    numa_nodes = threads > numa_max_node() + 1 ? numa_max_node() + 1 : threads;
  } else {
    uint32_t max = thread::cpu_cores.size() / (numa_max_node() + 1);
    numa_nodes = (threads + max - 1) / max;
    ALWAYS_ASSERT(numa_nodes);
  }

  LOG(INFO) << "Workloads may run on " << numa_nodes << " nodes";
}

void sanity_check() {
  LOG_IF(FATAL, tls_alloc && !threadpool)
      << "Cannot use TLS allocator without threadpool";
  // ALWAYS_ASSERT(recover_functor);
  ALWAYS_ASSERT(numa_nodes || !threadpool);
  ALWAYS_ASSERT(!pcommit || pcommit_queue_length);
}

} // namespace config
} // namespace ermia
