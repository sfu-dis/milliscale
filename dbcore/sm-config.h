#pragma once
#include <numa.h>

#include <iostream>
#include <string>
#include <thread>

#include "../third-party/foedus/uniform_random.hpp"
#include "sm-defs.h"

namespace ermia {

namespace config {

static const uint32_t MAX_THREADS = 96;
static const uint64_t KB = 1024;
static const uint64_t MB = KB * 1024;
static const uint64_t GB = MB * 1024;

// Common settings
extern bool tls_alloc;
extern bool threadpool;
extern std::string benchmark;
extern uint32_t threads;
extern uint32_t worker_threads;
extern uint32_t io_threads;
extern uint32_t remote_threads;
extern int numa_nodes;
extern bool numa_spread;
extern std::string tmpfs_dir;
extern bool htt_is_on;
extern bool physical_workers_only;
extern bool physical_io_workers_only;
extern uint32_t state;
extern bool enable_chkpt;
extern uint64_t chkpt_interval;
extern uint64_t log_buffer_kb;
extern uint64_t log_segment_mb;
extern std::string log_dir;
extern bool print_cpu_util;
extern uint32_t arena_size_mb;
extern bool enable_perf;
extern std::string perf_record_event;
extern bool kStateRunning;
extern bool iouring_read_log;
extern bool log_direct_io;
extern bool log_compress;
extern uint64_t node_memory_gb;
extern bool phantom_prot;
extern uint64_t fetch_cold_tx_interval;

extern bool parallel_loading;
extern bool retry_aborted_transactions;
extern int backoff_aborted_transactions;
extern bool enable_gc;
extern bool gc_scavenge;
extern bool null_log_device;
extern bool enable_s3;
extern bool default_flusher;
extern uint32_t optimize_dequeue;
extern uint64_t ssd_buffer_gb;
extern uint64_t buffer_hit_rate;
extern bool enable_uring;
extern std::vector<std::string> s3_bucket_names;
extern bool is_general_bucket;
extern bool dependency_aware;
extern uint64_t max_appends;
extern bool sync_io;
extern bool truncate_at_bench_start;
extern bool pcommit;
extern uint32_t pcommit_timeout_ms;
extern uint32_t pcommit_queue_length; // how much to reserve
extern bool pcommit_thread;
extern uint32_t loaders;

extern uint32_t benchmark_seconds;
extern uint64_t benchmark_transactions;

extern bool index_probe_only;
extern uint32_t flusher_thread;
extern uint32_t n_combine_log;

inline bool buff_hit() {
  thread_local foedus::assorted::UniformRandom uniform_rng;
  thread_local bool init = false;
  if (!init) {
    uniform_rng.set_current_seed(
        std::hash<std::thread::id>{}(std::this_thread::get_id()));
    init = true;
  }
  return uniform_rng.uniform_within(0, 99) < buffer_hit_rate;
}

enum SystemState { kStateLoading, kStateForwardProcessing, kStateShutdown };
inline bool IsLoading() { return volatile_read(state) == kStateLoading; }
inline bool IsForwardProcessing() {
  return volatile_read(state) == kStateForwardProcessing;
}
inline bool IsShutdown() { return volatile_read(state) == kStateShutdown; }

// Warm-up policy when recovering from a chkpt or the log.
// Set by --recovery-warm-up=[lazy/eager/whatever].
//
// lazy: spawn a thread to access every OID entry after recovery; log/chkpt
//       recovery will only oid_put objects that contain the records' log
//       location.
//       Tx's might encounter some storage-resident versions, if the tx tried
//       to
//       access them before the warm-up thread fetched those versions.
//
// eager: dig out versions from the log when scanning the chkpt and log; all
// OID
//        entries will point to some memory location after recovery finishes.
//        Txs will only see memory-residents, no need to dig them out during
//        execution.
//
// --recovery-warm-up ommitted or = anything else: don't do warm-up at all; it
//        is the tx's burden to dig out versions when accessing them.
enum WU_POLICY { WARM_UP_NONE, WARM_UP_LAZY, WARM_UP_EAGER };
extern int recovery_warm_up_policy; // no/lazy/eager warm-up at recovery

/* CC-related options */
extern uint64_t ssn_read_opt_threshold;
static const uint64_t SSN_READ_OPT_DISABLED = 0xffffffffffffffff;

// XXX(tzwang): enabling safesnap for tpcc basically halves the performance.
// perf says 30%+ of cycles are on oid_get_version, which makes me suspect
// it's because enabling safesnap makes the reader has to go deeper in the
// version chains to find the desired version. So perhaps don't enable this
// for update-intensive workloads, like tpcc. TPC-E to test and verify.
extern int enable_safesnap;

extern bool log_key_for_update;

extern double cycles_per_byte;

inline bool eager_warm_up() { return recovery_warm_up_policy == WARM_UP_EAGER; }

inline bool lazy_warm_up() { return recovery_warm_up_policy == WARM_UP_LAZY; }

void init();
void sanity_check();
inline bool ssn_read_opt_enabled() {
  return ssn_read_opt_threshold < SSN_READ_OPT_DISABLED;
}

} // namespace config
} // namespace ermia
