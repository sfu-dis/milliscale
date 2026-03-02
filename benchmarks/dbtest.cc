#include <gflags/gflags.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>

#include "dbtest.h"

#if defined(SSI) && defined(SSN)
#error "SSI + SSN?"
#endif

DEFINE_uint32(loaders, 0,
              "Number of loaders; overrides the default value of total number "
              "of threads in a socket");
DEFINE_bool(threadpool, true,
            "Whether to use ERMIA thread pool (no oversubscription)");
DEFINE_uint64(arena_size_mb, 8,
              "Size of transaction arena (private workspace) in MB");
DEFINE_bool(tls_alloc, true,
            "Whether to use the TLS allocator defined in sm-alloc.h");
DEFINE_bool(htt, true,
            "Whether the HW has hyper-threading enabled."
            "Ignored if auto-detection of physical cores succeeded.");
DEFINE_bool(
    physical_workers_only, true,
    "Whether to only use one thread per physical core as transaction workers.");
DEFINE_bool(physical_io_workers_only, true,
            "Whether to only use one thread per physical core as cold "
            "transaction workers.");
DEFINE_bool(index_probe_only, false,
            "Whether the read is only probing into index");
DEFINE_uint64(threads, 1, "Number of worker threads to run transactions.");
DEFINE_uint64(io_threads, 0, "Number of worker threads to run io operations.");
DEFINE_uint64(remote_threads, 0,
              "Number of worker threads to run remote operations.");
DEFINE_uint64(node_memory_gb, 16, "GBs of memory to allocate per node.");
DEFINE_bool(numa_spread, false,
            "Whether to pin threads in spread mode (compact if false)");
DEFINE_string(tmpfs_dir, "/dev/shm",
              "Path to a tmpfs location. Used by log buffer.");
DEFINE_string(log_data_dir, "/tmpfs/ermia-log", "Log directory.");
DEFINE_uint64(log_buffer_kb, 8, "Log buffer size in MB.");
DEFINE_uint64(log_segment_mb, 16384, "Log segment size in MB.");
DEFINE_bool(log_direct_io, true, "Whether to use O_DIRECT for dlog.");
DEFINE_bool(log_compress, false, "Whether to compress the log upon flush.");
DEFINE_bool(phantom_prot, false, "Whether to enable phantom protection.");
DEFINE_bool(print_cpu_util, false, "Whether to print CPU utilization.");
DEFINE_bool(enable_perf, false,
            "Whether to run Linux perf along with benchmark.");
DEFINE_string(perf_record_event, "n/a", "Perf record event");
#if defined(SSN) || defined(SSI)
DEFINE_bool(safesnap, false,
            "Whether to use the safe snapshot (for SSI and SSN only).");
#endif
#ifdef SSN
DEFINE_string(ssn_read_opt_threshold, "0xFFFFFFFFFFFFFFFF",
              "Threshold for SSN's read optimization."
              "0 - don't track reads at all;"
              "0xFFFFFFFFFFFFFFFF - track all reads.");
#endif
#ifdef SSI
DEFINE_bool(ssi_read_only_opt, false,
            "Whether to enable SSI's read-only optimization."
            "Note: this is **not** safe snapshot.");
#endif

// Options specific to the primary
DEFINE_uint64(seconds, 10, "Duration to run benchmark in seconds.");
DEFINE_uint64(transactions, 0, "Number of transactions to finish in each run.");
DEFINE_bool(parallel_loading, true, "Load data in parallel.");
DEFINE_bool(retry_aborted_transactions, false,
            "Whether to retry aborted transactions.");
DEFINE_bool(backoff_aborted_transactions, false,
            "Whether backoff when retrying.");
DEFINE_string(
    recovery_warm_up, "none",
    "Method to load tuples during recovery:"
    "none - don't load anything; lazy - load tuples using a background thread; "
    "eager - load everything to memory during recovery.");
DEFINE_bool(enable_chkpt, false, "Whether to enable checkpointing.");
DEFINE_uint64(chkpt_interval, 10, "Checkpoint interval in seconds.");
DEFINE_bool(null_log_device, false, "Whether to skip writing log records.");
DEFINE_bool(enable_s3, false, "Whether to enable S3.");
DEFINE_bool(default_flusher, false,
            "Whether to use default flusher thread(awssdk).");
DEFINE_uint32(optimize_dequeue, 1,
            "Optimize the false dependency in commit queue, 0: use txn commit csn to dequeue, 1: use max dep csn to dequeue, 2: use txn start_timestamp to dequeue.");
DEFINE_uint64(ssd_buffer_gb, 0, "Using ssd as buffer, buffer size in GB");
DEFINE_uint64(buffer_hit_rate, 0, "Hit rate for SSD buffer");
DEFINE_string(s3_bucket_names, "[sfu-hello-world--use1-az6--x-s3]",
              "Bucket name for S3, can be general or directory bucket");
DEFINE_bool(
    is_general_bucket, false,
    "Whether the bucket is general bucket, if not then it is directory bucket");
// Example: in the queue
// 10, 11 (upto_csn), 13 (local), 14 (local), 15 (local), 16 (remote), 17 (local)
// then I can commit 10-15, because from 11 to 
DEFINE_bool(dependency_aware, false, "Whether to put log into the same log as it's dependency, will also do a local dequeue optimization.");


DEFINE_bool(sync_io, false, "Whether to use sync IO");
DEFINE_bool(truncate_at_bench_start, false,
            "Whether truncate the log/chkpt file written before starting "
            "benchmark (save tmpfs space).");
DEFINE_bool(log_key_for_update, false,
            "Whether to store the key in update log records.");
// Group (pipelined) commit related settings. The daemon will flush the log
// buffer
// when the following happens, whichever is earlier:
// 1. queue is full; 2. the log buffer is half full; 3. after [timeout] seconds.
DEFINE_bool(pcommit, true, "Whether to enable pipelined commit.");
DEFINE_uint64(pcommit_queue_length, 50000, "Pipelined commit queue length");
DEFINE_uint64(pcommit_timeout_ms, 1000,
              "Pipelined commit flush interval (in milliseconds).");
DEFINE_bool(pcommit_thread, false,
            "Whether to use a dedicated pipelined committer thread.");
DEFINE_bool(enable_gc, false, "Whether to enable garbage collection.");
DEFINE_bool(gc_scavenge, false,
            "Whether to scavenge the oldest version upon update first.");
DEFINE_bool(iouring_read_log, true,
            "Whether to use iouring to load versions from logs.");
DEFINE_uint64(
    fetch_cold_tx_interval, 0,
    "The interval of fetching cold transactions measured by # of transactions");

DEFINE_uint32(flusher_thread, 0, "Using flusher thread to send requests.");
DEFINE_uint32(n_combine_log, 1, "n workers using 1 log");

static std::vector<std::string> split_ws(const std::string &s) {
  std::vector<std::string> r;
  std::istringstream iss(s);
  copy(std::istream_iterator<std::string>(iss),
       std::istream_iterator<std::string>(),
       std::back_inserter<std::vector<std::string>>(r));
  return r;
}

std::vector<std::string> parse_bucketnames(const std::string &s) {
  std::vector<std::string> result;
  if (s.size() < 2 || s.front() != '[' || s.back() != ']') {
    return result; // if format is incorrect
  }

  std::string inner = s.substr(1, s.size() - 2); // remove []
  std::stringstream ss(inner);
  std::string item;
  while (std::getline(ss, item, ',')) {
    // remove extra space
    item.erase(remove_if(item.begin(), item.end(), ::isspace), item.end());
    if (!item.empty()) {
      result.push_back(item);
    }
  }
  return result;
}

void bench_main(int argc, char **argv,
                std::function<void(ermia::Engine *)> test_fn) {
#ifndef NDEBUG
  std::cerr << "WARNING: benchmark built in DEBUG mode!!!" << std::endl;
#endif
  std::cerr << "PID: " << getpid() << std::endl;

  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  ermia::config::threadpool = FLAGS_threadpool;
  ermia::config::tls_alloc = FLAGS_tls_alloc;
  ermia::config::print_cpu_util = FLAGS_print_cpu_util;
  ermia::config::htt_is_on = FLAGS_htt;
  ermia::config::enable_perf = FLAGS_enable_perf;
  ermia::config::perf_record_event = FLAGS_perf_record_event;
  ermia::config::physical_workers_only = FLAGS_physical_workers_only;
  ermia::config::physical_io_workers_only = FLAGS_physical_io_workers_only;
  if (ermia::config::physical_workers_only)
    ermia::config::threads = FLAGS_threads;
  else
    ermia::config::threads = (FLAGS_threads + 1) / 2;

  ermia::config::index_probe_only = FLAGS_index_probe_only;
  ermia::config::node_memory_gb = FLAGS_node_memory_gb;
  ermia::config::numa_spread = FLAGS_numa_spread;
  ermia::config::tmpfs_dir = FLAGS_tmpfs_dir;
  ermia::config::log_dir = FLAGS_log_data_dir;
  ermia::config::log_buffer_kb = FLAGS_log_buffer_kb;
  ermia::config::log_segment_mb = FLAGS_log_segment_mb;
  ermia::config::log_direct_io = FLAGS_log_direct_io;
  ermia::config::log_compress = FLAGS_log_compress;

  if (FLAGS_log_direct_io) {
    // Log buffer must be 4KB aligned if enabled
    LOG_IF(FATAL, PAGE_SIZE & (4 * ermia::config::MB))
        << "PAGE_SIZE must be aligned to enable O_DIRECT.";
    LOG_IF(FATAL, FLAGS_log_buffer_kb * ermia::config::MB % PAGE_SIZE)
        << "Log buffer must be aligned to enable O_DIRECT.";
  }

  ermia::config::phantom_prot = FLAGS_phantom_prot;

#if defined(SSI) || defined(SSN)
  ermia::config::enable_safesnap = FLAGS_safesnap;
#endif
#ifdef SSI
  ermia::config::enable_ssi_read_only_opt = FLAGS_ssi_read_only_opt;
#endif
#ifdef SSN
  ermia::config::ssn_read_opt_threshold =
      strtoul(FLAGS_ssn_read_opt_threshold.c_str(), nullptr, 16);
#endif

  ermia::config::arena_size_mb = FLAGS_arena_size_mb;

  ermia::config::benchmark_seconds = FLAGS_seconds;
  ermia::config::benchmark_transactions = FLAGS_transactions;
  ermia::config::retry_aborted_transactions = FLAGS_retry_aborted_transactions;
  ermia::config::backoff_aborted_transactions =
      FLAGS_backoff_aborted_transactions;
  ermia::config::null_log_device = FLAGS_null_log_device;
  ermia::config::enable_s3 = FLAGS_enable_s3;
  ermia::config::default_flusher = FLAGS_default_flusher;
  ermia::config::optimize_dequeue = FLAGS_optimize_dequeue;
  ermia::config::ssd_buffer_gb = FLAGS_ssd_buffer_gb;
  ermia::config::buffer_hit_rate = FLAGS_buffer_hit_rate;
  ermia::config::enable_uring =
      (!ermia::config::enable_s3 && !ermia::config::null_log_device) ||
      (ermia::config::enable_s3 && ermia::config::buffer_hit_rate > 0);
  ermia::config::is_general_bucket = FLAGS_is_general_bucket;
  ermia::config::dependency_aware = FLAGS_dependency_aware;

  ermia::config::s3_bucket_names = parse_bucketnames(FLAGS_s3_bucket_names);
  LOG_IF(FATAL, ermia::config::s3_bucket_names.empty())
      << "s3_bucket_names is not in the correct format, should looks like "
         "[AAA, BBB]";
  LOG_IF(FATAL, (ermia::config::s3_bucket_names.size() < 1) ||
                    (ermia::config::s3_bucket_names.size() > 3))
      << "Must list 1-3 bucket names";

  if (FLAGS_enable_s3) {
    ermia::config::max_appends = FLAGS_is_general_bucket ? 1 : 10000;
  }
  ermia::config::sync_io = FLAGS_sync_io;
  ermia::config::truncate_at_bench_start = FLAGS_truncate_at_bench_start;

  ermia::config::worker_threads = FLAGS_threads;
  ermia::config::io_threads = FLAGS_io_threads;
  ermia::config::remote_threads = FLAGS_remote_threads;

  ermia::config::pcommit = FLAGS_pcommit;
  ermia::config::pcommit_queue_length = FLAGS_pcommit_queue_length;
  ermia::config::pcommit_timeout_ms = FLAGS_pcommit_timeout_ms;
  ermia::config::pcommit_thread = FLAGS_pcommit_thread;
  ermia::config::enable_chkpt = FLAGS_enable_chkpt;
  ermia::config::chkpt_interval = FLAGS_chkpt_interval;
  ermia::config::parallel_loading = FLAGS_parallel_loading;
  ermia::config::enable_gc = FLAGS_enable_gc;
  ermia::config::gc_scavenge = FLAGS_gc_scavenge;
  ermia::config::iouring_read_log = FLAGS_iouring_read_log;
  ermia::config::fetch_cold_tx_interval = FLAGS_fetch_cold_tx_interval;

  if (FLAGS_recovery_warm_up == "none") {
    ermia::config::recovery_warm_up_policy = ermia::config::WARM_UP_NONE;
  } else if (FLAGS_recovery_warm_up == "lazy") {
    ermia::config::recovery_warm_up_policy = ermia::config::WARM_UP_LAZY;
  } else if (FLAGS_recovery_warm_up == "eager") {
    ermia::config::recovery_warm_up_policy = ermia::config::WARM_UP_EAGER;
  } else {
    LOG(FATAL) << "Invalid recovery warm up policy: " << FLAGS_recovery_warm_up;
  }

  ermia::config::flusher_thread = FLAGS_flusher_thread;
  ermia::config::n_combine_log = FLAGS_n_combine_log;
  ermia::config::log_key_for_update = FLAGS_log_key_for_update;

  ermia::thread::Initialize();
  ermia::config::init();

  // Initialize the number of loaders - must happen after ermia::config::init()
  // which initializes ermia::config::numa_nodes
  ermia::config::loaders = FLAGS_loaders == 0
                               ? std::thread::hardware_concurrency() /
                                     (numa_max_node() + 1) / 2 *
                                     ermia::config::numa_nodes
                               : FLAGS_loaders;

  std::cerr << "CC: ";
#ifdef SSN
#ifdef RC
  std::cerr << "RC+SSN";
  std::cerr << "  safe snapshot          : " << ermia::config::enable_safesnap
            << std::endl;
  std::cerr << "  read opt threshold     : 0x" << std::hex
            << ermia::config::ssn_read_opt_threshold << std::dec << std::endl;
#else
  std::cerr << "SI+SSN";
  std::cerr << "  safe snapshot          : " << ermia::config::enable_safesnap
            << std::endl;
  std::cerr << "  read opt threshold     : 0x" << std::hex
            << ermia::config::ssn_read_opt_threshold << std::dec << std::endl;
#endif
#else
  std::cerr << "SI";
#endif
  std::cerr << std::endl;
  std::cerr << "  phantom-protection: " << ermia::config::phantom_prot
            << std::endl;

  std::cerr << "Settings and properties" << std::endl;
  std::cerr << "  arena-size-mb     : " << FLAGS_arena_size_mb << std::endl;
  std::cerr << "  enable-perf       : " << ermia::config::enable_perf
            << std::endl;
  std::cerr << "  garbage collection: " << ermia::config::enable_gc
            << std::endl;
  std::cerr << "  gc-scavenge       : " << ermia::config::gc_scavenge
            << std::endl;
  std::cerr << "  pipelined commit  : " << ermia::config::pcommit << std::endl;
  std::cerr << "  dedicated pcommit thread: " << ermia::config::pcommit_thread
            << std::endl;
  std::cerr << "  index-probe-only  : " << FLAGS_index_probe_only << std::endl;
  std::cerr << "  iouring-read-log  : " << FLAGS_iouring_read_log << std::endl;
  std::cerr << "  log-buffer-kb     : " << ermia::config::log_buffer_kb
            << std::endl;
  std::cerr << "  log-dir           : " << ermia::config::log_dir << std::endl;
  std::cerr << "  log-segment-mb    : " << ermia::config::log_segment_mb
            << std::endl;
  std::cerr << "  log-direct-io     : " << ermia::config::log_direct_io
            << std::endl;
  std::cerr << "  log-compress      : " << ermia::config::log_compress
            << std::endl;
  std::cerr << "  masstree_internal_node_size: "
            << ermia::ConcurrentMasstree::InternalNodeSize() << std::endl;
  std::cerr << "  masstree_leaf_node_size    : "
            << ermia::ConcurrentMasstree::LeafNodeSize() << std::endl;
  std::cerr << "  node-memory       : " << ermia::config::node_memory_gb << "GB"
            << std::endl;
  std::cerr << "  null-log-device   : " << ermia::config::null_log_device
            << std::endl;
  std::cerr << "  enable-s3         : " << ermia::config::enable_s3
            << std::endl;
  for (auto &s3_bucket_name : ermia::config::s3_bucket_names) {
    std::cerr << "  s3-bucket-name    : " << s3_bucket_name << std::endl;
  }
  std::cerr << "  s3-is-general-bucket    : "
            << ermia::config::is_general_bucket << std::endl;
  std::cerr << "  Dependency Aware  : " << ermia::config::dependency_aware << std::endl;
  std::cerr << "  Sync IO  : " << ermia::config::sync_io << std::endl;
  std::cerr << "  num-threads       : " << ermia::config::threads << std::endl;
  std::cerr << "  num-loaders       : " << ermia::config::loaders << std::endl;
  std::cerr << "  numa-nodes        : " << ermia::config::numa_nodes
            << std::endl;
  std::cerr << "  numa-mode         : "
            << (ermia::config::numa_spread ? "spread" : "compact") << std::endl;
  std::cerr << "  perf-record-event : " << ermia::config::perf_record_event
            << std::endl;
  std::cerr << "  physical-workers-only: "
            << ermia::config::physical_workers_only << std::endl;
  std::cerr << "  physical-io-workers-only: "
            << ermia::config::physical_io_workers_only << std::endl;
  std::cerr << "  print-cpu-util    : " << ermia::config::print_cpu_util
            << std::endl;
  std::cerr << "  threadpool        : " << ermia::config::threadpool
            << std::endl;
  std::cerr << "  tmpfs-dir         : " << ermia::config::tmpfs_dir
            << std::endl;
  std::cerr << "  tls-alloc         : " << FLAGS_tls_alloc << std::endl;
  std::cerr << "  total-threads     : " << ermia::config::threads << std::endl;
#ifdef USE_VARINT_ENCODING
  std::cerr << "  var-encode        : yes" << std::endl;
#else
  std::cerr << "  var-encode        : no" << std::endl;
#endif
  std::cerr << "  worker-threads    : " << ermia::config::worker_threads
            << std::endl;
  if (ermia::config::io_threads) {
    std::cerr << "  io-threads    : " << ermia::config::io_threads << std::endl;
  }

  if (ermia::config::remote_threads) {
    std::cerr << "  remote-threads    : " << ermia::config::remote_threads
              << std::endl;
  }

  system(("rm -rf " + FLAGS_log_data_dir + "/*").c_str());
  ermia::MM::prepare_node_memory();

  // Must have everything in config ready by this point
  ermia::config::sanity_check();
  ermia::Engine *db = new ermia::Engine();

  // FIXME(tzwang): the current thread doesn't belong to the thread pool, and
  // it could be on any node. But not all nodes will be used by benchmark
  // (i.e., config::numa_nodes) and so not all nodes will have memory pool. So
  // here run on the first NUMA node to ensure we got a place to allocate memory
  numa_run_on_node(0);
  test_fn(db);
  delete db;
}
