#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <utility>
#include <string>
#include <chrono>

#include <time.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/sysinfo.h>
#include <sys/times.h>
#include <sys/wait.h>

#include "bench.h"

#include "../dbcore/sm-config.h"
#include "../dbcore/sm-table.h"

volatile bool running = true;
volatile uint64_t committed_txn_count = 0;

std::vector<bench_worker *> bench_runner::workers;

void bench_worker::do_workload_function(uint32_t i) {
  ASSERT(workload.size());
retry:
  util::timer t;
  const unsigned long old_seed = r.get_seed();
  const auto ret = workload[i].fn(this);
  if (finish_workload(ret, i, t)) {
    r.set_seed(old_seed);
    goto retry;
  }
}

uint32_t bench_worker::fetch_workload() {
  if (fetch_cold_tx_interval) {
    if (fetch_cold_tx_interval == ermia::config::fetch_cold_tx_interval) {
      fetch_cold_tx_interval--;
      return 1;
    } else {
      if (fetch_cold_tx_interval == 1) {
        fetch_cold_tx_interval = ermia::config::fetch_cold_tx_interval;
      } else {
        fetch_cold_tx_interval--;
      }
      return 0;
    }
  } else {
    double d = r.next_uniform();
    for (size_t i = 0; i < workload.size(); i++) {
      if ((i + 1) == workload.size() || d < workload[i].frequency) {
          return i;
      }
      d -= workload[i].frequency;
    }
  }

  // unreachable
  return 0;
}

bool bench_worker::finish_workload(rc_t ret, uint32_t workload_idx, util::timer &t) {
  if (!ret.IsAbort()) {
    if (ermia::config::benchmark_transactions &&
        __atomic_fetch_add(&committed_txn_count, 1, __ATOMIC_ACQ_REL) >= ermia::config::benchmark_transactions) {
      running = false;
      return false;
    }

    ++ntxn_commits;
    std::get<0>(txn_counts[workload_idx])++;
    if (!ermia::config::pcommit) {
      latency_numer_us += t.lap_us();
    }
    backoff_shifts >>= 1;
  } else {
    ++ntxn_aborts;
    std::get<1>(txn_counts[workload_idx])++;
    if (ret._val == RC_ABORT_USER) {
      std::get<3>(txn_counts[workload_idx])++;
    } else {
      std::get<2>(txn_counts[workload_idx])++;
    }
    switch (ret._val) {
      case RC_ABORT_SERIAL:
        inc_ntxn_serial_aborts();
        break;
      case RC_ABORT_SI_CONFLICT:
        inc_ntxn_si_aborts();
        break;
      case RC_ABORT_RW_CONFLICT:
        inc_ntxn_rw_aborts();
        break;
      case RC_ABORT_INTERNAL:
        inc_ntxn_int_aborts();
        break;
      case RC_ABORT_PHANTOM:
        inc_ntxn_phantom_aborts();
        break;
      case RC_ABORT_USER:
        inc_ntxn_user_aborts();
        break;
      default:
        ALWAYS_ASSERT(false);
    }
    if (ermia::config::retry_aborted_transactions && !ret.IsUserAbort() && running) {
      if (ermia::config::backoff_aborted_transactions) {
        if (backoff_shifts < 63) backoff_shifts++;
        uint64_t spins = 1UL << backoff_shifts;
        spins *= 100;  // XXX: tuned pretty arbitrarily
        while (spins) {
          NOP_PAUSE;
          spins--;
        }
      }
      return true;
    }
  }
  return false;
}

void bench_worker::MyWork(char *) {
  auto start_total = std::chrono::steady_clock::now();
  auto start_on_cpu = get_thread_cpu_time();

  if (is_worker) {
    tlog = ermia::GetLog();
    workload = get_workload();
    txn_counts.resize(workload.size());
    barrier_a->count_down();
    barrier_b->wait_for();

    while (running) {
      uint32_t workload_idx = fetch_workload();
      do_workload_function(workload_idx);
    }
  }

  auto end_on_cpu = get_thread_cpu_time();
  auto end_total = std::chrono::steady_clock::now();
  total_time += std::chrono::duration_cast<std::chrono::duration<double>>(end_total - start_total).count();
  total_on_cpu_time += end_on_cpu - start_on_cpu;
}

void bench_runner::run() {
  // Get a thread to use benchmark-provided prepare(), which gathers
  // information about index pointers created by create_file_task.
  ermia::thread::Thread::Task runner_task =
    std::bind(&bench_runner::prepare, this, std::placeholders::_1);
  ermia::thread::Thread *runner_thread = ermia::thread::GetThread(true /* physical */);
  runner_thread->StartTask(runner_task);
  runner_thread->Join();
  ermia::thread::PutThread(runner_thread);

  // load data, unless we recover from logs or is a backup server (recover from
  // shipped logs)
  ermia::volatile_write(ermia::config::state, ermia::config::kStateLoading);
  std::vector<bench_loader *> loaders = make_loaders();
  {
    util::scoped_timer t("dataloading");
    uint32_t done = 0;
    uint32_t n_running = 0;
  process:
    for (uint i = 0; i < loaders.size(); i++) {
      auto *loader = loaders[i];
      if (loader && !loader->IsImpersonated() && n_running < ermia::config::loaders && 
          loader->TryImpersonate()) {
        loader->Start();
        ++n_running;
      }
    }

    // Loop over existing loaders to scavenge and reuse available threads
    while (done < loaders.size()) {
      for (uint i = 0; i < loaders.size(); i++) {
        auto *loader = loaders[i];
        if (loader && loader->IsImpersonated() && loader->TryJoin()) {
          delete loader;
          loaders[i] = nullptr;
          done++;
          --n_running;
          goto process;
        }
      }
    }
  }

  ermia::volatile_write(ermia::MM::safesnap_lsn, ermia::dlog::current_csn);
  ALWAYS_ASSERT(ermia::MM::safesnap_lsn);

  // Persist the database
  ermia::dlog::flush_all();
  if (ermia::config::pcommit) {
    ermia::dlog::dequeue_committed_xcts();
    // Sanity check to make sure all transactions are fully committed
    for (auto &tlog : ermia::dlog::tlogs) {
      LOG_IF(FATAL, tlog->get_commit_queue_size() > 0);
    }
  }

  // if (ermia::config::enable_chkpt) {
  //  ermia::chkptmgr->do_chkpt();  // this is synchronous
  // }

  /*
  // Start checkpointer after database is ready
  if (ermia::config::enable_chkpt) {
    ermia::chkptmgr->start_chkpt_thread();
  }
  */
  ermia::volatile_write(ermia::config::state, ermia::config::kStateForwardProcessing);
  for(auto& log: ermia::dlog::tlogs){
    log->reset_latency();
    log->reset_flushcount();
  }

  if (ermia::config::worker_threads) {
    start_measurement();
  }
}

void bench_runner::start_measurement() {
  workers = make_workers();
  ALWAYS_ASSERT(!workers.empty());
  for (std::vector<bench_worker *>::const_iterator it = workers.begin();
       it != workers.end(); ++it) {
    while (!(*it)->IsImpersonated()) {
      (*it)->TryImpersonate();
    }
    (*it)->Start();
  }

  pid_t perf_pid;
  if (ermia::config::enable_perf) {
    std::cerr << "start perf..." << std::endl;

    std::stringstream parent_pid;
    parent_pid << getpid();

    pid_t pid = fork();
    // Launch profiler
    if (pid == 0) {
      if(ermia::config::perf_record_event != "") {
        exit(execl("/usr/bin/perf","perf","record", "-F", "99", "-e", ermia::config::perf_record_event.c_str(),
                   "-p", parent_pid.str().c_str(), nullptr));
      } else {
        exit(execl("/usr/bin/perf","perf","stat", "-B", "-e",  "cache-references,cache-misses,cycles,instructions,branches,faults",
                   "-p", parent_pid.str().c_str(), nullptr));
      }
    } else {
      perf_pid = pid;
    }
  }

  barrier_a.wait_for();  // wait for all threads to start up
  std::map<std::string, size_t> table_sizes_before;
  for (std::map<std::string, ermia::UnorderedIndex *>::iterator it = open_tables.begin();
       it != open_tables.end(); ++it) {
    const size_t s = it->second->Size();
    std::cerr << "table " << it->first << " size " << s << std::endl;
    table_sizes_before[it->first] = s;
  }
  std::cerr << "starting benchmark..." << std::endl;

  // Print some results every second
  uint64_t slept = 0;
  uint64_t last_commits = 0, last_aborts = 0;

  // Print CPU utilization as well. Code adapted from:
  // https://stackoverflow.com/questions/63166/how-to-determine-cpu-and-memory-consumption-from-inside-a-process
  FILE* file;
  struct tms timeSample;
  char line[128];

  clock_t lastCPU = times(&timeSample);
  clock_t lastSysCPU = timeSample.tms_stime;
  clock_t lastUserCPU = timeSample.tms_utime;
  uint32_t nprocs = std::thread::hardware_concurrency();

  file = fopen("/proc/cpuinfo", "r");
  fclose(file);

  auto get_cpu_util = [&]() {
    ASSERT(ermia::config::print_cpu_util);
    struct tms timeSample;
    clock_t now;
    double percent;

    now = times(&timeSample);
    if (now <= lastCPU || timeSample.tms_stime < lastSysCPU ||
      timeSample.tms_utime < lastUserCPU){
      percent = -1.0;
    }
    else{
      percent = (timeSample.tms_stime - lastSysCPU) +
      (timeSample.tms_utime - lastUserCPU);
      percent /= (now - lastCPU);
      percent /= nprocs;
      percent *= 100;
    }
    lastCPU = now;
    lastSysCPU = timeSample.tms_stime;
    lastUserCPU = timeSample.tms_utime;
    return percent;
  };

  util::timer t;
  barrier_b.count_down();

  double total_util = 0;
  double sec_util = 0;
  volatile uint64_t total_commits = 0;
  if (!ermia::config::benchmark_transactions) {
    // Time-based benchmark.
    if (ermia::config::print_cpu_util) {
      printf("Sec,Commits,Aborts,CPU\n");
    } else {
      printf("Sec,Commits,Aborts\n");
    }

    auto gather_stats = [&]() {
      sleep(1);
      uint64_t sec_commits = 0, sec_aborts = 0;
      for (size_t i = 0; i < ermia::config::worker_threads; i++) {
        sec_commits += workers[i]->get_ntxn_commits();
        sec_aborts += workers[i]->get_ntxn_aborts();
      }
      sec_commits -= last_commits;
      sec_aborts -= last_aborts;
      last_commits += sec_commits;
      last_aborts += sec_aborts;

      if (ermia::config::print_cpu_util) {
        sec_util = get_cpu_util();
        total_util += sec_util;
        printf("%lu,%lu,%lu,%.2f%%\n", slept + 1, sec_commits, sec_aborts, sec_util);
      } else {
        printf("%lu,%lu,%lu\n", slept + 1, sec_commits, sec_aborts);
      }
      slept++;
    };

    // Backups run forever until told to stop.
    while (slept < ermia::config::benchmark_seconds) {
      gather_stats();
    }

    running = false;
  } else {
    // Operation-based benchmark.
#if 0
    auto gather_stats = [&]() {
      sleep(1);
      total_commits = 0;
      for (size_t i = 0; i < ermia::config::worker_threads; i++) {
        total_commits += workers[i]->get_ntxn_commits();
      }
      printf("%lu,%.2f%%\n", slept + 1, 100 * double(total_commits) / ermia::config::benchmark_transactions);
      slept++;
    };
#endif

#if 0
    while (total_commits < ermia::config::benchmark_transactions) {
      //gather_stats();
      total_commits = 0;
      for (size_t i = 0; i < ermia::config::worker_threads; i++) {
        total_commits += workers[i]->get_ntxn_commits();
      }
    }

    running = false;
#endif
    while (ermia::volatile_read(running)) {}
  }

  ermia::volatile_write(ermia::config::state, ermia::config::kStateShutdown);
  for (size_t i = 0; i < ermia::config::worker_threads; i++) {
    workers[i]->Join();
  }

  const unsigned long elapsed_us = t.lap_us();
  ermia::dlog::flush_all();

  if (ermia::config::enable_perf) {
    std::cerr << "stop perf..." << std::endl;
    kill(perf_pid, SIGINT);
    waitpid(perf_pid, nullptr, 0);
  }

  size_t n_commits = 0;
  size_t n_aborts = 0;
  size_t n_user_aborts = 0;
  size_t n_int_aborts = 0;
  size_t n_si_aborts = 0;
  size_t n_serial_aborts = 0;
  size_t n_rw_aborts = 0;
  size_t n_phantom_aborts = 0;
  size_t n_query_commits = 0;
  double latency_numer_us = 0;
  double total_time = 0;
  double on_cpu_time = 0;
  if (ermia::config::pcommit) {
    for (auto &log : ermia::dlog::tlogs) {
      latency_numer_us += (double)log->get_latency_ns() / 1000.0;
    }
  }
  for (size_t i = 0; i < ermia::config::worker_threads; i++) {
    n_commits += workers[i]->get_ntxn_commits();
    n_aborts += workers[i]->get_ntxn_aborts();
    n_int_aborts += workers[i]->get_ntxn_int_aborts();
    n_user_aborts += workers[i]->get_ntxn_user_aborts();
    n_si_aborts += workers[i]->get_ntxn_si_aborts();
    n_serial_aborts += workers[i]->get_ntxn_serial_aborts();
    n_rw_aborts += workers[i]->get_ntxn_rw_aborts();
    n_phantom_aborts += workers[i]->get_ntxn_phantom_aborts();
    n_query_commits += workers[i]->get_ntxn_query_commits();
    if (!ermia::config::pcommit) {
      latency_numer_us += workers[i]->get_latency_numer_us();
    }
    on_cpu_time += workers[i]->total_on_cpu_time;
    total_time += workers[i]->total_time;
/*
    cpu_time += workers[i]->get_log()->cpu_util;
    std::cout << workers[i]->get_log()->get_pct(0.1) << std::endl; 
    std::cout << workers[i]->get_log()->get_pct(0.5) << std::endl; 
    std::cout << workers[i]->get_log()->get_pct(0.8) << std::endl; 
    std::cout << workers[i]->get_log()->get_pct(0.9) << std::endl;
    std::cout << workers[i]->get_log()->get_pct(0.95) << std::endl;
    std::cout << workers[i]->get_log()->get_pct(0.99) << std::endl;
    std::cout << workers[i]->get_log()->get_pct(1) << std::endl;
*/
  }
  double cpu_util = on_cpu_time * 100 / total_time;
  double p50_latency_ms = 0;
  double p90_latency_ms = 0;
  double p95_latency_ms = 0;
  double p99_latency_ms = 0;
  double p999_latency_ms = 0;
  double p9999_latency_ms = 0;
  double max_latency_ms = 0;
  double min_latency_ms = std::numeric_limits<double>::max();
  uint64_t max_latency_ns = 0;
  uint64_t min_latency_ns = std::numeric_limits<uint64_t>::max();
  uint64_t dequeue_count = 0;

  double avg_logbuf_fill_time_ms = 0;
  uint64_t tot_logbuf_fill_time_us = 0;
  uint64_t tot_logbuf_fill_nb_times = 0;
  uint64_t flush_count = 0;

  if (ermia::config::pcommit) {
    std::vector<uint64_t> latencies;
    for (auto &worker : workers) {
      latencies.insert(latencies.end(), 
        worker->get_log()->get_sampling_latencies().begin(), 
        worker->get_log()->get_sampling_latencies().end());
      max_latency_ns = std::max(max_latency_ns, worker->get_log()->get_max_latency_ns());
      min_latency_ns = std::min(min_latency_ns, worker->get_log()->get_min_latency_ns());
    }
    for (auto &log : ermia::dlog::tlogs) {
      dequeue_count += log->tcommitter.dequeue_count;
      tot_logbuf_fill_time_us += log->fill_buf_tot_duration_us;
      tot_logbuf_fill_nb_times += log->fill_buf_nb_times;
      flush_count += log->flush_count;
    }

    std::sort(latencies.begin(), latencies.end());
    p50_latency_ms = latencies[latencies.size() * 0.5] / 1000000.0;
    p90_latency_ms = latencies[latencies.size() * 0.9] / 1000000.0;
    p95_latency_ms = latencies[latencies.size() * 0.95] / 1000000.0;
    p99_latency_ms = latencies[latencies.size() * 0.99] / 1000000.0;
    p999_latency_ms = latencies[latencies.size() * 0.999] / 1000000.0;
    p9999_latency_ms = latencies[latencies.size() * 0.9999] / 1000000.0;
    max_latency_ms = (double)max_latency_ns / 1000000.0;
    min_latency_ms = (double)min_latency_ns / 1000000.0;
    avg_logbuf_fill_time_ms = (double)(tot_logbuf_fill_time_us/(tot_logbuf_fill_nb_times*1000.0));
  }

  const double elapsed_sec = double(elapsed_us) / 1000000.0;
  const double agg_throughput = double(n_commits) / elapsed_sec;
  const double avg_per_core_throughput =
      agg_throughput / double(workers.size());

  const double agg_abort_rate = double(n_aborts) / elapsed_sec;
  const double avg_per_core_abort_rate =
      agg_abort_rate / double(workers.size());

  const double agg_system_abort_rate =
      double(n_aborts - n_user_aborts) / elapsed_sec;
  const double agg_user_abort_rate = double(n_user_aborts) / elapsed_sec;
  const double agg_int_abort_rate = double(n_int_aborts) / elapsed_sec;
  const double agg_si_abort_rate = double(n_si_aborts) / elapsed_sec;
  const double agg_serial_abort_rate = double(n_serial_aborts) / elapsed_sec;
  const double agg_phantom_abort_rate = double(n_phantom_aborts) / elapsed_sec;
  const double agg_rw_abort_rate = double(n_rw_aborts) / elapsed_sec;

  const double avg_latency_us = double(latency_numer_us) / double(n_commits);
  const double avg_latency_ms = avg_latency_us / 1000.0;
  uint64_t agg_latency_us = 0;
  uint64_t agg_redo_batches = 0;
  uint64_t agg_redo_size = 0;

  const double agg_replay_latency_ms = agg_latency_us / 1000.0;

  tx_stat_map agg_txn_counts = workers[0]->get_txn_counts();
  for (size_t i = 1; i < workers.size(); i++) {
    auto &c = workers[i]->get_txn_counts();
    for (auto &t : c) {
      std::get<0>(agg_txn_counts[t.first]) += std::get<0>(t.second);
      std::get<1>(agg_txn_counts[t.first]) += std::get<1>(t.second);
      std::get<2>(agg_txn_counts[t.first]) += std::get<2>(t.second);
      std::get<3>(agg_txn_counts[t.first]) += std::get<3>(t.second);
    }
  }

  //if (ermia::config::enable_chkpt) {
  //  delete ermia::chkptmgr;
  //}

  std::cerr << "--- table statistics ---" << std::endl;
  for (std::map<std::string, ermia::UnorderedIndex *>::iterator it = open_tables.begin();
       it != open_tables.end(); ++it) {
    const size_t s = it->second->Size();
    const ssize_t delta = ssize_t(s) - ssize_t(table_sizes_before[it->first]);
    std::cerr << "table " << it->first << " size " << it->second->Size();
    if (delta < 0)
      std::cerr << " (" << delta << " records)" << std::endl;
    else
      std::cerr << " (+" << delta << " records)" << std::endl;
  }
  std::cerr << "--- benchmark statistics ---" << std::endl;
  std::cerr << "runtime: " << elapsed_sec << " sec" << std::endl;
  std::cerr << "agg_throughput: " << agg_throughput << " ops/sec" << std::endl;
  std::cerr << "avg_per_core_throughput: " << avg_per_core_throughput
       << " ops/sec/core" << std::endl;
  std::cerr << "avg_latency: " << avg_latency_ms << " ms" << std::endl;
  std::cerr << "agg_abort_rate: " << agg_abort_rate << " aborts/sec" << std::endl;
  std::cerr << "avg_per_core_abort_rate: " << avg_per_core_abort_rate
       << " aborts/sec/core" << std::endl;
  if (ermia::config::pcommit) {
    std::cerr << "min_latency: " << min_latency_ms << " ms" << std::endl;
    std::cerr << "p50_latency: " << p50_latency_ms << " ms" << std::endl;
    std::cerr << "p90_latency: " << p90_latency_ms << " ms" << std::endl;
    std::cerr << "p95_latency: " << p95_latency_ms << " ms" << std::endl;
    std::cerr << "p99_latency: " << p99_latency_ms << " ms" << std::endl;
    std::cerr << "p999_latency: " << p999_latency_ms << " ms" << std::endl;
    std::cerr << "p9999_latency: " << p9999_latency_ms << " ms" << std::endl;
    std::cerr << "max_latency: " << max_latency_ms << " ms" << std::endl;
    std::cerr << "avg_logbuf_fill_time: " << avg_logbuf_fill_time_ms << " ms" << std::endl;
  }
  std::cerr << "Dequeue count: " << dequeue_count << std::endl;
  std::cerr << "Flush count: " << flush_count << std::endl;
  // output for plotting script
  std::cout << "---------------------------------------\n";
  std::cout << "CPU util: " << cpu_util << "%" << std::endl;
  std::cout << agg_throughput << " commits/s, "
       //       << avg_latency_ms << " "
       << agg_abort_rate << " total_aborts/s, " << agg_system_abort_rate
       << " system_aborts/s, " << agg_user_abort_rate << " user_aborts/s, "
       << agg_int_abort_rate << " internal aborts/s, " << agg_si_abort_rate
       << " si_aborts/s, " << agg_serial_abort_rate << " serial_aborts/s, "
       << agg_rw_abort_rate << " rw_aborts/s, " << agg_phantom_abort_rate
       << " phantom aborts/s." << std::endl;
  std::cout << n_commits << " commits, " << n_query_commits << " query_commits, "
       << n_aborts << " total_aborts, " << n_aborts - n_user_aborts
       << " system_aborts, " << n_user_aborts << " user_aborts, "
       << n_int_aborts << " internal_aborts, " << n_si_aborts << " si_aborts, "
       << n_serial_aborts << " serial_aborts, " << n_rw_aborts << " rw_aborts, "
       << n_phantom_aborts << " phantom_aborts" << std::endl;

  std::cout << "---------------------------------------\n";
  for (auto &c : agg_txn_counts) {
    std::cout << c.first << "\t" << std::get<0>(c.second) / (double)elapsed_sec
         << " commits/s\t" << std::get<1>(c.second) / (double)elapsed_sec
         << " aborts/s\t" << std::get<2>(c.second) / (double)elapsed_sec
         << " system aborts/s\t" << std::get<3>(c.second) / (double)elapsed_sec
         << " user aborts/s\n";
  }
  std::cout.flush();
}

template <typename K, typename V>
struct map_maxer {
  typedef std::map<K, V> map_type;
  void operator()(map_type &agg, const map_type &m) const {
    for (typename map_type::const_iterator it = m.begin(); it != m.end(); ++it)
      agg[it->first] = std::max(agg[it->first], it->second);
  }
};

const tx_stat_map bench_worker::get_txn_counts() const {
  tx_stat_map m;
  const workload_desc_vec workload = get_workload();
  for (size_t i = 0; i < txn_counts.size(); i++)
    m[workload[i].name] = txn_counts[i];
  return m;
}

