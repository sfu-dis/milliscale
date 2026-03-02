/*
 * A YCSB implementation based off of Silo's and equivalent to FOEDUS's.
 */
#include "../dbtest.h"
#include "ycsb.h"

extern YcsbWorkload ycsb_workload;

class ycsb_sequential_worker : public ycsb_base_worker {
 public:
  ycsb_sequential_worker(unsigned int worker_id, unsigned long seed, ermia::Engine *db,
                         const std::map<std::string, ermia::UnorderedIndex *> &open_tables,
                         spin_barrier *barrier_a, spin_barrier *barrier_b)
    : ycsb_base_worker(worker_id, seed, db, open_tables, barrier_a, barrier_b) {
  }

  virtual void MyWork(char *) override {
    auto start_total = std::chrono::steady_clock::now();
    auto start_on_cpu = get_thread_cpu_time();
    if (is_worker) {
      tlog = ermia::GetLog();
      workload = get_workload();
      txn_counts.resize(workload.size());
      LOG_IF(FATAL, ermia::config::io_threads + ermia::config::remote_threads > ermia::config::worker_threads) << "Not enough threads.";
      if (ermia::config::io_threads || ermia::config::remote_threads) {
        if (worker_id < ermia::config::io_threads) {
          workload = get_cold_workload();
        } else if (worker_id < ermia::config::io_threads + ermia::config::remote_threads) {
          workload = get_remote_workload();
        } else {
          workload = get_hot_workload();
        }
      }

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

  virtual workload_desc_vec get_workload() const override {
    workload_desc_vec w;

    if (ycsb_workload.read_percent()) {
      w.push_back(workload_desc("0-HotRead", FLAGS_ycsb_hot_tx_percent * double(ycsb_workload.read_percent()) / 100.0, TxnHotRead));
      w.push_back(workload_desc("1-ColdRead", (1 - FLAGS_ycsb_hot_tx_percent - FLAGS_ycsb_remote_tx_percent) * double(ycsb_workload.read_percent()) / 100.0, TxnRead));
    }

    if (ycsb_workload.rmw_percent()) {
      LOG_IF(FATAL, ermia::config::index_probe_only) << "Not supported";
      w.push_back(workload_desc("0-HotRMW", FLAGS_ycsb_hot_tx_percent * double(ycsb_workload.rmw_percent()) / 100.0, TxnHotRMW));
      w.push_back(workload_desc("1-ColdRMW", (1 - FLAGS_ycsb_hot_tx_percent - FLAGS_ycsb_remote_tx_percent) * double(ycsb_workload.rmw_percent()) / 100.0, TxnRMW));
    }

    if (ycsb_workload.scan_percent()) {
      LOG_IF(FATAL, ermia::config::index_probe_only) << "Not supported";
      w.push_back(workload_desc("Scan", double(ycsb_workload.scan_percent()) / 100.0, TxnScan));
    }

    if (ycsb_workload.insert_percent()) {
      w.push_back(workload_desc("0-Insert", double(ycsb_workload.insert_percent()) / 100.0, TxnInsert));
    }

    if (ycsb_workload.update_percent()) {
      w.push_back(workload_desc("0-HotUpdate", FLAGS_ycsb_hot_tx_percent * double(ycsb_workload.update_percent()) / 100.0, TxnHotUpdate));
      w.push_back(workload_desc("1-ColdUpdate", (1 - FLAGS_ycsb_hot_tx_percent) * double(ycsb_workload.update_percent()) / 100.0, TxnColdUpdate));
    }

    return w;
  }

  workload_desc_vec get_hot_workload() const  {
    workload_desc_vec w;
    if (ycsb_workload.read_percent()) {
      w.push_back(workload_desc("0-HotRead", 1, TxnHotRead));
      w.push_back(workload_desc("1-ColdRead", 0, TxnRead));
    }

    if (ycsb_workload.rmw_percent()) {
      LOG_IF(FATAL, ermia::config::index_probe_only) << "Not supported";
      w.push_back(workload_desc("0-HotRMW", 1, TxnHotRMW));
      w.push_back(workload_desc("1-ColdRMW", 0, TxnRMW));
    }

    return w;
  }

  workload_desc_vec get_cold_workload() const {
    workload_desc_vec w;
    if (ycsb_workload.read_percent()) {
      w.push_back(workload_desc("0-HotRead", 0, TxnHotRead));
      w.push_back(workload_desc("1-ColdRead", 1, TxnRead));
    }

    if (ycsb_workload.rmw_percent()) {
      LOG_IF(FATAL, ermia::config::index_probe_only) << "Not supported";
      w.push_back(workload_desc("0-HotRMW", 0, TxnHotRMW));
      w.push_back(workload_desc("1-ColdRMW", 1, TxnRMW));
    }

    return w;
  }

  workload_desc_vec get_remote_workload() const {
    LOG(FATAL) << "Not implemented.";
    workload_desc_vec w;
    return w;
  }

  static rc_t TxnRead(bench_worker *w) { return static_cast<ycsb_sequential_worker *>(w)->txn_read(); }
  static rc_t TxnHotRead(bench_worker *w) { return static_cast<ycsb_sequential_worker *>(w)->txn_hot_read(); }
  static rc_t TxnRMW(bench_worker *w) { return static_cast<ycsb_sequential_worker *>(w)->txn_rmw(); }
  static rc_t TxnHotRMW(bench_worker *w) { return static_cast<ycsb_sequential_worker *>(w)->txn_hot_rmw(); }
  static rc_t TxnScan(bench_worker *w) { return static_cast<ycsb_sequential_worker *>(w)->txn_scan(); }
  static rc_t TxnInsert(bench_worker *w) { return static_cast<ycsb_sequential_worker *>(w)->txn_insert(); }
  static rc_t TxnHotUpdate(bench_worker *w) { return static_cast<ycsb_sequential_worker *>(w)->txn_hot_update(); }
  static rc_t TxnColdUpdate(bench_worker *w) { return static_cast<ycsb_sequential_worker *>(w)->txn_cold_update(); }

  // Read transaction using traditional sequential execution
  rc_t txn_read() {
    ermia::transaction *txn = nullptr;

    if (ermia::config::index_probe_only) {
      // Reset the arena as txn will be nullptr and GenerateKey will get space from it
      arena->reset();
    } else {
      txn = db->NewTransaction(ermia::transaction::TXN_FLAG_READ_ONLY, *arena, txn_buf());
    }

    for (uint64_t i = 0; i < FLAGS_ycsb_ops_per_tx; ++i) {
      ermia::varstr &v = str((ermia::config::index_probe_only) ? 0 : sizeof(ycsb_kv::value));

      // TODO(tzwang): add read/write_all_fields knobs
      rc_t rc = rc_t{RC_INVALID};
      if (!ermia::config::index_probe_only) {
        bool hot = false;
        if (i < FLAGS_ycsb_cold_ops_per_tx) {
          hot = false;
        } else {
          hot = true;
        }
        auto &k = GenerateKey(txn, hot);
        table_index->GetRecord(txn, rc, k, v);  // Read
        ycsb_kv::value v_y_temp;
        const ycsb_kv::value *v_y = Decode(v, v_y_temp);
      } else {
        auto &k = GenerateKey(txn, true);
        ermia::OID oid = 0;
        ermia::ConcurrentMasstree::versioned_node_t sinfo;
        rc = (table_index->GetMasstree().search(k, oid, &sinfo)) ? RC_TRUE : RC_FALSE;
      }

#if defined(SSN)
      TryCatch(rc);
#else
      ALWAYS_ASSERT(rc._val == RC_TRUE);  // Under SI this must succeed
#endif

      if (!ermia::config::index_probe_only) {
        memcpy((char *)(&v) + sizeof(ermia::varstr), (char *)v.data(), sizeof(ycsb_kv::value));
      }
    }

    if (!ermia::config::index_probe_only) {
      TryCatch(db->Commit(txn));
    }

    return {RC_TRUE};
  }

  // Read hot data only
  rc_t txn_hot_read() {
    ermia::transaction *txn = nullptr;

    if (ermia::config::index_probe_only) {
      // Reset the arena as txn will be nullptr and GenerateKey will get space from it
      arena->reset();
    } else {
      txn = db->NewTransaction(ermia::transaction::TXN_FLAG_READ_ONLY, *arena, txn_buf());
    }

    for (uint64_t i = 0; i < FLAGS_ycsb_ops_per_hot_tx; ++i) {
      ermia::varstr &v = str((ermia::config::index_probe_only) ? 0 : sizeof(ycsb_kv::value));

      // TODO(tzwang): add read/write_all_fields knobs
      rc_t rc = rc_t{RC_INVALID};
      if (!ermia::config::index_probe_only) {
        auto &k = GenerateKey(txn, true);
        table_index->GetRecord(txn, rc, k, v);  // Read
        ycsb_kv::value v_y_temp;
        const ycsb_kv::value *v_y = Decode(v, v_y_temp);
      } else {
        auto &k = GenerateKey(txn, true);
        ermia::OID oid = 0;
        ermia::ConcurrentMasstree::versioned_node_t sinfo;
        rc = (table_index->GetMasstree().search(k, oid, &sinfo)) ? RC_TRUE : RC_FALSE;
      }

#if defined(SSN)
      TryCatch(rc);
#else
      ALWAYS_ASSERT(rc._val == RC_TRUE);  // Under SI this must succeed
#endif
    }

    if (!ermia::config::index_probe_only) {
      TryCatch(db->Commit(txn));
    }

    return {RC_TRUE};
  }

  // Read-modify-write transaction. Sequential execution only
  rc_t txn_rmw() {
    ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
    for (uint64_t i = 0; i < FLAGS_ycsb_ops_per_tx; ++i) {
      ermia::varstr &k = GenerateKey(txn);
      ermia::varstr &v = str(sizeof(ycsb_kv::value));

      // TODO(tzwang): add read/write_all_fields knobs
      rc_t rc = rc_t{RC_INVALID};
      table_index->GetRecord(txn, rc, k, v);  // Read

#if defined(SSN)
      TryCatch(rc);  // Might abort if we use SSN
#else
      ALWAYS_ASSERT(rc._val == RC_TRUE);  // Under SI this must succeed
#endif

      ycsb_kv::value v_y_temp;
      const ycsb_kv::value *v_y = Decode(v, v_y_temp);

      // Update a field (the first non-key field)
      ycsb_kv::value v_y_new(*v_y);
      *(uint64_t *)v_y_new.y_value.data() = *(uint64_t *)v_y_temp.y_value.data() + 1;
      if (FLAGS_ycsb_log_update_delta) {
        // The updated field is the first non-key field, which is at offset 8, size 8B
        TryCatch(table_index->UpdateRecord(txn, k, Encode(v, v_y_new), 8, (char *)v_y_temp.y_value.data(), 8));
      } else {
        TryCatch(table_index->UpdateRecord(txn, k, Encode(v, v_y_new)));
      }
    }

    for (uint64_t i = 0; i < FLAGS_ycsb_rmw_additional_reads; ++i) {
      bool hot;
      if (i < FLAGS_ycsb_cold_ops_per_tx) {
        hot = false;
      } else {
        hot = true;
      }
      ermia::varstr &k = GenerateKey(txn, hot);
      ermia::varstr &v = str(sizeof(ycsb_kv::value));

      // TODO(tzwang): add read/write_all_fields knobs
      rc_t rc = rc_t{RC_INVALID};
      table_index->GetRecord(txn, rc, k, v);  // Read

#if defined(SSN)
      TryCatch(rc);  // Might abort if we use SSN
#else
      ALWAYS_ASSERT(rc._val == RC_TRUE);  // Under SI this must succeed
#endif

      ycsb_kv::value v_y_temp;
      const ycsb_kv::value *v_y = Decode(v, v_y_temp);
    }

    TryCatch(db->Commit(txn));
    return {RC_TRUE};
  }

  rc_t txn_hot_rmw() {
    ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
    for (uint64_t i = 0; i < FLAGS_ycsb_ops_per_tx; ++i) {
      ermia::varstr &k = GenerateKey(txn);
      ermia::varstr &v = str(sizeof(ycsb_kv::value));

      // TODO(tzwang): add read/write_all_fields knobs
      rc_t rc = rc_t{RC_INVALID};
      table_index->GetRecord(txn, rc, k, v);  // Read

#if defined(SSN)
      TryCatch(rc);  // Might abort if we use SSN
#else
      ALWAYS_ASSERT(rc._val == RC_TRUE);  // Under SI this must succeed
#endif

      ycsb_kv::value v_y_temp;
      const ycsb_kv::value *v_y = Decode(v, v_y_temp);
      ycsb_kv::value v_y_new(*v_y);

      // Re-initialize the value structure to use my own allocated memory -
      // DoTupleRead will change v.p to the object's data area to avoid memory
      // copy (in the read op we just did).

      *(uint64_t *)v_y_new.y_value.data() = *(uint64_t *)v_y_temp.y_value.data() + 1;
      if (FLAGS_ycsb_log_update_delta) {
        // The updated field is the first non-key field, which is at offset 8, size 8B
        TryCatch(table_index->UpdateRecord(txn, k, Encode(v, v_y_new), 8, (char *)v_y_temp.y_value.data(), 8));
      } else {
        TryCatch(table_index->UpdateRecord(txn, k, Encode(v, v_y_new)));
      }
    }

    for (uint64_t i = 0; i < FLAGS_ycsb_rmw_additional_reads; ++i) {
      ermia::varstr &k = GenerateKey(txn);
      ermia::varstr &v = str(sizeof(ycsb_kv::value));

      // TODO(tzwang): add read/write_all_fields knobs
      rc_t rc = rc_t{RC_INVALID};
      table_index->GetRecord(txn, rc, k, v);  // Read

#if defined(SSN)
      TryCatch(rc);  // Might abort if we use SSN
#else
      ALWAYS_ASSERT(rc._val == RC_TRUE);  // Under SI this must succeed
#endif

      ycsb_kv::value v_y_temp;
      const ycsb_kv::value *v_y = Decode(v, v_y_temp);
    }

    TryCatch(db->Commit(txn));
    return {RC_TRUE};
  }

  rc_t txn_hot_update() {
    ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
    for (uint64_t i = 0; i < FLAGS_ycsb_update_per_tx; ++i) {
      ermia::varstr &k = GenerateKey(txn);
      ermia::varstr &v = str(sizeof(ycsb_kv::value));
      // Blind update - if we don't want to read the record first, then we need
      // to update the entire tuple
      BuildValue(*(uint64_t *)k.data(), v);
      TryCatch(table_index->UpdateRecord(txn, k, v));
    }

    TryCatch(db->Commit(txn));
    return {RC_TRUE};
  }

  rc_t txn_cold_update() {
    ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
    for (uint64_t i = 0; i < FLAGS_ycsb_update_per_tx; ++i) {
      if (i < FLAGS_ycsb_cold_ops_per_tx) {
        ermia::varstr &k = GenerateKey(txn, false);
        ermia::varstr &v = str(sizeof(ycsb_kv::value));

        // TODO(tzwang): add read/write_all_fields knobs
        rc_t rc = rc_t{RC_INVALID};
        table_index->GetRecord(txn, rc, k, v);  // Read

#if defined(SSN)
        TryCatch(rc);  // Might abort if we use SSN
#else
        ALWAYS_ASSERT(rc._val == RC_TRUE);  // Under SI this must succeed
#endif

        ycsb_kv::value v_y_temp;
        const ycsb_kv::value *v_y = Decode(v, v_y_temp);

        ycsb_kv::value v_y_new;
        *(uint64_t *)v_y_new.y_value.data() = *(uint64_t *)v_y_temp.y_value.data() + 1;

        k = GenerateKey(txn);
        if (FLAGS_ycsb_log_update_delta) {
          // The updated field is the first non-key field, which is at offset 8, size 8B
          TryCatch(table_index->UpdateRecord(txn, k, Encode(v, v_y_new), 8, (char *)v_y_temp.y_value.data(), 8));
        } else {
          TryCatch(table_index->UpdateRecord(txn, k, Encode(v, v_y_new)));
        }

      } else {
        ermia::varstr &k = GenerateKey(txn);
        ermia::varstr &v = str(sizeof(ycsb_kv::value));
        BuildValue(*(uint64_t *)k.data(), v);
        TryCatch(table_index->UpdateRecord(txn, k, v));
      }
    }

    TryCatch(db->Commit(txn));
    return {RC_TRUE};
  }

  rc_t txn_scan() {
    ermia::transaction *txn = db->NewTransaction(ermia::transaction::TXN_FLAG_READ_ONLY, *arena, txn_buf());

    for (uint64_t j = 0; j < FLAGS_ycsb_ops_per_tx; ++j) {
      rc_t rc = rc_t{RC_INVALID};
      ScanRange range = GenerateScanRange(txn);
      ycsb_scan_callback callback;
      rc = table_index->Scan(txn, range.start_key, &range.end_key, callback);

      ALWAYS_ASSERT(callback.size() <= FLAGS_ycsb_max_scan_size);
#if defined(SSN)
      TryCatch(rc);
#else
      ALWAYS_ASSERT(rc._val == RC_TRUE);
#endif
    }

    TryCatch(db->Commit(txn));
    return {RC_TRUE};
  }

  rc_t txn_insert() {
    ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());

    for (uint64_t i = 0; i < FLAGS_ycsb_ins_per_tx; ++i) {
      auto &k = GenerateNewKey(txn);
      ermia::varstr &v = str(sizeof(ycsb_kv::value));
      BuildValue(*(uint64_t *)k.data(), v);

      rc_t rc = rc_t{RC_INVALID};
      rc = table_index->InsertRecord(txn, k, v);
      TryCatch(rc);
    }

    TryCatch(db->Commit(txn));
    return {RC_TRUE};
  }

 private:
  std::vector<ermia::varstr *> keys;
  std::vector<ermia::varstr *> values;
};

void ycsb_do_test(ermia::Engine *db) {
  ycsb_parse_options();
  ycsb_bench_runner<ycsb_sequential_worker> r(db);
  r.run();
}

int main(int argc, char **argv) {
  bench_main(argc, argv, ycsb_do_test);
  return 0;
}

