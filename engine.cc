#include "engine.h"
#include "index/btree_wrapper.h"
#include "index/hash_wrapper.h"
#include "index/masstree_wrapper.h"

namespace ermia {

std::atomic<uint32_t> log_counter{0};

dlog::tls_log *GetLog(uint32_t logid) {
  // XXX(tzwang): this lock may become a problem; should be safe to not use it -
  // the set of tlogs are stable before the system starts to run, i.e., only
  // needed when creating logs
  // std::lock_guard<std::mutex> guard(tlog_lock);
  return dlog::tlogs[logid];
}

dlog::tls_log *GetLog() {
  thread_local dlog::tls_log *tlog_ptr = nullptr;
  if (!tlog_ptr) {
    uint32_t my_id = log_counter.fetch_add(1);
    tlog_ptr = dlog::tlogs[my_id / ermia::config::n_combine_log];

    if (ermia::config::flusher_thread) {
      uint32_t cpu = sched_getcpu();
      for (uint32_t i = 0; i < ermia::config::s3_bucket_names.size(); ++i) {
        tlog_ptr->write_thread_task[i].set_affinity(cpu);
      }
    }
  }
  return tlog_ptr;
}

// Engine initialization, including creating the OID, log, and checkpoint
// managers and recovery if needed.
Engine::Engine() {
  config::sanity_check();
  ALWAYS_ASSERT(config::log_dir.size());
  ALWAYS_ASSERT(!oidmgr);
  sm_oid_mgr::create();
  ALWAYS_ASSERT(oidmgr);
  ermia::dlog::initialize();
}

Engine::~Engine() { ermia::dlog::uninitialize(); }

TableDescriptor *Engine::CreateTable(const char *name) {
  auto *td = TableDescriptor::New(name);

  if (true) { //! sm_log::need_recovery) {
    // Note: this will insert to the log and therefore affect min_flush_lsn,
    // so must be done in an sm-thread which must be created by the user
    // application (not here in ERMIA library).
    // ASSERT(ermia::logmgr);

    // TODO(tzwang): perhaps make this transactional to allocate it from
    // transaction string arena to avoid malloc-ing memory (~10k size).
    // char *log_space = (char *)malloc(sizeof(sm_tx_log));
    // ermia::sm_tx_log *log = ermia::logmgr->new_tx_log(log_space);
    td->Initialize();
    // log->log_table(td->GetTupleFid(), td->GetKeyFid(), td->GetName());
    // log->commit(nullptr);
    // free(log_space);
  }
  return td;
}

void Engine::LogIndexCreation(bool primary, FID table_fid, FID index_fid,
                              const std::string &index_name) {
  /*
  if (!sm_log::need_recovery) {
    // Note: this will insert to the log and therefore affect min_flush_lsn,
    // so must be done in an sm-thread which must be created by the user
    // application (not here in ERMIA library).
    ASSERT(ermia::logmgr);

    // TODO(tzwang): perhaps make this transactional to allocate it from
    // transaction string arena to avoid malloc-ing memory (~10k size).
    char *log_space = (char *)malloc(sizeof(sm_tx_log));
    ermia::sm_tx_log *log = ermia::logmgr->new_tx_log(log_space);
    log->log_index(table_fid, index_fid, index_name, primary);
    log->commit(nullptr);
    free(log_space);
  }
  */
}

template <uint32_t KeyLength>
void Engine::CreateIndex(const uint16_t type, const char *table_name,
                         const std::string &index_name, bool is_primary) {
  auto *td = TableDescriptor::Get(table_name);
  ALWAYS_ASSERT(td);
  UnorderedIndex *index = nullptr;

  switch (type) {
  case kIndexMasstree:
    index = new ConcurrentMasstreeIndex(table_name, is_primary);
    break;
  case kIndexBTreeOLC:
    index = new BTreeOLCIndex<KeyLength>(table_name, is_primary);
    break;
  case kIndexExHash:
    index = new ExHashIndex<KeyLength>(table_name, is_primary);
    break;
  default:
    LOG(FATAL) << "Unknown index type: " << type;
    break;
  }

  if (is_primary) {
    td->SetPrimaryIndex(index, index_name);
  } else {
    td->AddSecondaryIndex(index, index_name);
  }
  FID index_fid = index->GetIndexFid();
  LogIndexCreation(is_primary, td->GetTupleFid(), index_fid, index_name);
}

void UnorderedIndex::GetVersion(transaction *t, rc_t &rc, varstr &value,
                                OID oid) {
  rc = {RC_INVALID};

  dbtuple *tuple = oidmgr->oid_get_version(table_descriptor->GetTupleArray(),
                                           oid, t->GetXIDContext());
  volatile_write(rc._val,
                 tuple ? t->DoTupleRead(tuple, &value)._val : RC_FALSE);

#ifndef SSN
  ASSERT(rc._val == RC_FALSE || rc._val == RC_TRUE);
#endif
}

////////////////// Index-independent implementations /////////////////
void UnorderedIndex::GetRecord(transaction *t, rc_t &rc, const varstr &key,
                               varstr &value, OID *out_oid) {
  OID oid = INVALID_OID;
  GetOID(key, rc, t->xc, oid);
  if (t && rc._val == RC_TRUE) {
    GetVersion(t, rc, value, oid);
  }

  if (out_oid) {
    *out_oid = oid;
  }
}

rc_t UnorderedIndex::UpdateRecord(transaction *t, const varstr &key,
                                  varstr &value,
                                  uint32_t delta_offset, char *delta, uint32_t delta_size) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());

  // Search for OID
  OID oid = INVALID_OID;
  rc_t rc = {RC_INVALID};
  GetOID(key, rc, t->xc, oid);

  if (rc._val == RC_TRUE) {
    return t->Update(table_descriptor, oid, &value, delta_offset, delta, delta_size);
  } else {
    return rc_t{RC_ABORT_INTERNAL};
  }
}

rc_t UnorderedIndex::InsertRecord(transaction *t, const varstr &key,
                                  varstr &value, OID *out_oid) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());

  ASSERT((char *)key.data() == (char *)&key + sizeof(varstr));

  // Insert to the table first
  dbtuple *tuple = nullptr;
  OID oid = t->Insert(table_descriptor, false, &value, &tuple);

  // Done with table record, now set up index
  ASSERT((char *)key.data() == (char *)&key + sizeof(varstr));
  if (!InsertOID(t, key, oid)) {
    if (config::enable_chkpt) {
      volatile_write(table_descriptor->GetKeyArray()->get(oid)->_ptr, 0);
    }
    return rc_t{RC_ABORT_INTERNAL};
  }

  // Succeeded, now put the key there if we need it
  if (config::enable_chkpt) {
    // XXX(tzwang): only need to install this key if we need chkpt; not a
    // realistic setting here to not generate it, the purpose of skipping
    // this is solely for benchmarking CC.
    varstr *new_key = (varstr *)MM::allocate(sizeof(varstr) + key.size());
    new (new_key) varstr((char *)new_key + sizeof(varstr), 0);
    new_key->copy_from(&key);
    auto *key_array = table_descriptor->GetKeyArray();
    key_array->ensure_size(oid);
    oidmgr->oid_put(key_array, oid,
                    fat_ptr::make((void *)new_key, INVALID_SIZE_CODE));
  }

  if (out_oid) {
    *out_oid = oid;
  }

  return rc_t{RC_TRUE};
}

rc_t UnorderedIndex::InsertColdRecord(transaction *t, const varstr &key,
                                      varstr &value, OID *out_oid) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());

  ASSERT((char *)key.data() == (char *)&key + sizeof(varstr));

  // Insert to the table first
  dbtuple *tuple = nullptr;
  OID oid = t->Insert(table_descriptor, true, &value, &tuple);

  // Done with table record, now set up index
  ASSERT((char *)key.data() == (char *)&key + sizeof(varstr));
  if (!InsertOID(t, key, oid)) {
    if (config::enable_chkpt) {
      volatile_write(table_descriptor->GetKeyArray()->get(oid)->_ptr, 0);
    }
    return rc_t{RC_ABORT_INTERNAL};
  }

  // Succeeded, now put the key there if we need it
  if (config::enable_chkpt) {
    // XXX(tzwang): only need to install this key if we need chkpt; not a
    // realistic setting here to not generate it, the purpose of skipping
    // this is solely for benchmarking CC.
    varstr *new_key = (varstr *)MM::allocate(sizeof(varstr) + key.size());
    new (new_key) varstr((char *)new_key + sizeof(varstr), 0);
    new_key->copy_from(&key);
    auto *key_array = table_descriptor->GetKeyArray();
    key_array->ensure_size(oid);
    oidmgr->oid_put(key_array, oid,
                    fat_ptr::make((void *)new_key, INVALID_SIZE_CODE));
  }

  if (out_oid) {
    *out_oid = oid;
  }

  return rc_t{RC_TRUE};
}

rc_t UnorderedIndex::RemoveRecord(transaction *t, const varstr &key) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());

  // Search for OID
  OID oid = 0;
  rc_t rc = {RC_INVALID};
  GetOID(key, rc, t->xc, oid);

  if (rc._val == RC_TRUE) {
    // Allocate an empty record version as the "new" version
    varstr *null_val = t->string_allocator().next(0);
    return t->Update(table_descriptor, oid, null_val);
  } else {
    return rc_t{RC_ABORT_INTERNAL};
  }
}

////////////////// Table interfaces /////////////////
rc_t Table::Insert(transaction &t, varstr *value, OID *out_oid) {
  OID oid = t.Insert(td, false, value);
  if (out_oid) {
    *out_oid = oid;
  }
  return oid == INVALID_OID ? rc_t{RC_FALSE} : rc_t{RC_FALSE};
}

rc_t Table::Read(transaction &t, OID oid, varstr *out_value) {
  auto *tuple =
      oidmgr->oid_get_version(td->GetTupleArray(), oid, t.GetXIDContext());
  rc_t rc = {RC_INVALID};
  if (tuple) {
    // Record exists
    volatile_write(rc._val, t.DoTupleRead(tuple, out_value)._val);
  } else {
    volatile_write(rc._val, RC_FALSE);
  }
  ASSERT(rc._val == RC_FALSE || rc._val == RC_TRUE);
  return rc;
}

rc_t Table::Update(transaction &t, OID oid, varstr &value,
                   uint32_t delta_offset, char *delta, uint32_t delta_size) {
  return t.Update(td, oid, &value, delta_offset, delta, delta_size);
}

rc_t Table::Remove(transaction &t, OID oid) {
  return t.Update(td, oid, nullptr);
}

////////////////// End of Table interfaces //////////

UnorderedIndex::UnorderedIndex(std::string table_name, bool is_primary)
    : is_primary(is_primary) {
  table_descriptor = TableDescriptor::Get(table_name);
  self_fid = oidmgr->create_file(true);
}

// Explicit instantiations
template void ermia::Engine::CreateIndex<8>(const uint16_t, const char *,
                                            const std::string &, bool);

} // namespace ermia
