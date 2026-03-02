#include "txn.h"
#include "dbcore/serial.h"
#include "engine.h"
#include "macros.h"

namespace ermia {

transaction::transaction(uint64_t flags, str_arena &sa)
    : flags(flags), log(nullptr), log_size(0), sa(&sa), is_local_log(true), prev_log_id(kInvalidLogID), max_dependent_csn(0) {
  if (config::phantom_prot) {
    masstree_absent_set.set_empty_key(NULL); // google dense map
    masstree_absent_set.clear();
  }
  write_set.clear();
#if defined(SSN)
  read_set.clear();
#endif
  xid = TXN::xid_alloc();
  xc = TXN::xid_get_context(xid);
  xc->xct = this;

#if defined(SSN)
  // If there's a safesnap, then SSN treats the snapshot as a transaction
  // that has read all the versions, which means every update transaction
  // should have a initial pstamp of the safesnap.

  // Take a safe snapshot if read-only.
  if (config::enable_safesnap && (flags & TXN_FLAG_READ_ONLY)) {
    ASSERT(MM::safesnap_lsn);
    xc->begin = volatile_read(MM::safesnap_lsn);
  } else {
    TXN::serial_register_tx(xid);
    log = logmgr->new_tx_log(
        (char *)string_allocator().next(sizeof(sm_tx_log))->data());
    // Must +1: a tx T can only update a tuple if its latest version was
    // created before T's begin timestamp (i.e., version.clsn < T.begin,
    // note the range is exclusive; see first updater wins rule in
    // oid_put_update() in sm-oid.cpp). Otherwise we risk making no
    // progress when retrying an aborted transaction: everyone is trying
    // to update the same tuple with latest version stamped at cur_lsn()
    // but no one can succeed (because version.clsn == cur_lsn == t.begin).
    xc->begin = logmgr->cur_lsn().offset() + 1;
    xc->pstamp = volatile_read(MM::safesnap_lsn);
  }
#else
  // Give a log regardless - with pipelined commit, read-only tx needs
  // to go through the queue as well
  log = GetLog();
  xc->begin = dlog::current_csn.load(std::memory_order_relaxed);
  dlog::thread_begin_csns[log->id] = xc->begin;
#endif
  is_disk = false;
  is_in_memory_queue = true;
  m_abort_if_cold = false;
  m_is_forced_abort = false;
  pos_in_queue = ~uint16_t{0};
  cold_log_io_size = 0;
  io_uring_user_data[0] = -1;
}

void transaction::uninitialize() {
  // transaction shouldn't fall out of scope w/o resolution
  // resolution means TXN_CMMTD, and TXN_ABRTD
  ASSERT(state() != TXN::TXN_ACTIVE && state() != TXN::TXN_COMMITTING);
#if defined(SSN)
  if (!config::enable_safesnap || (!(flags & TXN_FLAG_READ_ONLY))) {
    TXN::serial_deregister_tx(xid);
  }
#endif
  TXN::xid_free(xid);
}

void transaction::Abort() {
  // Mark the dirty tuple as invalid, for oid_get_version to
  // move on more quickly.
  volatile_write(xc->state, TXN::TXN_ABRTD);

#if defined(SSN)
  // Go over the read set first, to deregister from the tuple
  // asap so the updater won't wait for too long.
  for (uint32_t i = 0; i < read_set.size(); ++i) {
    auto &r = read_set[i];
    ASSERT(r->GetObject()->GetClsn().asi_type() == fat_ptr::ASI_LOG);
    // remove myself from reader list
    serial_deregister_reader_tx(&r->readers_bitmap);
  }
#endif

  for (uint32_t i = 0; i < write_set.size(); ++i) {
    auto &w = write_set[i];
    dbtuple *tuple = (dbtuple *)w.get_object()->GetPayload();
    ASSERT(tuple);
#if defined(SSN)
    ASSERT(XID::from_ptr(tuple->GetObject()->GetClsn()) == xid);
    if (tuple->NextVolatile()) {
      volatile_write(tuple->NextVolatile()->sstamp, NULL_PTR);
      tuple->NextVolatile()->welcome_read_mostly_tx();
    }
#endif

    Object *obj = w.get_object();
    fat_ptr entry = *w.entry;
    obj->SetCSN(NULL_PTR);
    oidmgr->UnlinkTuple(w.entry);
    MM::deallocate(entry);
  }
}

rc_t transaction::commit() {
  ALWAYS_ASSERT(state() == TXN::TXN_ACTIVE);
  volatile_write(xc->state, TXN::TXN_COMMITTING);
  rc_t ret;
#if defined(SSN)
  // Safe snapshot optimization for read-only transactions:
  // Use the begin ts as cstamp if it's a read-only transaction
  if (config::enable_safesnap && (flags & TXN_FLAG_READ_ONLY)) {
    ASSERT(!log);
    ASSERT(write_set.size() == 0);
    xc->end = xc->begin;
    volatile_write(xc->state, TXN::TXN_CMMTD);
    ret = {RC_TRUE};
  } else {
    ASSERT(log);
    xc->end = log->pre_commit().offset();
    if (xc->end == 0) {
      ret = rc_t{RC_ABORT_INTERNAL};
    }
    ret = parallel_ssn_commit();
  }
#else
  ret = si_commit();
#endif

  // Enqueue to pipelined commit queue, if enabled
  if (ret._val == RC_TRUE) {
    uninitialize();
  }

  return ret;
}

#if !defined(SSN)
rc_t transaction::si_commit() {
  if (!log && ((flags & TXN_FLAG_READ_ONLY) || write_set.size() == 0)) {
    volatile_write(xc->state, TXN::TXN_CMMTD);
    return rc_t{RC_TRUE};
  }

  if (config::phantom_prot && !MasstreeCheckPhantom()) {
    return rc_t{RC_ABORT_PHANTOM};
  }

  ASSERT(log);

  dlog::log_block *lb = nullptr;
  dlog::tlog_lsn lb_lsn = dlog::INVALID_TLOG_LSN;
  uint64_t segnum = -1;
  
  if (ermia::config::dependency_aware && this->is_local_log && this->prev_log_id != kInvalidLogID) {
    log = GetLog(this->prev_log_id); // dependency aware log, put transaction to the same log as it's dependency
  }
  if (ermia::config::optimize_dequeue == 2) {
    this->max_dependent_csn = xc->begin-1;
  }
  // When allocate log block, will create csn, enqueue csn and set first/last csn
  // need to call it no matter log_size is 0
  lb = log->allocate_log_block(log_size, &lb_lsn, &segnum, this);

  // For read only transaction, it is possible that the transaction can be committed
  if (xc->end < ermia::pcommit::global_upto_csn) {
    log->tcommitter.dequeue_committed_xcts(true, true);
  }
  // Normally, we'd generate each version's persitent address along the way or
  // here first before toggling the CSN's "committed" bit. But we can actually
  // do it first, and generate the log block as we scan the write set once,
  // leveraging pipelined commit!

  // Post-commit: install CSN to tuples (traverse write-tuple), generate log
  // records, etc.
  for (uint32_t i = 0; i < write_set.size(); ++i) {
    auto &w = write_set[i];
    Object *object = w.get_object();
    dbtuple *tuple = (dbtuple *)object->GetPayload();

    // Populate log block and obtain persistent address
    uint32_t off = lb->payload_size;
    if (w.is_insert) {
      auto ret_off = dlog::log_insert(lb, w.fid, w.oid, (char *)tuple, w.size);
      ALWAYS_ASSERT(ret_off == off);
    } else {
      // Use the delta if delta is provided, otherwise use the full record
      auto ret_off = dlog::log_update(lb, w.fid, w.oid, w.delta ? w.delta : (char *)tuple, w.size, w.delta);
      ALWAYS_ASSERT(ret_off == off);
    }
    ALWAYS_ASSERT(lb->payload_size <= lb->capacity);

    // This aligned_size should match what was calculated during
    // add_to_write_set, and the size_code calculated based on this aligned size
    // will be part of the persistent address, which a read can directly use to
    // load the log record from the log (i.e., knowing how many bytes to read to
    // obtain the log record header + dbtuple header + record data).
    auto aligned_size = align_up(w.size + sizeof(dlog::log_record));
    auto size_code = encode_size_aligned(aligned_size);

    // lb_lsn points to the start of the log block which has a header, followed
    // by individual log records, so the log record's direct address would be
    // lb_lsn + sizeof(log_block) + off
    fat_ptr pdest =
        LSN::make(log->get_id(), lb_lsn + sizeof(dlog::log_block) + off, segnum,
                  size_code)
            .to_ptr();

    if (w.is_cold) {
      // Overwrite the entry to directly carry LSN
      MM::deallocate(*w.entry);
      volatile_write(w.entry->_ptr, pdest._ptr);
    } else {
      object->SetPersistentAddress(pdest);
      ASSERT(object->GetPersistentAddress().asi_type() == fat_ptr::ASI_LOG);

      // Set CSN
      fat_ptr csn_ptr = object->GenerateCsnPtr(xc->end, xc->logid);
      object->SetCSN(csn_ptr);
      ASSERT(tuple->GetObject()->GetCSN().asi_type() == fat_ptr::ASI_CSN);
    }
  }
  if (write_set.size()) {
    log->holes --;
  }
  ALWAYS_ASSERT(!lb || lb->payload_size == lb->capacity);

  // NOTE: make sure this happens after populating log block,
  // otherwise readers will see inconsistent data!
  // This is when (committed) tuple data are made visible to readers
  volatile_write(xc->state, TXN::TXN_CMMTD);
  return rc_t{RC_TRUE};
}
#endif

// returns true if btree versions have changed, ie there's phantom
bool transaction::MasstreeCheckPhantom() {
  for (auto &r : masstree_absent_set) {
    const uint64_t v = ConcurrentMasstree::ExtractVersionNumber(r.first);
    if (unlikely(v != r.second))
      return false;
  }
  return true;
}

rc_t transaction::Update(TableDescriptor *td, OID oid, varstr *v,
                         uint32_t delta_offset, char *delta, uint32_t delta_size) {
  oid_array *tuple_array = td->GetTupleArray();
  FID tuple_fid = td->GetTupleFid();

  // first *updater* wins
  fat_ptr new_obj_ptr = NULL_PTR;
  fat_ptr prev_obj_ptr =
      oidmgr->UpdateTuple(tuple_array, oid, v, xc, &new_obj_ptr);
  Object *prev_obj = (Object *)prev_obj_ptr.offset();

  if (prev_obj) { // succeeded
    dbtuple *tuple = ((Object *)new_obj_ptr.offset())->GetPinnedTuple(this);
    ASSERT(tuple);
    dbtuple *prev = prev_obj->GetPinnedTuple(this);
    ASSERT((uint64_t)prev->GetObject() == prev_obj_ptr.offset());
    ASSERT(xc);
#ifdef SSN
    // update hi watermark
    // Overwriting a version could trigger outbound anti-dep,
    // i.e., I'll depend on some tx who has read the version that's
    // being overwritten by me. So I'll need to see the version's
    // access stamp to tell if the read happened.
    ASSERT(prev->sstamp == NULL_PTR);
    auto prev_xstamp = volatile_read(prev->xstamp);
    if (xc->pstamp < prev_xstamp)
      xc->pstamp = prev_xstamp;

#ifdef EARLY_SSN_CHECK
    if (not ssn_check_exclusion(xc)) {
      // unlink the version here (note abort_impl won't be able to catch
      // it because it's not yet in the write set)
      oidmgr->UnlinkTuple(tuple_array, oid);
      return rc_t{RC_ABORT_SERIAL};
    }
#endif

    // copy access stamp to new tuple from overwritten version
    // (no need to copy sucessor lsn (slsn))
    volatile_write(tuple->xstamp, prev->xstamp);
#endif

    // read prev's CSN first, in case it's a committing XID, the CSN's state
    // might change to ASI_CSN anytime
    ASSERT((uint64_t)prev->GetObject() == prev_obj_ptr.offset());
    fat_ptr prev_csn = prev->GetObject()->GetCSN();
    fat_ptr prev_persistent_ptr = NULL_PTR;
    if (prev_csn.asi_type() == fat_ptr::ASI_XID and
        XID::from_ptr(prev_csn) == xid) {
      // updating my own updates!
      // prev's prev: previous *committed* version
      prev_persistent_ptr = prev_obj->GetNextPersistent();
      // FIXME(tzwang): 20190210: seems the deallocation here is too early,
      // causing readers to not find any visible version. Fix this together with
      // GC later.
      // MM::deallocate(prev_obj_ptr);
    } else { // prev is committed (or precommitted but in post-commit now) head
#if defined(SSN)
      volatile_write(prev->sstamp, xc->owner.to_ptr());
      ASSERT(prev->sstamp.asi_type() == fat_ptr::ASI_XID);
      ASSERT(XID::from_ptr(prev->sstamp) == xc->owner);
      ASSERT(tuple->NextVolatile() == prev);
#endif
      /*
      if (delta) {
        LOG(INFO) << "OFF=" << delta_offset << " NEW=" << *(uint32_t *)delta << " SIZE=" << delta_size << " VS " << tuple->size;
      }
      */
      add_to_write_set(tuple_array->get(oid), tuple_fid, oid, 
                       delta ? delta_size : tuple->size,
                       false, false, delta_offset, delta);
      prev_persistent_ptr = prev_obj->GetPersistentAddress();
    }

    ASSERT(tuple->GetObject()->GetCSN().asi_type() == fat_ptr::ASI_XID);
    ASSERT(oidmgr->oid_get_version(tuple_fid, oid, xc) == tuple);
    ASSERT(log);

    // FIXME(tzwang): mark deleted in all 2nd indexes as well?
    return rc_t{RC_TRUE};
  } else { // somebody else acted faster than we did
    return rc_t{RC_ABORT_SI_CONFLICT};
  }
}

OID transaction::Insert(TableDescriptor *td, bool cold, varstr *value,
                        dbtuple **out_tuple) {
  auto *tuple_array = td->GetTupleArray();
  FID tuple_fid = td->GetTupleFid();

  fat_ptr new_head = Object::Create(value);
  ASSERT(new_head.size_code() != INVALID_SIZE_CODE);
  ASSERT(new_head.asi_type() == 0);
  auto *tuple = (dbtuple *)((Object *)new_head.offset())->GetPayload();
  ASSERT(decode_size_aligned(new_head.size_code()) >= tuple->size);
  tuple->GetObject()->SetCSN(xid.to_ptr());
  OID oid = oidmgr->alloc_oid(tuple_fid);
  ALWAYS_ASSERT(oid != INVALID_OID);
  oidmgr->oid_put_new(tuple_array, oid, new_head);

  ASSERT(tuple->size == value->size());
  add_to_write_set(tuple_array->get(oid), tuple_fid, oid, tuple->size, true,
                   cold);

  if (out_tuple) {
    *out_tuple = tuple;
  }
  return oid;
}

void transaction::LogIndexInsert(UnorderedIndex *index, OID oid,
                                 const varstr *key) {
  /*
  // Note: here we log the whole key varstr so that recovery can figure out the
  // real key length with key->size(), otherwise it'll have to use the decoded
  // (inaccurate) size (and so will build a different index).
  auto record_size = align_up(sizeof(varstr) + key->size());
  ASSERT((char *)key->data() == (char *)key + sizeof(varstr));
  auto size_code = encode_size_aligned(record_size);
  log->log_insert_index(index->GetIndexFid(), oid,
                        fat_ptr::make((void *)key, size_code),
                        DEFAULT_ALIGNMENT_BITS, NULL);
  */
}

rc_t transaction::DoTupleRead(dbtuple *tuple, varstr *out_v) {
  ASSERT(tuple);
  ASSERT(xc);
  bool read_my_own =
      (tuple->GetObject()->GetCSN().asi_type() == fat_ptr::ASI_XID);
  ASSERT(!read_my_own ||
         (read_my_own &&
          XID::from_ptr(tuple->GetObject()->GetCSN()) == xc->owner));
  ASSERT(!read_my_own || !(flags & TXN_FLAG_READ_ONLY));

#if defined(SSN)
  if (!read_my_own) {
    rc_t rc = {RC_INVALID};
    if (flags & TXN_FLAG_READ_ONLY) {
      if (config::enable_safesnap) {
        return rc_t{RC_TRUE};
      }
    } else {
      rc = ssn_read(tuple);
    }
    if (rc.IsAbort()) {
      return rc;
    }
  } // otherwise it's my own update/insert, just read it
#endif

  // do the actual tuple read
  out_v->p = tuple->get_value_start();
  out_v->l = tuple->size;
  return tuple->size > 0 ? rc_t{RC_TRUE} : rc_t{RC_FALSE};
}

} // namespace ermia
