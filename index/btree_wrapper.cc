#include "btree_wrapper.h"
#include "btree/btree_olc.h"
#include "../engine.h"
#include "../txn.h"

// Wrapper implementing the OrderedIndex interface for an optimistic locking based B+-tree 

namespace ermia {

template<uint32_t KeyLength>
bool BTreeOLCIndex<KeyLength>::InsertOID(transaction *t, const varstr &key, OID oid) {
  FixedLengthKey<KeyLength> *k = (FixedLengthKey<KeyLength> *)key.p;
  bool inserted = btree.insert(*k, oid);

  if (inserted) {
    t->LogIndexInsert(this, oid, &key);
    if (config::enable_chkpt) {
      auto *key_array = GetTableDescriptor()->GetKeyArray();
      volatile_write(key_array->get(oid)->_ptr, 0);
    }
  }
  return inserted;
}

template<uint32_t KeyLength>
rc_t BTreeOLCIndex<KeyLength>::Scan(transaction *t, const varstr &start_key,
                                    const varstr *end_key, ScanCallback &callback) {
  rc_t rc = {RC_INVALID};
  auto scan_get_version_cb = [&](FixedLengthKey<KeyLength> key, OID oid) -> bool {
    dbtuple *tuple = oidmgr->oid_get_version(table_descriptor->GetTupleArray(), oid, t->GetXIDContext());
    varstr vv;
    volatile_write(rc._val, tuple ? t->DoTupleRead(tuple, &vv)._val : RC_FALSE);
    if (rc._val == RC_TRUE) {
      return callback.Invoke((const char *)&key, KeyLength, vv);
    } else if (rc.IsAbort()) {
      return false;
    }
    return true;
  };

  FixedLengthKey<KeyLength> *k = (FixedLengthKey<KeyLength> *)start_key.p;
  FixedLengthKey<KeyLength> *end_k = (FixedLengthKey<KeyLength> *)end_key->p;
  btree.scan_cb(*k, *end_k, scan_get_version_cb);

  return rc;
}

// Explicit instantiations
template class ermia::BTreeOLCIndex<8>;

} // namespace ermia
