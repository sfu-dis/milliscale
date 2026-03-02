#include "hash_wrapper.h"
#include "../engine.h"
#include "../txn.h"

// Wrapper implementing the OrderedIndex interface for an optimistic locking based B+-tree 

namespace ermia {

template<uint32_t KeyLength>
bool ExHashIndex<KeyLength>::InsertOID(transaction *t, const varstr &key, OID oid) {
  FixedLengthKey<KeyLength> *k = (FixedLengthKey<KeyLength> *)key.p;
  dash_eh::Value_t v = (dash_eh::Value_t)(uint64_t)oid;
  bool inserted = hash_table.Insert(*k, v) == 0;

  if (inserted) {
    t->LogIndexInsert(this, oid, &key);
    if (config::enable_chkpt) {
      auto *key_array = GetTableDescriptor()->GetKeyArray();
      volatile_write(key_array->get(oid)->_ptr, 0);
    }
  }
  return inserted;
}

// Explicit instantiations
template class ermia::ExHashIndex<8>;

} // namespace ermia
