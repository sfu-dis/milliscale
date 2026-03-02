#pragma once

#include "../txn.h"
#include "dash/ex_finger.h"
#include "fixed_length_key.h"
#include "index_table.h"

namespace ermia {

template <uint32_t KeyLength>
struct ExHashIndex : public UnorderedIndex {
  dash_eh::Finger_EH<FixedLengthKey<KeyLength>> hash_table;

  ExHashIndex(const char *table_name, bool primary)
    : UnorderedIndex(table_name, primary), hash_table(64) {}
  ~ExHashIndex() {}

  bool InsertOID(transaction *t, const varstr &key, OID oid) override;

  inline void GetOID(const varstr &key, rc_t &rc, TXN::xid_context *xc, OID &out_oid) override {
    FixedLengthKey<KeyLength> *k = (FixedLengthKey<KeyLength> *)key.p;
    dash_eh::Value_t v;
    bool found = hash_table.Get(*k, &v);
    if (found) {
      out_oid = (OID)((uint64_t)v & 0xFFFFFFFF);
    }
    volatile_write(rc._val, found ? RC_TRUE : RC_FALSE);
  }

  inline size_t Size() override { return 0; // TODO
  }
};

} // namespace ermia
