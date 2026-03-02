#pragma once

#include "../txn.h"
#include "btree/btree_olc.h"
#include "fixed_length_key.h"
#include "index_table.h"

namespace ermia {

template <uint32_t KeyLength>
struct BTreeOLCIndex : public OrderedIndex {
  btreeolc::BTreeOLC<FixedLengthKey<KeyLength>, OID> btree;

  BTreeOLCIndex(const char *table_name, bool primary)
    : OrderedIndex(table_name, primary) {}
  ~BTreeOLCIndex() {}

  bool InsertOID(transaction *t, const varstr &key, OID oid) override;

  rc_t Scan(transaction *t, const varstr &start_key, const varstr *end_key, ScanCallback &callback) override;
  rc_t ReverseScan(transaction *t, const varstr &start_key, const varstr *end_key, ScanCallback &callback) override { return {RC_FALSE}; }

  inline void GetOID(const varstr &key, rc_t &rc, TXN::xid_context *xc, OID &out_oid) override {
    FixedLengthKey<KeyLength> *k = (FixedLengthKey<KeyLength> *)key.p;
    bool found = btree.lookup(*k, out_oid);
    volatile_write(rc._val, found ? RC_TRUE : RC_FALSE);
  }

  inline size_t Size() override { return 0; // TODO
  }
};

} // namespace ermia
