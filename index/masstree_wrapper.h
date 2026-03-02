#pragma once

#include "index_table.h"
#include "masstree/masstree_btree.h"

struct transaction;

namespace ermia {

// User-facing concurrent Masstree
struct ConcurrentMasstreeIndex : public OrderedIndex {
  ConcurrentMasstree masstree_;

  struct SearchRangeCallback {
    SearchRangeCallback(OrderedIndex::ScanCallback &upcall)
        : upcall(&upcall), return_code(rc_t{RC_FALSE}) {}
    ~SearchRangeCallback() {}

    inline bool Invoke(const ConcurrentMasstree::string_type &k,
                       const varstr &v) {
      return upcall->Invoke(k.data(), k.length(), v);
    }

    OrderedIndex::ScanCallback *upcall;
    rc_t return_code;
  };

  struct XctSearchRangeCallback
      : public ConcurrentMasstree::low_level_search_range_callback {
    XctSearchRangeCallback(transaction *t, SearchRangeCallback *caller_callback)
        : t(t), caller_callback(caller_callback) {}

    virtual void
    on_resp_node(const typename ConcurrentMasstree::node_opaque_t *n,
                 uint64_t version);
    virtual bool invoke(const ConcurrentMasstree *btr_ptr,
                        const typename ConcurrentMasstree::string_type &k,
                        dbtuple *v,
                        const typename ConcurrentMasstree::node_opaque_t *n,
                        uint64_t version);

    transaction *const t;
    SearchRangeCallback *const caller_callback;
  };

  struct PurgeTreeWalker : public ConcurrentMasstree::tree_walk_callback {
    virtual void
    on_node_begin(const typename ConcurrentMasstree::node_opaque_t *n);
    virtual void on_node_success();
    virtual void on_node_failure();

    std::vector<std::pair<typename ConcurrentMasstree::value_type, bool>>
        spec_values;
  };

  static rc_t DoNodeRead(transaction *t,
                         const ConcurrentMasstree::node_opaque_t *node,
                         uint64_t version);

  ConcurrentMasstreeIndex(const char *table_name, bool primary)
    : OrderedIndex(table_name, primary) {}
  ~ConcurrentMasstreeIndex() {}

  ConcurrentMasstree &GetMasstree() { return masstree_; }

  bool InsertOID(transaction *t, const varstr &key, OID oid) override;

  rc_t Scan(transaction *t, const varstr &start_key, const varstr *end_key,
                     ScanCallback &callback) override;
  rc_t ReverseScan(transaction *t, const varstr &start_key,
                            const varstr *end_key, ScanCallback &callback) override;

  inline size_t Size() override { return masstree_.size(); }
  std::map<std::string, uint64_t> Clear();

  inline void GetOID(const varstr &key, rc_t &rc, TXN::xid_context *xc, OID &out_oid) override {
    bool found = masstree_.search(key, out_oid, nullptr);
    volatile_write(rc._val, found ? RC_TRUE : RC_FALSE);
  }

  bool InsertIfAbsent(transaction *t, const varstr &key, OID oid);
};
} // namespace ermia
