#pragma once
#include "../dbcore/sm-table.h"

namespace ermia {

// Base class for user-facing index table implementations
struct UnorderedIndex {
  TableDescriptor *table_descriptor;
  bool is_primary;
  FID self_fid;

  UnorderedIndex(std::string table_name, bool is_primary);
  virtual ~UnorderedIndex() {}
  inline TableDescriptor *GetTableDescriptor() { return table_descriptor; }
  inline bool IsPrimary() { return is_primary; }
  inline FID GetIndexFid() { return self_fid; }

  // Get a record with a key of length keylen. The underlying DB does not manage
  // the memory associated with key. [rc] stores TRUE if found, FALSE otherwise.
  void GetRecord(transaction *t, rc_t &rc, const varstr &key, varstr &value,
                 OID *out_oid = nullptr);

  // Update a database record with a key of length keylen, with mapping of length
  // valuelen.  The underlying DB does not manage the memory pointed to by key or
  // value (a copy is made).
  //
  // If the does not already exist and config::upsert is set to true, insert.
  rc_t UpdateRecord(transaction *t, const varstr &key, varstr &value,
                    uint32_t delta_offset = 0, char *delta = nullptr, uint32_t delta_size = 0);

  // Insert a hot record with a key of length keylen.
  rc_t InsertRecord(transaction *t, const varstr &key, varstr &value, OID *out_oid = nullptr);

  // Insert a cold record with a key of length keylen.
  rc_t InsertColdRecord(transaction *t, const varstr &key, varstr &value, OID *out_oid = nullptr);

  // Default implementation calls put() with NULL (zero-length) value
  rc_t RemoveRecord(transaction *t, const varstr &key);

  // Map a key to an existing OID. Could be used for primary or secondary index.
  virtual bool InsertOID(transaction *t, const varstr &key, OID oid) = 0;

  virtual size_t Size() = 0;

  virtual void GetOID(const varstr &key, rc_t &rc, TXN::xid_context *xc, OID &out_oid) = 0;

  // Traverse the version chain to obtain the target record version
  void GetVersion(transaction *t, rc_t &rc, varstr &value, OID oid);
};

// Base class for user-facing ordered index implementations
struct OrderedIndex : public UnorderedIndex {
  OrderedIndex(std::string table_name, bool is_primary)
    : UnorderedIndex(table_name, is_primary) {}
  virtual ~OrderedIndex() {}

  struct ScanCallback {
    virtual ~ScanCallback() {}
    virtual bool Invoke(const char *keyp, size_t keylen, const varstr &value) = 0;
  };

  // Search [start_key, *end_key) if end_key is not null, otherwise
  // search [start_key, +infty)
  virtual rc_t Scan(transaction *t, const varstr &start_key,
                    const varstr *end_key, ScanCallback &callback) = 0;
  // Search (*end_key, start_key] if end_key is not null, otherwise
  // search (-infty, start_key] (starting at start_key and traversing
  // backwards)
  virtual rc_t ReverseScan(transaction *t, const varstr &start_key,
                           const varstr *end_key, ScanCallback &callback) = 0;
};

}  // namespace ermia
