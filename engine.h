#pragma once

#include "txn.h"

namespace ermia {

// Get "my" own log
dlog::tls_log *GetLog();

// Get a log with a specified log id
dlog::tls_log *GetLog(uint32_t logid);

struct Engine {
  void LogIndexCreation(bool primary, FID table_fid, FID index_fid, const std::string &index_name);
  template<uint32_t KeyLength = 8>
  void CreateIndex(const uint16_t type, const char *table_name, const std::string &index_name, bool is_primary);

  Engine();
  ~Engine();

  // All supported index types
  static const uint16_t kIndexMasstree = 0x1;
  static const uint16_t kIndexBTreeOLC = 0x2;
  static const uint16_t kIndexExHash = 0x3;

  // Create a table without any index (at least yet)
  TableDescriptor *CreateTable(const char *name);

  // Create the primary index for a table
  inline void CreateMasstreePrimaryIndex(const char *table_name, const std::string &index_name) {
    CreateIndex(kIndexMasstree, table_name, index_name, true);
  }

  // Create a secondary masstree index
  inline void CreateMasstreeSecondaryIndex(const char *table_name, const std::string &index_name) {
    CreateIndex(kIndexMasstree, table_name, index_name, false);
  }

  // Create the primary extendible hashing index for a table
  template<uint32_t KeyLength = 8>
  inline void CreateExHashPrimaryIndex(const char *table_name, const std::string &index_name) {
    CreateIndex<KeyLength>(kIndexExHash, table_name, index_name, true);
  }

  // Create a secondary extendible hashing index
  inline void CreateExHashSecondaryIndex(const char *table_name, const std::string &index_name) {
    CreateIndex(kIndexExHash, table_name, index_name, false);
  }

  // Create the primary BTreeOLC index for a table
  template<uint32_t KeyLength = 8>
  inline void CreateBTreeOLCPrimaryIndex(const char *table_name, const std::string &index_name) {
    CreateIndex<KeyLength>(kIndexBTreeOLC, table_name, index_name, true);
  }

  // Create a secondary BTreeOLC index
  inline void CreateBTreeOLCSecondaryIndex(const char *table_name, const std::string &index_name) {
    CreateIndex(kIndexBTreeOLC, table_name, index_name, false);
  }

  inline transaction *NewTransaction(uint64_t txn_flags, str_arena &arena, transaction *buf) {
    // Reset the arena here - can't rely on the benchmark/user code to do it
    arena.reset();
    new (buf) transaction(txn_flags, arena);
    return buf;
  }

  inline rc_t Commit(transaction *t) {
    rc_t rc = t->commit();
    return rc;
  }

  inline void Abort(transaction *t) {
    t->Abort();
    t->uninitialize();
  }
};

// User-facing table abstraction, operates on OIDs only
struct Table {
  TableDescriptor *td;
  Table() : td(nullptr) {}
  ~Table() {}

  rc_t Insert(transaction &t, varstr *value, OID *out_oid);
  rc_t Update(transaction &t, OID oid, varstr &value, uint32_t delta_offset, char *delta, uint32_t delta_size);
  rc_t Read(transaction &t, OID oid, varstr *out_value);
  rc_t Remove(transaction &t, OID oid);
};

} // namespace ermia
