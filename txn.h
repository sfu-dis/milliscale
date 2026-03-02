#pragma once

#include <stdint.h>
#include <sys/types.h>

#include <vector>

#include "dbcore/dlog.h"
#include "dbcore/dlog-tx.h"
#include "dbcore/xid.h"
#include "dbcore/sm-config.h"
#include "dbcore/sm-oid.h"
#include "dbcore/sm-object.h"
#include "dbcore/sm-table.h"
#include "dbcore/sm-rc.h"
#include "index/masstree_wrapper.h"
#include "macros.h"
#include "str_arena.h"
#include "tuple.h"

#include <sparsehash/dense_hash_map>
using google::dense_hash_map;

namespace ermia {


#if defined(SSN)
#define set_tuple_xstamp(tuple, s)                                    \
  {                                                                   \
    uint64_t x;                                                       \
    do {                                                              \
      x = volatile_read(tuple->xstamp);                               \
    } while (x < s and                                                \
             not __sync_bool_compare_and_swap(&tuple->xstamp, x, s)); \
  }
#endif

// A write-set entry is essentially a pointer to the OID array entry
// begin updated. The write-set is naturally de-duplicated: repetitive
// updates will leave only one entry by the first update. Dereferencing
// the entry pointer results a fat_ptr to the new object.
struct write_record_t {
  fat_ptr *entry;
  FID fid;
  OID oid;
  uint64_t size;  // size of the record if delta isn't provided, otherwise size of the delta
  bool is_insert;
  bool is_cold;
  uint32_t delta_offset;
  char *delta;
  write_record_t(fat_ptr *entry, FID fid, OID oid, uint64_t size, bool insert, bool cold,
                 uint32_t delta_offset, char *delta)
    : entry(entry), fid(fid), oid(oid), size(size), is_insert(insert), is_cold(cold),
      delta_offset(delta_offset), delta(delta) {}
  write_record_t() : entry(nullptr), fid(0), oid(0), size(0), is_insert(false), is_cold(false),
                     delta_offset(0), delta(nullptr) {}
  inline Object *get_object() { return (Object *)entry->offset(); }
};

struct write_set_t {
  static const uint32_t kMaxEntries = 256;
  uint32_t num_entries;
  write_record_t entries[kMaxEntries];
  write_set_t() : num_entries(0) {}
  inline void emplace_back(fat_ptr *oe, FID fid, OID oid, uint32_t size, bool insert, bool cold, uint32_t delta_offset, char *delta) {
    ALWAYS_ASSERT(num_entries < kMaxEntries);
    new (&entries[num_entries]) write_record_t(oe, fid, oid, size, insert, cold, delta_offset, delta);
    ++num_entries;
    ASSERT(entries[num_entries - 1].entry == oe);
  }
  inline uint32_t size() { return num_entries; }
  inline void clear() { num_entries = 0; }
  inline write_record_t &operator[](uint32_t idx) { return entries[idx]; }
};

struct transaction {
  static const uint8_t kInvalidLogID = 255;

  typedef TXN::txn_state txn_state;

#if defined(SSN)
  typedef std::vector<dbtuple *> read_set_t;
#endif

  enum {
    // true to mark a read-only transaction- if a txn marked read-only
    // does a write, it is aborted. SSN uses it to implement to safesnap.
    // No bookeeping is done with SSN if this is enable for a tx.
    TXN_FLAG_READ_ONLY = 0x2,

    TXN_FLAG_READ_MOSTLY = 0x3,
  };

  inline bool is_read_mostly() { return flags & TXN_FLAG_READ_MOSTLY; }
  inline bool is_read_only() { return flags & TXN_FLAG_READ_ONLY; }

  inline txn_state state() const { return xc->state; }

  // the absent set is a mapping from (masstree node -> version_number).
  typedef dense_hash_map<const ConcurrentMasstree::node_opaque_t *, uint64_t > MasstreeAbsentSet;
  MasstreeAbsentSet masstree_absent_set;

  transaction(uint64_t flags, str_arena &sa);
  ~transaction() {}

  void uninitialize();

  rc_t commit();
#ifdef SSN
  rc_t parallel_ssn_commit();
  rc_t ssn_read(dbtuple *tuple);
#else
  rc_t si_commit();
#endif

  bool MasstreeCheckPhantom();
  void Abort();

  // Insert a record to the underlying table
  OID Insert(TableDescriptor *td, bool cold, varstr *value, dbtuple **out_tuple = nullptr);

  rc_t Update(TableDescriptor *td, OID oid, varstr *v,
              uint32_t delta_offset = -1, char *delta = nullptr, uint32_t delta_size = -1);

  void LogIndexInsert(UnorderedIndex *index, OID oid, const varstr *key);

public:
  // Reads the contents of tuple into v within this transaction context
  rc_t DoTupleRead(dbtuple *tuple, varstr *out_v);

  // expected public overrides

  inline str_arena &string_allocator() { return *sa; }

  inline void add_to_write_set(fat_ptr *entry, FID fid, OID oid, uint64_t size, bool insert, bool cold,
                               uint32_t delta_offset = 0, char *delta = nullptr) {
#ifndef NDEBUG
    for (uint32_t i = 0; i < write_set.size(); ++i) {
      auto &w = write_set[i];
      ASSERT(w.entry);
      ASSERT(w.entry != entry);
    }
#endif

    // Work out the encoded size to be added to the log block later
    // If this is a delta, then we need to stuff the delta_offset and size information
    // as the first two uint32_ts in the payload area of the log record, so + sizeof(uint32_t) * 2
    auto logrec_size = align_up(size + (delta ? sizeof(uint32_t) * 2 : sizeof(dbtuple)) + sizeof(dlog::log_record));
    log_size += logrec_size;
    // Each write set entry still just records the size of the actual "data" to
    // be inserted to the log excluding dlog::log_record, which will be
    // prepended by log_insert/update etc.
    write_set.emplace_back(entry, fid, oid, size + (delta ? 0 : sizeof(dbtuple)), insert, cold, delta_offset, delta);
  }

  inline TXN::xid_context *GetXIDContext() { return xc; }

  inline void set_cold(bool _is_disk) { is_disk = _is_disk; }
  inline bool is_cold() { return is_disk; }

  inline void set_abort_if_cold(bool _abort_if_cold) { m_abort_if_cold = _abort_if_cold; }
  inline bool abort_if_cold() { return m_abort_if_cold; }

  inline void set_forced_abort(bool _is_forced_abort) { m_is_forced_abort = _is_forced_abort; }
  inline bool is_forced_abort() { return m_is_forced_abort; }

  inline size_t get_expected_io_size() { return cold_log_io_size; }
  inline void set_expected_io_size(size_t _cold_log_io_size) {
    cold_log_io_size = _cold_log_io_size;
  }

  inline int *get_user_data() { return io_uring_user_data; }
  inline void set_user_data(int _user_data) {
    io_uring_user_data[0] = _user_data;
  }

  inline bool in_memory_queue() { return is_in_memory_queue; }
  inline void set_in_memory_queue(bool _is_in_memory_queue) {
    is_in_memory_queue = _is_in_memory_queue;
  }

  inline uint16_t index() { return pos_in_queue; }
  inline void set_index(uint16_t _pos_in_queue) {
    pos_in_queue = _pos_in_queue;
  }

  inline void set_position(bool _is_in_memory_queue, uint16_t _pos_in_queue) {
    is_in_memory_queue = _is_in_memory_queue;
    pos_in_queue = _pos_in_queue;
  }

  inline void set_max_dependent_csn(uint64_t csn) {
    max_dependent_csn = std::max(max_dependent_csn, csn);
  }

  const uint64_t flags;
  XID xid;
  TXN::xid_context *xc;
  dlog::tls_log *log;
  uint32_t log_size;
  str_arena *sa;
  write_set_t write_set;
  bool is_local_log;
  uint8_t prev_log_id;
  uint64_t max_dependent_csn;
#if defined(SSN)
  read_set_t read_set;
#endif
  bool is_disk;
  bool is_in_memory_queue;
  bool m_abort_if_cold;
  bool m_is_forced_abort;
  uint16_t pos_in_queue;
  size_t cold_log_io_size;
  int io_uring_user_data[1];
};

}  // namespace ermia
