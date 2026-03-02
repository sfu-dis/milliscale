#pragma once

#include "dlog.h"
#include "dlog-defs.h"
#include "sm-common.h"
#include "sm-config.h"
#include "../varstr.h"
#include "xid.h"

namespace ermia {

struct dbtuple;
struct sm_log_recover_mgr;

struct Object {
  static const uint32_t kStatusMemory = 1;
  static const uint32_t kStatusStorage = 2;
  static const uint32_t kStatusLoading = 3;
  static const uint32_t kStatusDeleted = 4;

  // Where exactly is the payload? must be the first field
  uint32_t status;

  // The object's permanent home in the log/chkpt
  fat_ptr pdest;

  // The permanent home of the older version that's overwritten by me
  fat_ptr next_pdest;

  // Volatile pointer to the next older version that's in memory.
  // There might be a gap between the versions represented by next_pdest
  // and next_volatile.
  fat_ptr next_volatile;

  // Commit timestamp of this version. Type is XID (CSN) before (after)
  // commit.
  fat_ptr csn;

  static fat_ptr Create(const varstr* tuple_value);
  static fat_ptr InPlaceCreate(Object *obj, const varstr* tuple_value);

  Object()
      : status(kStatusMemory),
        pdest(NULL_PTR),
        next_pdest(NULL_PTR),
        next_volatile(NULL_PTR) {}

  Object(fat_ptr pdest, fat_ptr next, bool in_memory)
      : status(in_memory ? kStatusMemory : kStatusStorage),
        pdest(pdest),
        next_pdest(next),
        next_volatile(NULL_PTR) {}

  inline bool IsDeleted() { return status == kStatusDeleted; }
  inline bool IsInMemory() { return status == kStatusMemory; }
  inline fat_ptr* GetPersistentAddressPtr() { return &pdest; }
  inline fat_ptr GetPersistentAddress() { return pdest; }
  inline void SetPersistentAddress(fat_ptr ptr) { pdest._ptr = ptr._ptr; }
  inline fat_ptr GetCSN() { return csn; }
  inline void SetCSN(fat_ptr csnptr) { volatile_write(csn._ptr, csnptr._ptr); }
  inline fat_ptr GetNextPersistent() { return volatile_read(next_pdest); }
  inline fat_ptr* GetNextPersistentPtr() { return &next_pdest; }
  inline fat_ptr GetNextVolatile() { return volatile_read(next_volatile); }
  inline fat_ptr* GetNextVolatilePtr() { return &next_volatile; }
  inline void SetNextPersistent(fat_ptr next) { volatile_write(next_pdest, next); }
  inline void SetNextVolatile(fat_ptr next) { volatile_write(next_volatile, next); }
  inline char* GetPayload() { return (char*)((char*)this + sizeof(Object)); }
  inline void SetStatus(uint32_t s) { volatile_write(status, s); }
  inline dbtuple *GetPinnedTuple(transaction *xct) {
    if (IsDeleted()) {
      return nullptr;
    }
    Pin(xct);
    return (dbtuple*)GetPayload();
  }
  inline dbtuple* SyncGetPinnedTuple(transaction *xct) {
    if (IsDeleted()) {
      return nullptr;
    }
    SyncPin(xct);
    return (dbtuple*)GetPayload();
  }

  static fat_ptr GenerateCsnPtr(uint64_t csn, uint8_t log_id);
  static dbtuple *LoadFromStorage(transaction *xct, fat_ptr pdest, Object *object);
  static dbtuple* SyncLoadFromStorage(transaction *xct, fat_ptr pdest, Object *object);
  void Pin(transaction *xct);  // Make sure the payload is in memory
  void SyncPin(transaction *xct);

  static inline void PrefetchHeader(Object *p) {
    uint32_t i = 0;
    do {
      ::prefetch((const char *)(p + i));
      i += CACHELINE_SIZE;
    } while (i < sizeof(Object));
  }
};
}  // namespace ermia
