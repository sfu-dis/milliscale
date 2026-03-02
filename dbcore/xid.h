#pragma once
#include <atomic>

#include "sm-common.h"

namespace ermia {

struct transaction;
namespace TXN {

enum txn_state {
  TXN_ACTIVE,
  TXN_COMMITTING,
  TXN_CMMTD,
  TXN_ABRTD,
  TXN_INVALID
};

struct xid_context {
  XID owner;
  uint64_t begin;
  uint64_t end;
  uint8_t logid;
#ifdef SSN
  uint64_t pstamp;               // youngest predecessor (\eta)
  std::atomic<uint64_t> sstamp;  // oldest successor (\pi)
  bool set_sstamp(uint64_t s);
#endif
  transaction *xct;
  txn_state state;

#ifdef SSN
  const static uint64_t sstamp_final_mark = 1UL << 63;
  inline void finalize_sstamp() {
    std::atomic_fetch_or(&sstamp, sstamp_final_mark);
  }
  inline void set_pstamp(uint64_t p) {
    volatile_write(pstamp, std::max(pstamp, p));
  }
#endif
  inline bool verify_owner(XID assumed) {
    return volatile_read(owner) == assumed;
  }

  xid_context() {}

  xid_context(XID o) {
    owner = o;
#ifdef SSN
    sstamp = 0;
    pstamp = 0;
    xct = NULL;
#endif
    // Note: transaction needs to initialize xc->begin in ctor
    end = 0;
    state = TXN_ACTIVE;
    xct = nullptr;
  }
};

static uint32_t const NCONTEXTS = 256;
extern xid_context contexts[NCONTEXTS * 2];

/* Request a new XID and an associated context. The former is globally
   unique and the latter is distinct from any other transaction whose
   lifetime overlaps with this one.
 */
XID xid_alloc();

/* Release an XID and its associated context. The XID will no longer
   be associated with any context after this call returns.
 */
inline void xid_free(XID x) {
  auto id = x.id();
  ASSERT(id < NCONTEXTS);
  auto *ctx = &contexts[id];
  ASSERT(ctx->state == TXN_CMMTD || ctx->state == TXN_ABRTD);
  LOG_IF(FATAL, ctx->owner != x) << "Invalid XID";
  ctx->owner._val = 0;
}

/* Return the context associated with the given XID.
 */
inline xid_context *xid_get_context(XID x) {
  auto *ctx = &contexts[x.id()];
  // read a consistent copy of owner
  XID owner = volatile_read(ctx->owner);
  if (!owner._val) {
    return nullptr;
  }
  ASSERT(owner.id() == x.id());
  if (owner.seq() != x.seq()) {
    return nullptr;
  }
  return ctx;
}

#if defined(SSN)
inline txn_state spin_for_cstamp(XID xid, xid_context *xc) {
  txn_state state;
  do {
    state = volatile_read(xc->state);
    if (volatile_read(xc->owner) != xid) {
      return TXN_INVALID;
    }
  } while (state != TXN_CMMTD and state != TXN_ABRTD);
  return state;
}
#endif
}  // namespace TXN
}  // namespace ermia
