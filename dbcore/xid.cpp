#include "serial.h"
#include "sm-config.h"
#include "xid.h"

namespace ermia {
namespace TXN {

/*
 * There are a fixed number of transaction contexts in the system - without designs like CoroBase or
 * PreemptDB, there will be only one transaction pert thread at any give time, and our primary task
 * is to create unique XIDs and then assign them contexts in a way that no two XID map to the same
 * context at the same time. We assume thread-to-transaction (ie no CoroBase or PreemptDB yet) and
 * accomplish this as follows:
 *
 * The system maintains a single array of transaction contexts which all threads race to allocate
 * upon processing the first transaction. After that, each thread keeps and reuses the context.
 *
 * The system-wide contexts array is fixed-length and can be exactly the maximum number of threads
 * we want to support. Currently the hard limit is 65536 since it is indexed by a 16-bit unsigned
 * int.
 *
 * Each thread maintains also a thread-local sequence number to avoid the ABA problem which may
 * happen when a thread A takes a copy of B's XID and obtains a context that is now being used by a
 * different transaction but has the same id (i.e., same entry in the contexts array).
 *
 * With the id and sequence number, the XID is a 48-bit integer, followed by an ASI_XID identifier.
 * This gives the following layout of a 64-bit integer:
 * |------ sequence number (32 bits)------|----id (16 bits) ----|-----flags (16 bits----|
 *
 * We can expand this to support multiple XID/transactions per thread by allowing multiple contexts
 * entries (thus multiple context ids) per thread. 
 */

// Central context id counter
std::atomic<uint16_t> num_contexts(0);

// All contexts we have in the system, two per thread; NCONTEXTS must be >= the max number of
// threads supported
xid_context contexts[NCONTEXTS * 2];

// TID epochs per thread
uint64_t xid_epochs[ermia::config::MAX_THREADS];

// Allocate an XID (and its associated xid_context)
XID xid_alloc() {
  thread_local bool initialized = false;
  thread_local uint32_t seq = 0;
  thread_local uint16_t id = 0;
  thread_local uint64_t my_epoch = 0;

  if (!initialized) {
    id = num_contexts.fetch_add(1, std::memory_order_release);
    initialized = true;
    xid_epochs[id] = my_epoch = 0;
  }

  // TODO(tzwang): handle sequence number wrap-around
  if (seq == ~uint32_t{0}) {
    // Advance my epoch
    my_epoch = volatile_read(xid_epochs[id]) + 1;
    volatile_write(xid_epochs[id], my_epoch);

    // Start to use the other context of mine if it is now available
    for (uint32_t i = 0; i < ermia::config::MAX_THREADS; ++i) {
      if (i != id) {
        // Wait for everyone else to advance
        // TODO(tzwang): more allowed epochs for better straggler handling
        while (volatile_read(xid_epochs[i]) < my_epoch) {}
      }
    }
  }

  XID x = XID::make(++seq, id);
  new (&contexts[id + NCONTEXTS * (my_epoch % 2)]) xid_context(x);
  return x;
}

#ifdef SSN
bool xid_context::set_sstamp(uint64_t s) {
  ALWAYS_ASSERT(!(s & xid_context::sstamp_final_mark));
  // If I'm not read-mostly, nobody else would call this
  if (xct->is_read_mostly() && config::ssn_read_opt_enabled()) {
    // This has to be a CAS because with read-optimization, the updater might
    // need
    // to update the reader's sstamp.
    uint64_t ss = sstamp.load(std::memory_order_acquire);
    do {
      if (ss & sstamp_final_mark) {
        return false;
      }
    } while ((ss == 0 || ss > s) &&
             !std::atomic_compare_exchange_strong(&sstamp, &ss, s));
  } else {
    sstamp.store(std::min(sstamp.load(std::memory_order_relaxed), s),
                 std::memory_order_relaxed);
  }
  return true;
}
#endif
}  // namespace TXN
}  // namespace ermia
