#pragma once
#include <unordered_map>
#include <unordered_set>
#include "sm-config.h"
#include "sm-defs.h"
#include "sm-object.h"
#include "dlog.h"

namespace ermia {

/*
 * tzwang(20250913): Memory allocation, deallocation, and garbage collection:
 *
 * The engine reserves large chunks of hugepages from the OS upon
 * initialization for each socket. Each thread maintains a local pool of free
 * memory got from the corresponding socket's reserved memory and serves
 * requests using this TLS pool (bump allocator). All memory allocations are
 * rounded up to a certain alignment size. Deallocated objects are stored in
 * thread-local free object pools for reuse. During each succeeded update
 * operation, the underlying thread will traverse the updated version chain to
 * try garbage collecting versions that are no needed any more (decided by the
 * minimum begin CSN across all threads). Objects GC'ed from version chains 
 * will be put again to the TLS free objects pool. It is the update threads
 * that traverse the chain and recycle stale versions because an update means
 * we're potentially making older versions stale and becoming candidates of GC.
 *
 * The TLS free object pool is a hash table indexed by aligned object sizes.
 * Objects of the same size will be put into the same set (std::unordered_set).
 *
 * Upon allocation, if the TLS object pool is non-empty, the thread will try to
 * find an object of the requested size in this pool, instead of the TLS bump
 * allocator.  If the free object pool is empty, we continue with the bump
 * allocator; if the bump allocator also doesn't have free memory, we ask the
 * central per-socket reserved memory pool; if we still don't get any free
 * memory, the allocation fails.
 *
 * In this way, GC time is amortized in updates, saving extra costs for
 * maintaining the set of updated OIDs and extra thread resources for the GC
 * thread.
 */
namespace MM {
void gc_version_chain(fat_ptr *oid_entry);

// A hashtab storing recycled (freed) objects by size. No CC.
class TlsFreeObjectPool {
 private:
  // The unordered_set stores a fat_ptr (just in the form of an int)
  std::unordered_map<size_t, std::unordered_set<uint64_t> *> pool_;

 public:
  TlsFreeObjectPool() {}
  inline void Put(fat_ptr ptr) {
    if (pool_.find(ptr.size_code()) == pool_.end()) {
      pool_[ptr.size_code()] = new std::unordered_set<uint64_t>;
    }
    pool_[ptr.size_code()]->insert(ptr._ptr);
  }

  inline fat_ptr Get(uint16_t size_code) {
    uint64_t gc_csn = dlog::get_min_thread_begin_csn();

    if (pool_.find(size_code) != pool_.end()) {
      auto *set = pool_[size_code];
      uint32_t tries = 10;
      for (auto &p : *set) {
        if (p) {
          fat_ptr ret_ptr{p};
          Object *obj = (Object *)ret_ptr.offset();
          if (obj->GetCSN().offset() < gc_csn) {
            set->erase(p);
            return ret_ptr;
          }
          // XXX(tzwang): try a few times, too slow to look at all candidates.
          // Tune it if memory space is a concern.
          // TODO(tzwang): make this a list, consume from one end, add at
          // another.
          if (--tries == 0) {
            return NULL_PTR;
          }
        }
      }
    }
    return NULL_PTR;
  }
};

extern uint64_t safesnap_lsn;

void prepare_node_memory();
void *allocate(size_t size);
void deallocate(fat_ptr p);
void *allocate_onnode(size_t size);
}  // namespace MM
}  // namespace ermia
