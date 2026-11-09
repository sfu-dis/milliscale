#include <numa.h>
#include <sched.h>
#include <sys/mman.h>

#include <atomic>
#include <future>

#include "sm-alloc.h"
//#include "sm-chkpt.h"
#include "sm-common.h"
#include "sm-object.h"
#include "../txn.h"

namespace ermia {
namespace MM {

uint64_t safesnap_lsn = 0;

thread_local TlsFreeObjectPool *tls_free_object_pool CACHE_ALIGNED;
char **node_memory = nullptr;
uint64_t *allocated_node_memory = nullptr;
static uint64_t thread_local tls_allocated_node_memory CACHE_ALIGNED;
static const uint64_t tls_node_memory_mb = 200;

void prepare_node_memory() {
  if (!config::tls_alloc) {
    return;
  }

  ALWAYS_ASSERT(config::numa_nodes);
  allocated_node_memory =
      (uint64_t *)malloc(sizeof(uint64_t) * config::numa_nodes);
  node_memory = (char **)malloc(sizeof(char *) * config::numa_nodes);
  std::vector<std::future<void> > futures;
  LOG(INFO) << "Will run and allocate on " << config::numa_nodes << " nodes, "
            << config::node_memory_gb << "GB each";
  for (int i = 0; i < config::numa_nodes; i++) {
    LOG(INFO) << "Allocating " << config::node_memory_gb << "GB on node " << i;
    auto f = [=] {
      ALWAYS_ASSERT(config::node_memory_gb);
      allocated_node_memory[i] = 0;
      numa_set_preferred(i);
      node_memory[i] = (char *)mmap(
          nullptr, config::node_memory_gb * config::GB, PROT_READ | PROT_WRITE,
          MAP_ANONYMOUS | MAP_PRIVATE | MAP_HUGETLB | MAP_POPULATE, -1, 0);
      THROW_IF(node_memory[i] == nullptr or node_memory[i] == MAP_FAILED,
               os_error, errno, "Unable to allocate huge pages");
      ALWAYS_ASSERT(node_memory[i]);
      LOG(INFO) << "Allocated " << config::node_memory_gb << "GB on node " << i;
    };
    futures.push_back(std::async(std::launch::async, f));
  }
  for (auto &f : futures) {
    f.get();
  }
}

void gc_version_chain(fat_ptr *oid_entry) {
#if 0
  fat_ptr ptr = *oid_entry;
  Object *cur_obj = (Object *)ptr.offset();
  if (!cur_obj) {
    // Tuple is deleted, skip
    return;
  }

  // Start from the first **committed** version, and delete after its next,
  // because the head might be still being modified (hence its _next field)
  // and might be gone any time (tx abort). Skip records in chkpt file as
  // well - not even in memory.
  auto clsn = cur_obj->GetClsn();
  fat_ptr *prev_next = nullptr;
  if (clsn.asi_type() == fat_ptr::ASI_CHK) {
    return;
  }
  if (clsn.asi_type() != fat_ptr::ASI_LOG) {
    DCHECK(clsn.asi_type() == fat_ptr::ASI_XID);
    ptr = cur_obj->GetNextVolatile();
    cur_obj = (Object *)ptr.offset();
  }

  // Now cur_obj should be the fisrt committed version, continue to the version
  // that can be safely recycled (the version after cur_obj).
  ptr = cur_obj->GetNextVolatile();
  prev_next = cur_obj->GetNextVolatilePtr();

  while (ptr.offset()) {
    cur_obj = (Object *)ptr.offset();
    clsn = cur_obj->GetClsn();
    if (clsn == NULL_PTR || clsn.asi_type() != fat_ptr::ASI_LOG) {
      // Might already got recycled, give up
      break;
    }
    ptr = cur_obj->GetNextVolatile();
    prev_next = cur_obj->GetNextVolatilePtr();
    // If the chkpt needs to be a consistent one, must make sure not to GC a
    // version that might be needed by chkpt:
    // uint64_t glsn = std::min(logmgr->durable_flushed_lsn().offset(),
    // volatile_read(gc_lsn));
    // This makes the GC thread has to traverse longer in the chain, unless
    // with small log buffers or frequent log flush, which is bad for disk
    // performance.  For good performance, we use inconsistent chkpt which
    // grabs the latest committed version directly. Log replay after the
    // chkpt-start lsn is necessary for correctness.
    uint64_t glsn = volatile_read(gc_lsn);
    if (LSN::from_ptr(clsn).offset() <= glsn && ptr._ptr) {
      // Fast forward to the **second** version < gc_lsn. Consider that we set
      // safesnap lsn to 1.8, and gc_lsn to 1.6. Assume we have two versions
      // with LSNs 2 and 1.5.  We need to keep the one with LSN=1.5 although
      // its < gc_lsn; otherwise the tx using safesnap won't be able to find
      // any version available.
      //
      // We only traverse and GC a version chain when an update transaction
      // successfully installed a version. So at any time there will be only
      // one guy possibly doing this for a version chain - just blind write. If
      // we're traversing at other times, e.g., after committed, then a CAS is
      // needed: __sync_bool_compare_and_swap(&prev_next->_ptr, ptr._ptr, 0)
      volatile_write(prev_next->_ptr, 0);
      while (ptr.offset()) {
        cur_obj = (Object *)ptr.offset();
        clsn = cur_obj->GetClsn();
        ALWAYS_ASSERT(clsn.asi_type() == fat_ptr::ASI_LOG);
        ALWAYS_ASSERT(LSN::from_ptr(clsn).offset() <= glsn);
        fat_ptr next_ptr = cur_obj->GetNextVolatile();
        cur_obj->SetClsn(NULL_PTR);
        cur_obj->SetNextVolatile(NULL_PTR);
        if (!tls_free_object_pool) {
          tls_free_object_pool = new TlsFreeObjectPool;
        }
        tls_free_object_pool->Put(ptr);
        ptr = next_ptr;
      }
      break;
    }
  }
#endif
}

void *allocate(size_t size) {
  size = align_up(size);
  if (!config::tls_alloc) {
    return malloc(size);
  }

  void *p = NULL;

  // Try the tls free object store first
  if (tls_free_object_pool) {
    auto size_code = encode_size_aligned(size);
    fat_ptr ptr = tls_free_object_pool->Get(size_code);
    if (ptr.offset()) {
      p = (void *)ptr.offset();
      goto out;
    }
  }

  ALWAYS_ASSERT(!p);

  // Have to use the vanilla bump allocator, hopefully later we reuse them
  static thread_local char *tls_node_memory CACHE_ALIGNED;
  if (unlikely(!tls_node_memory) ||
      tls_allocated_node_memory + size >= tls_node_memory_mb * config::MB) {
    tls_node_memory = (char *)allocate_onnode(tls_node_memory_mb * config::MB);
    tls_allocated_node_memory = 0;
  }

  if (likely(tls_node_memory)) {
    p = tls_node_memory + tls_allocated_node_memory;
    tls_allocated_node_memory += size;
    goto out;
  }

out:
  LOG_IF(FATAL, !p) << "Out of memory";
  return p;
}

// Allocate memory directly from the node pool
void *allocate_onnode(size_t size) {
  size = align_up(size);
  auto node = numa_node_of_cpu(sched_getcpu());
  ALWAYS_ASSERT(node < config::numa_nodes);
  auto offset = __sync_fetch_and_add(&allocated_node_memory[node], size);
  if (likely(offset + size <= config::node_memory_gb * config::GB)) {
    return node_memory[node] + offset;
  }
  return nullptr;
}

void deallocate(fat_ptr p) {
  if (!ermia::config::enable_gc) {
    return;
  }

  ASSERT(p != NULL_PTR);
  ASSERT(p.size_code());
  ASSERT(p.size_code() != INVALID_SIZE_CODE);
  Object *obj = (Object *)p.offset();
  obj->SetNextVolatile(NULL_PTR);
  obj->SetCSN(NULL_PTR);
  if (!tls_free_object_pool) {
    tls_free_object_pool = new TlsFreeObjectPool;
  }
  tls_free_object_pool->Put(p);
}
}  // namespace MM
}  // namespace ermia
