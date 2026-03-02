#pragma once

#include <glog/logging.h>
#include <immintrin.h>
#include <stdint.h>

#include <atomic>
#include <type_traits>

#include "../macros.h"

struct OptLock {
  static constexpr uint64_t kLockedBit = 1ull << 63;
  static constexpr uint64_t kInvalidVersion = 0;
  static constexpr uint64_t kVersionStride = 1;
  static constexpr uint64_t kNextUnlockedVersion = kVersionStride - kLockedBit;

  static inline bool HasLockedBit(const uint64_t val) {
    return val & kLockedBit;
  }

  static inline bool is_version(const uint64_t ptr) { return !HasLockedBit(ptr); }

  static inline uint64_t MakeLockedVersion(const uint64_t v) {
    ASSERT(!HasLockedBit(v));
    return v | kLockedBit;
  }

  std::atomic<uint64_t> lock{0};

  uint64_t Lock() {
    int cas_failure = 0;
    while (true) {
      bool restart = false;
      uint64_t version = TryBeginRead(restart);
      if (restart) {
        continue;
      }

      uint64_t curr = version;
      uint64_t locked = MakeLockedVersion(version);
      if (!lock.compare_exchange_strong(curr, locked)) {
        // XXX(shiges): This seem to hinder performance; maybe tune params?
        // cas_failure++;
        continue;
      }
      // lock acquired
      return version;
    }
  }

  bool TryLock(uint64_t version) {
    if (!ValidateRead(version)) {
      return false;
    }

    uint64_t curr = version;
    uint64_t locked = MakeLockedVersion(version);
    return lock.compare_exchange_strong(curr, locked);
  }

  void Unlock() {
    ASSERT(HasLockedBit(lock.load()));
    lock.fetch_add(kNextUnlockedVersion);
  }

  void Unlock(uint64_t v) {
    (void)v;
    Unlock();
  }

  uint64_t BeginRead() const {
    while (true) {
      bool restart = false;
      uint64_t version = TryBeginRead(restart);
      if (!restart) {
        return version;
      }
    }
  }

  uint64_t TryBeginRead(bool &restart) const {
    uint64_t version = lock.load(std::memory_order_acquire);
    restart = HasLockedBit(version);
    // If [restart] hasn't been changed to false, [version]
    // is guaranteed to be a version.
    return version;
  }

  bool ValidateRead(uint64_t version) const {
    uint64_t v = lock.load(std::memory_order_acquire);
    return version == v;
  }

  bool IsLocked() const {
    uint64_t version = lock.load(std::memory_order_acquire);
    return HasLockedBit(version);
  }
};
