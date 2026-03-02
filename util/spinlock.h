#pragma once

#include <atomic>
#include <cpuid.h>
#include <immintrin.h>
#include <x86intrin.h>

#define MIN_DELAY_MICROSEC 1L

#define DELAY(n)                       \
  do {                                 \
    volatile int x = 0;                \
    for (int i = 0; i < (n); ++i) x++; \
  } while (0)

constexpr int kExpBackoffBase = 4000;
constexpr int kExpBackoffLimit = 32000;
constexpr int kExpBackoffMultiplier = 2;

namespace ermia {
static void microdelay(long microsec) {
  if (microsec > 0) {
    struct timeval delay;
    delay.tv_sec = microsec / 1000000L;
    delay.tv_usec = microsec % 1000000L;
    (void) select(0, NULL, NULL, NULL, &delay);
  }
}

struct TATAS {
  std::atomic<uint64_t> lock_word;
  uint64_t attempt;
  uint64_t success;

  TATAS() : lock_word(0), attempt(0), success(0) {}
  ~TATAS() {}

  inline void lock() {
    int cas_failure = 0;
    uint64_t seed = (uintptr_t)(&cas_failure);
    auto next_u32 = [&]() {
      seed = seed * 0xD04C3175 + 0x53DA9022;
      return (seed >> 32) ^ (seed & 0xFFFFFFFF);
    };

    next_u32();
    int maxDelay = kExpBackoffBase;
  retry:
    auto locked = lock_word.load(std::memory_order_acquire);
    if (locked) {
      int delay = next_u32() % maxDelay;
      maxDelay = std::min(maxDelay * kExpBackoffMultiplier, kExpBackoffLimit);
      DELAY(delay);
      goto retry;
    }

    if (!lock_word.compare_exchange_strong(locked, 1ul)) {
      cas_failure++;
      int delay = next_u32() % maxDelay;
      maxDelay = std::min(maxDelay * kExpBackoffMultiplier, kExpBackoffLimit);
      DELAY(delay);
      goto retry;
    }
  }

  inline void unlock() {
    lock_word.store(0, std::memory_order_release);
  }
};

}
