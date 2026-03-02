#pragma once

#include <chrono>
#include <iostream>
#include <sstream>
#include <string>
#include <limits>
#include <queue>
#include <utility>
#include <memory>
#include <atomic>
#include <tuple>
#include <algorithm>

#include <stdint.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>
#include <cxxabi.h>

#include "../macros.h"

namespace util {

static uint64_t rdtsc(void) {
#if defined(__x86_64__)
  uint32_t low, high;
  asm volatile("rdtsc" : "=a"(low), "=d"(high));
  return (static_cast<uint64_t>(high) << 32) | static_cast<uint64_t>(low);
#else
#pragma message("Warning: unknown architecture, no rdtsc() support")
  return 0;
#endif
}

// padded, aligned primitives
template <typename T, bool Pedantic = true>
class aligned_padded_elem {
 public:
  template <class... Args>
  aligned_padded_elem(Args &&... args)
      : elem(std::forward<Args>(args)...) {
    if (Pedantic) ALWAYS_ASSERT(((uintptr_t) this % CACHELINE_SIZE) == 0);
  }

  T elem;
  CACHE_PADOUT;

  // syntactic sugar- can treat like a pointer
  inline T &operator*() { return elem; }
  inline const T &operator*() const { return elem; }
  inline T *operator->() { return &elem; }
  inline const T *operator->() const { return &elem; }

 private:
  inline void __cl_asserter() const {
    static_assert((sizeof(*this) % CACHELINE_SIZE) == 0, "xx");
  }
} CACHE_ALIGNED;

// some pre-defs
typedef aligned_padded_elem<uint8_t> aligned_padded_u8;
typedef aligned_padded_elem<uint16_t> aligned_padded_u16;
typedef aligned_padded_elem<uint32_t> aligned_padded_u32;
typedef aligned_padded_elem<uint64_t> aligned_padded_u64;

template <typename T>
struct host_endian_trfm {
  ALWAYS_INLINE T operator()(const T &t) const { return t; }
};

template <>
struct host_endian_trfm<uint16_t> {
  ALWAYS_INLINE uint16_t operator()(uint16_t t) const {
    return be16toh(t);
  }
};

template <>
struct host_endian_trfm<int16_t> {
  ALWAYS_INLINE int16_t operator()(int16_t t) const {
    return be16toh(t);
  }
};

template <>
struct host_endian_trfm<int32_t> {
  ALWAYS_INLINE int32_t operator()(int32_t t) const {
    return be32toh(t);
  }
};

template <>
struct host_endian_trfm<uint32_t> {
  ALWAYS_INLINE uint32_t operator()(uint32_t t) const {
    return be32toh(t);
  }
};

template <>
struct host_endian_trfm<int64_t> {
  ALWAYS_INLINE int64_t operator()(int64_t t) const {
    return be64toh(t);
  }
};

template <>
struct host_endian_trfm<uint64_t> {
  ALWAYS_INLINE uint64_t operator()(uint64_t t) const {
    return be64toh(t);
  }
};

template <typename T>
struct big_endian_trfm {
  ALWAYS_INLINE T operator()(const T &t) const { return t; }
};

template <>
struct big_endian_trfm<uint16_t> {
  ALWAYS_INLINE uint16_t operator()(uint16_t t) const {
    return htobe16(t);
  }
};

template <>
struct big_endian_trfm<int16_t> {
  ALWAYS_INLINE int16_t operator()(int16_t t) const {
    return htobe16(t);
  }
};

template <>
struct big_endian_trfm<int32_t> {
  ALWAYS_INLINE int32_t operator()(int32_t t) const {
    return htobe32(t);
  }
};

template <>
struct big_endian_trfm<uint32_t> {
  ALWAYS_INLINE uint32_t operator()(uint32_t t) const {
    return htobe32(t);
  }
};

template <>
struct big_endian_trfm<int64_t> {
  ALWAYS_INLINE int64_t operator()(int64_t t) const {
    return htobe64(t);
  }
};

template <>
struct big_endian_trfm<uint64_t> {
  ALWAYS_INLINE uint64_t operator()(uint64_t t) const {
    return htobe64(t);
  }
};

// not thread-safe
//
// taken from java:
//   http://developer.classpath.org/doc/java/util/Random-source.html
class fast_random {
 public:
  fast_random(unsigned long seed) : seed(0) { set_seed0(seed); }

  inline unsigned long next() {
    return ((unsigned long)next(32) << 32) + next(32);
  }

  inline uint32_t next_u32() { return next(32); }

  inline uint16_t next_u16() { return next(16); }

  /** [0.0, 1.0) */
  inline double next_uniform() {
    return (((unsigned long)next(26) << 27) + next(27)) / (double)(1L << 53);
  }

  inline char next_char() { return next(8) % 256; }

  inline char next_readable_char() {
    static const char readables[] =
        "0123456789@ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
    return readables[next(6)];
  }

  inline std::string next_string(size_t len) {
    std::string s(len, 0);
    for (size_t i = 0; i < len; i++) s[i] = next_char();
    return s;
  }

  inline std::string next_readable_string(size_t len) {
    std::string s(len, 0);
    for (size_t i = 0; i < len; i++) s[i] = next_readable_char();
    return s;
  }

  inline unsigned long get_seed() { return seed; }

  inline void set_seed(unsigned long seed) { this->seed = seed; }

 private:
  inline void set_seed0(unsigned long seed) {
    this->seed = (seed ^ 0x5DEECE66DL) & ((1L << 48) - 1);
  }

  inline unsigned long next(unsigned int bits) {
    seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
    return (unsigned long)(seed >> (48 - bits));
  }

  unsigned long seed;
};

template <typename ForwardIterator>
std::string format_list(ForwardIterator begin, ForwardIterator end) {
  std::ostringstream ss;
  ss << "[";
  bool first = true;
  while (begin != end) {
    if (!first) ss << ", ";
    first = false;
    ss << *begin++;
  }
  ss << "]";
  return ss.str();
}

struct timer {
  timer &operator=(const timer &) = delete;
  timer(timer &&) = delete;
  ~timer() {}

  timer() { start = std::chrono::steady_clock::now(); }

  inline uint64_t lap_ms() {
    auto t0 = start;
    auto t1 = std::chrono::steady_clock::now();
    start = t1;
    return std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
  }

  inline uint64_t lap_us() {
    auto t0 = start;
    auto t1 = std::chrono::steady_clock::now();
    start = t1;
    return std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
  }

  std::chrono::steady_clock::time_point start;
};

class scoped_timer {
 private:
  timer t;
  std::string region;
  bool enabled;

 public:
  scoped_timer(const std::string &region, bool enabled = true)
      : region(region), enabled(enabled) {}

  ~scoped_timer() {
    if (enabled) {
      std::cerr << "timed region " << region << " took " << t.lap_ms() << " ms" << std::endl;
    }
  }
};

static inline std::vector<std::string> split(const std::string &s, char delim) {
  std::vector<std::string> elems;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, delim)) elems.emplace_back(item);
  return elems;
}

}  // namespace util

// pretty printer for std::pair<A, B>
template <typename A, typename B>
inline std::ostream &operator<<(std::ostream &o, const std::pair<A, B> &p) {
  o << "[" << p.first << ", " << p.second << "]";
  return o;
}

// pretty printer for std::vector<T, Alloc>
template <typename T, typename Alloc>
static std::ostream &operator<<(std::ostream &o,
                                const std::vector<T, Alloc> &v) {
  bool first = true;
  o << "[";
  for (auto &p : v) {
    if (!first) o << ", ";
    first = false;
    o << p;
  }
  o << "]";
  return o;
}

// pretty printer for std::tuple<...>
namespace private_ {
template <size_t Idx, bool Enable, class... Types>
struct helper {
  static inline void apply(std::ostream &o, const std::tuple<Types...> &t) {
    if (Idx) o << ", ";
    o << std::get<Idx, Types...>(t);
    helper<Idx + 1, (Idx + 1) < std::tuple_size<std::tuple<Types...>>::value,
           Types...>::apply(o, t);
  }
};

template <size_t Idx, class... Types>
struct helper<Idx, false, Types...> {
  static inline void apply(std::ostream &o, const std::tuple<Types...> &t) {
    MARK_REFERENCED(o);
    MARK_REFERENCED(t);
  }
};
}

template <class... Types>
static inline std::ostream &operator<<(std::ostream &o,
                                       const std::tuple<Types...> &t) {
  o << "[";
  private_::helper<0, 0 < std::tuple_size<std::tuple<Types...>>::value,
                   Types...>::apply(o, t);
  o << "]";
  return o;
}

/**
 * Barrier implemented by spinning
 */

class spin_barrier {
 public:
  spin_barrier(size_t n) : n(n) {}

  spin_barrier(const spin_barrier &) = delete;
  spin_barrier(spin_barrier &&) = delete;
  spin_barrier &operator=(const spin_barrier &) = delete;

  ~spin_barrier() { ALWAYS_ASSERT(n == 0); }

  void count_down() {
    // written like this (instead of using __sync_fetch_and_add())
    // so we can have assertions
    for (;;) {
      size_t copy = n;
      ALWAYS_ASSERT(copy > 0);
      if (__sync_bool_compare_and_swap(&n, copy, copy - 1)) return;
    }
  }

  void wait_for() {
    while (n > 0) NOP_PAUSE;
  }

 private:
  volatile size_t n;
};
