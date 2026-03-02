#pragma once

#include "dbcore/sm-common.h"
#include "util/util.h"

#ifdef MASSTREE
#include "masstree/string_slice.hh"
#endif

namespace ermia {
struct varstr {
  inline varstr() : l(0), p(NULL) {}
  inline varstr(const uint8_t *p, uint32_t l) : l(l), p(p) {}
  inline varstr(const char *p, uint32_t l) : l(l), p((const uint8_t *)p) {}
  inline void copy_from(const uint8_t *s, uint32_t l) {
    copy_from((const char *)s, l);
  }
  inline void copy_from(const varstr *v) { copy_from(v->p, v->l); }

  inline void copy_from(const char *s, uint32_t ll) {
    if (ll) {
      memcpy((void *)p, s, ll);
    }
    l = ll;
  }

  inline bool operator==(const varstr &that) const {
    if (size() != that.size()) return false;
    return memcmp(data(), that.data(), size()) == 0;
  }

  inline bool operator!=(const varstr &that) const { return !operator==(that); }

  inline bool operator<(const varstr &that) const {
    int r = memcmp(data(), that.data(), std::min(size(), that.size()));
    return r < 0 || (r == 0 && size() < that.size());
  }

  inline bool operator>=(const varstr &that) const { return !operator<(that); }

  inline bool operator<=(const varstr &that) const {
    int r = memcmp(data(), that.data(), std::min(size(), that.size()));
    return r < 0 || (r == 0 && size() <= that.size());
  }

  inline bool operator>(const varstr &that) const { return !operator<=(that); }

#ifdef MASSTREE
  inline uint64_t slice_at(int pos) const {
    return string_slice<uint64_t>::make_comparable((const char *)p + pos,
                                                   std::min(int(l - pos), 8));
  }
#endif
  inline uint32_t size() const { return l; }
  inline const uint8_t *data() const { return p; }
  inline uint8_t *data() { return (uint8_t *)(void*)p; }

#ifdef MASSTREE
  inline operator lcdf::Str() const { return lcdf::Str(p, l); }
#endif

  inline void prefetch() {
    uint32_t i = 0;
    do {
      ::prefetch((const char *)(p + i));
      i += CACHELINE_SIZE;
    } while (i < l);
  }

  uint64_t l;
  fat_ptr ptr;
  const uint8_t *p;  // must be the last field
};
}  // namespace ermia
