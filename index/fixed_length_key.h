#pragma once

namespace ermia {

template <uint32_t KeyLength>
struct FixedLengthKey {
  char key[KeyLength];

  // comparators
  bool operator==(const FixedLengthKey &other) {
    return memcmp(key, other.key, KeyLength) == 0;
  }

  bool operator>(const FixedLengthKey &other) {
    return memcmp(key, other.key, KeyLength) > 0;
  }

  bool operator>=(const FixedLengthKey &other) {
    return memcmp(key, other.key, KeyLength) >= 0;
  }

  bool operator<(const FixedLengthKey &other) {
    return memcmp(key, other.key, KeyLength) < 0;
  }
  bool operator<=(const FixedLengthKey &other) {
    return memcmp(key, other.key, KeyLength) <= 0;
  }
  bool operator!=(const FixedLengthKey &other) {
    return memcmp(key, other.key, KeyLength) != 0;
  }
};

}  // namespace ermia
