#pragma once
#include <algorithm>
#include <random>
#include <vector>

template <typename T>
struct ReservoirSampler {
  ReservoirSampler(std::size_t k = 3000)
    : k(k), n(0), rng(std::random_device{}()), sorted(false) {
    reservoir.reserve(k);
  }

  void process(const T& value) {
    ++n;
    if (reservoir.size() < k) {
      reservoir.push_back(value);
    } else {
      std::uniform_int_distribution<std::size_t> dist(0, n - 1);
      std::size_t j = dist(rng);
      if (j < k) {
        reservoir[j] = value;
      }
    }
    sorted = false;
  }

  const std::vector<T>& result() const {
    return reservoir;
  }

  uint64_t get_pct(double pct) {
    // assert(n > 0 && pct >= 0 && pct <= 1);
    if (!sorted) {
      std::sort(reservoir.begin(), reservoir.end());
    }
    auto lb = std::min(k, n);
    return reservoir[uint32_t(pct * (lb-1))];
  }

  void clear() {
    n = 0;
    sorted = false;
    reservoir.clear();
  }

  std::size_t k;
  std::size_t n;
  std::vector<T> reservoir;
  std::mt19937 rng;
  bool sorted;
};
