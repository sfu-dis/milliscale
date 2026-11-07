#include <iostream>
#include <memory>
#include <stdexcept>

#include "../../dbcore/ringbuffer.h"
#include "../../dbcore/versionbuffer.h"

void check(bool condition, const char* message) {
    if (!condition)
        throw std::runtime_error(message);
}

void test_ringbuffer() {
    char memory[32]{};
    RingBuffer ring(memory, sizeof(memory));

    check(ring.peek_front() == nullptr, "Ring should start empty");

    check(ring.try_reserve(16) == memory,
          "First allocation should start at the beginning");
    check(ring.try_reserve(16) == memory + 16,
          "Second allocation should fill the ring");
    check(ring.try_reserve(1) == nullptr,
          "Full ring should reject allocations");

    check(ring.evict(16) == memory,
          "Should evict the oldest allocation");
    check(ring.peek_front() == memory + 16,
          "Front should advance");

    check(ring.try_reserve(8) == memory,
          "Allocation should wrap to the beginning");
}

void test_versionbuffer() {
    auto memory = std::make_unique<char[]>(VERSION_BUFFER_CAPACITY);
    VersionBuffer buffer(memory.get());

    check(buffer.allocate(0) == nullptr,
          "Zero-sized request should fail");
    check(buffer.allocate(size_t(VERSION_BUFFER_CAPACITY) + 1) == nullptr,
          "Oversized request should fail");

    check(buffer.allocate(64) == memory.get(),
          "First allocation should start at the beginning");
    check(buffer.allocate(128) == memory.get() + 64,
          "Second allocation should follow the first");
}

int main() {
    try {
        test_ringbuffer();
        test_versionbuffer();
        std::cout << "All tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Test failed: " << error.what() << '\n';
        return 1;
    }
}