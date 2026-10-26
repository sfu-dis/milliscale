#ifndef MILLISCALE_RING_H
#define MILLISCALE_RING_H
#include "util.h"

class RingBuffer {
public:
    RingBuffer()
        : pointer(nullptr), head(0), tail(0), capacity_(0)
    {
    }

    RingBuffer(char* storage, const uint64_t capacity)  // storage here is just a pointer to the first byte of allocation
    : pointer(storage), head(0), tail(0), capacity_(capacity), wrap_around(capacity)
    {
    }

    void* try_reserve(const uint64_t size)
    {
        // Note that, reserve really doesn't 'reserve' anything. It just tells us where we can put our version
        // by giving us an address
        const uint64_t current_tail = tail;
        uint64_t limit = (tail < head) ? head : capacity_;
        if (tail + size < limit)
        {
            // then we are good to allocate
            tail += size;
        }
        else
        {
            // not enough space, we need to wrap around - i guess this gets complicated for a variable sized buffer
            // we may need to pad and then evict
            // We want the caller to evict something. Let's check to see if they already have

            if (head == tail) return nullptr; // full ring buffer; evict first

            if (head > size)
            {
                // Since we wrap the head, we can assume that it tells us how much space is left behind by eviction
                // So if head > size, then we have enough space in front of head for our new object
                wrap_around = tail;
                tail = size;
                return pointer;
            }
            return nullptr;
        }
        return pointer + current_tail;
    }

    void* evict(uint64_t size)
    {
        // we assume that it is the callers responsibility to track the version sizes as we want ringbuffer to be
        // generic - that is, in VersionBuffer, we should call evict(version.size) so one version is removed. We loop
        // through the versions until we find enough space for our new allocation
        // This solves the problem where if we need to ensure that if more than one version needs to be evicted
        // to make room for size given,
        // then we evict both whole versions rather than one version and a half of another

        const uint64_t old_head = head;
        head += size;
        if (head == wrap_around || head == capacity_) {
            head = 0;
            wrap_around = capacity_;
        }
        return pointer + old_head;
    }

    void* peek_front()
    {
        // lets assume we have something in the ring for now since we will need to track empty
        // TODO: add is_empty check
        return pointer + head;
    }

private:
    char* pointer;  // byte sized pointer to some memory that will be handed to us - this should never really change
                    // apparently, c++17 has std::byte. can we use assume c++17?
                    // I guess, we should know how much memory each ringbuffer maintains
                    // do I have to worry about aligned? we have two things: 64 bytes cache align and 16 bytes alignment
    uint64_t head;
    uint64_t tail;
    const uint64_t capacity_;  // in bytes, not element count
    uint64_t wrap_around;
};

#endif //MILLISCALE_RING_H
