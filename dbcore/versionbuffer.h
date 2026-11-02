#ifndef MILLISCALE_VERSIONBUFFER_H
#define MILLISCALE_VERSIONBUFFER_H
#include "ringbuffer.h"
#include "serial.h"
#include "sm-object.h"
#include "../tuple.h"
#include "sm-config.h"

constexpr int VERSION_BUFFER_CAPACITY = 200 * ermia::config::MB;  // It might be a good idea to let caller pass capacity
                                              // (so threads can ensure enough memory)

class VersionBuffer {
public:
    VersionBuffer(char* memory)
        : ringbuffer_(memory, VERSION_BUFFER_CAPACITY)
    {
    }

    void* allocate(size_t size)
    {
        if (size == 0 || size > VERSION_BUFFER_CAPACITY) return nullptr;
        auto result = ringbuffer_.try_reserve(size);
        // result is nullptr if we have no space; must evict
        while (!result)
        {
            auto* first_element = ringbuffer_.peek_front();
            if (!first_element) return nullptr;

            // for now, lets assume we store Objects. Then, we would have gotten back Object*
            auto* first_obj = static_cast<ermia::Object*>(first_element);
            auto* tuple = reinterpret_cast<ermia::dbtuple*>(first_obj->GetPayload());
            uint64_t bytes = ermia::align_up(sizeof(ermia::Object) + sizeof(ermia::dbtuple) + tuple->size);
            ringbuffer_.evict(bytes);
            result = ringbuffer_.try_reserve(size);
        }

        return result;
    }

private:
    RingBuffer ringbuffer_;
};

#endif //MILLISCALE_VERSIONBUFFER_H
