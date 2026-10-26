#ifndef MILLISCALE_VERSIONBUFFER_H
#define MILLISCALE_VERSIONBUFFER_H
#include "ringbuffer.h"
#include "serial.h"
#include "sm-object.h"
#include "../tuple.h"

constexpr int VERSION_BUFFER_CAPACITY = 200;

class VersionBuffer {
public:
    VersionBuffer(char* memory)
        : ringbuffer_(memory, VERSION_BUFFER_CAPACITY)
    {
    }

    bool allocate(uint64_t size)
    {
        auto result = ringbuffer_.try_reserve(size);
        // result is nullptr if we have no space; must evict
        while (!result)
        {
            auto* first_element = ringbuffer_.peek_front();
            // for now, lets assume we store Objects. Then, we would have gotten back Object*
            auto* first_obj = static_cast<ermia::Object*>(first_element);
            auto* tuple = reinterpret_cast<ermia::dbtuple*>(first_obj->GetPayload());
            uint64_t bytes = ermia::align_up(sizeof(ermia::Object) + sizeof(ermia::dbtuple) + tuple->size);
            ringbuffer_.evict(bytes);
            result = ringbuffer_.try_reserve(size);
        }

        return true;
    }

private:
    RingBuffer ringbuffer_;
};

#endif //MILLISCALE_VERSIONBUFFER_H
