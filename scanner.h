#include <unistd.h>
#include <sys/types.h>
#include <iostream>
#include <cstring>
#include <cstdint>
#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <functional>
#include "tuple.h"
#include "dbcore/dlog-tx.h"

#define IO_SIZE (2*1024*1024)

int32_t read_page_from_file(int fd, size_t page_size, size_t page_id, void* pointer) {
    if (fd < 0) {
        std::cerr << "Error: Invalid file descriptor." << std::endl;
        return -1;
    }
    if (pointer == nullptr) {
        std::cerr << "Error: Destination pointer is null." << std::endl;
        return -1;
    }
    if (page_size == 0) {
        std::cerr << "Error: Page size cannot be zero." << std::endl;
        return -1;
    }

    off_t offset = static_cast<off_t>(page_id) * page_size;
    ssize_t bytes_read = pread(fd, pointer, page_size, offset);

    if (bytes_read == -1) {
        std::cerr << "Read error: " << std::strerror(errno) 
                  << " (Page ID: " << page_id << ", Offset: " << offset << ")" << std::endl;
        return -1;
    } 
    
    return bytes_read;
}

void* parse_log_block_records(const void* block_ptr, const void* end_ptr, std::function<void(FID, OID, uint64_t, uint32_t, char*)> callback) {
    // No enough space for log block header
    if ((char*)block_ptr + sizeof(log_block) >= end_ptr) {
        return nullptr;
    }
    
    const auto* lb = static_cast<const log_block*>(block_ptr);

    // No enough space for log block body
    if ((char*)block_ptr + sizeof(log_block) + lb->payload_size > end_ptr) {
        return nullptr;
    }

    uint32_t current_offset = 0;
    int record_count = 0;

    while (current_offset + sizeof(log_record) <= lb->payload_size) {
        
        const auto* rec = reinterpret_cast<const log_record*>(lb->payload + current_offset);
        const auto* tuple = reinterpret_cast<const ermia::dbtuple*>(rec->data);
        callback(rec->fid, rec->oid, rec->csn, tuple->size, (char*) &tuple->value_start);
        // std::cout << "  Record #" << ++record_count << ":" << std::endl;
        // std::cout << "    Type: " << static_cast<int>(rec->type) 
        //           << " (Size: " << rec->size << ", FID: " << rec->fid 
        //           << ", OID: " << rec->oid << ", CSN: " << rec->csn << ")" << std::endl;

        // Check if record size exceed payload size
        assert (current_offset + rec->size <= lb->payload_size);

        // // Read tuple
        // if (current_offset + sizeof(log_record) + sizeof(ermia::dbtuple) <= lb->payload_size) {
        //     const auto* tuple = reinterpret_cast<const ermia::dbtuple*>(rec->data);
        //     // std::cout << "    Tuple Size: " << tuple->size << std::endl;
        //     // std::cout << "    current offset: " << current_offset << std::endl;
        // }

        uint32_t step = align_up(rec->size + sizeof(log_record));
        current_offset += step;

        if (current_offset >= lb->payload_size) {
            break;
        }
    }
    
    return (void*) (lb->payload + lb->capacity);
}

void parse_log_stream(int fd, std::function<void(FID, OID, uint64_t, uint32_t, char*)> callback) {
    char* buff = new char[IO_SIZE * 2]; 
    
    size_t current_page_id = 0;
    size_t pages_read_count = 0;
    
    size_t residual_bytes = 0;

    while (true) {
    
        char* read_target_ptr = buff + residual_bytes;
        int read_bytes = read_page_from_file(fd, IO_SIZE, current_page_id, read_target_ptr);
        if (read_bytes < 0) {
            std::cerr << "[Stream] Read Error." << std::endl;
            break; 
        }
        if (read_bytes == 0) {
            std::cerr << "[Stream] Reached EOF." << std::endl;
            break; 
        }

        pages_read_count++;
        current_page_id++;

        size_t total_valid_bytes = residual_bytes + read_bytes;
        char* buff_end = buff + total_valid_bytes;

        char* next_lb = buff;
        char* last_successful_lb = buff;

        while (next_lb != nullptr && next_lb < buff_end) {
            last_successful_lb = next_lb;
            next_lb = static_cast<char*>(parse_log_block_records(next_lb, buff_end, callback));
        }

        if (next_lb == buff_end) {
            residual_bytes = 0;
        } else {
            residual_bytes = buff_end - last_successful_lb;
            assert(residual_bytes < IO_SIZE);

            if (residual_bytes > 0 && last_successful_lb != buff) {
                std::memmove(buff, last_successful_lb, residual_bytes);
            }
        }
    }
}