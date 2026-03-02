#pragma once

#include "dlog-defs.h"
#include "sm-oid.h"

// Transaction-facing logging infrastructure that determines anything related to
// manupulating the bytes recorded by dlog, such as log record format

namespace ermia {

namespace dlog {

struct log_record {
  enum logrec_type {
    INSERT,
    UPDATE,
    UPDATE_DELTA,
  };

  logrec_type type;

  FID fid;
  OID oid;

  uint64_t csn;

  char data[0];
};

static uint32_t populate_log_record(log_record::logrec_type type,
                                    log_block *block,
                                    FID fid, OID oid,
                                    const char *after_image,
                                    const uint32_t size,
                                    const uint32_t delta_offset = 0) {
  LOG_IF(FATAL, type != log_record::logrec_type::INSERT &&
                type != log_record::logrec_type::UPDATE &&
                type != log_record::logrec_type::UPDATE_DELTA)
                << "Wrong log record type";

  LOG_IF(FATAL, block->payload_size + size > block->capacity) << "No enough space in log block";
  uint32_t off = block->payload_size;

  // Initialize the logrecord header
  log_record *logrec = (log_record *)(&block->payload[off]);

  // Copy contents
  logrec->type = type;
  logrec->fid = fid;
  logrec->oid = oid;
  logrec->csn = block->csn;

  char *p = &logrec->data[0];
  if (type == log_record::UPDATE_DELTA) {
    *(uint32_t *)p = delta_offset;
    *(uint32_t *)(p + sizeof(delta_offset)) = size;
    p += sizeof(uint32_t) * 2; 
  }
  memcpy(p, after_image, size);

  // Account for the occupied space for delta_offset and size
  if (type == log_record::UPDATE_DELTA) {
    block->payload_size += align_up(size + sizeof(uint32_t) * 2 + sizeof(log_record));
  } else {
    block->payload_size += align_up(size + sizeof(log_record));
  }
  return off;
}

inline static uint32_t log_insert(log_block *block, FID fid, OID oid, const char *image, const uint32_t size) {
  return populate_log_record(log_record::INSERT, block, fid, oid, image, size);
}

inline static uint32_t log_update(log_block *block, FID fid, OID oid, const char *image, const uint32_t size, bool is_delta = false, uint32_t delta_offset = 0) {
  return populate_log_record(is_delta ? log_record::UPDATE_DELTA : log_record::UPDATE, 
                             block, fid, oid, image, size, delta_offset);
}


}

}  // namespace ermia
