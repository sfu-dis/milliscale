#pragma once

#include "dlog-defs.h"
#include "sm-oid.h"
#include <filesystem>

// Transaction-facing logging infrastructure that determines anything related to
// manupulating the bytes recorded by dlog, such as log record format

namespace ermia {

namespace dlog {

struct log_record {
  enum logrec_type: uint8_t {
    INSERT,
    UPDATE,
    UPDATE_DELTA,
    INSERT_KEY,
  };

  logrec_type type : 8;
  uint32_t size : 24;
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
                type != log_record::logrec_type::INSERT_KEY &&
                type != log_record::logrec_type::UPDATE &&
                type != log_record::logrec_type::UPDATE_DELTA)
                << "Wrong log record type";

  LOG_IF(FATAL, block->payload_size + size > block->capacity) << "No enough space in log block";
  uint32_t off = block->payload_size;

  // Initialize the logrecord header
  log_record *logrec = (log_record *)(&block->payload[off]);

  // Copy contents
  logrec->type = type;
  logrec->size = size;
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

inline static uint32_t log_insert_key(log_block *block, FID fid, OID oid, const char *image, const uint32_t size) {
  return populate_log_record(log_record::INSERT_KEY, block, fid, oid, image, size);
}

inline static uint32_t log_insert(log_block *block, FID fid, OID oid, const char *image, const uint32_t size) {
  return populate_log_record(log_record::INSERT, block, fid, oid, image, size);
}

inline static uint32_t log_update(log_block *block, FID fid, OID oid, const char *image, const uint32_t size, bool is_delta = false, uint32_t delta_offset = 0) {
  return populate_log_record(is_delta ? log_record::UPDATE_DELTA : log_record::UPDATE, 
                             block, fid, oid, image, size, delta_offset);
}


struct ddl_log {
  enum log_type:uint8_t {
    TABLE_LOG,
    PRIMARY_INDEX_LOG,
    SECONDARY_INDEX_LOG,
  };
  log_type t;
  FID first_fid;
  FID second_fid;
  uint32_t size;
  char name[0];
};

// NO Lock protect
static int GetDDLFD() {
static int fd = -1;
  if (fd == -1) {
    std::filesystem::path dir = ermia::config::log_dir;
    std::filesystem::path file = dir / "ddl_log";
    fd = open(file.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_FSYNC);
  }
  return fd;
}

// No CC, because currently we creaete all index in one thread
inline static uint32_t flush_ddl_log(ddl_log::log_type t, FID first_fid, FID second_fid, uint32_t size, const char* name) {
  auto lb = (ddl_log*) malloc(sizeof(ddl_log) + size);
  lb->first_fid = first_fid;
  lb->second_fid = second_fid;
  lb->size = size;
  memcpy(lb->name, name, size);
  
  // TODO: S3 support
  int fd = GetDDLFD();
  return pwrite(fd, lb, sizeof(ddl_log) + size, 0);
}

inline static uint32_t log_table(FID tuple_fid, FID key_fid, uint32_t size, const char* name) {
  return flush_ddl_log(ddl_log::log_type::TABLE_LOG, tuple_fid, key_fid, size, name);
}

inline static uint32_t log_primary_index(FID table_fid, FID index_fid, uint32_t size, const char* name) {
  return flush_ddl_log(ddl_log::log_type::PRIMARY_INDEX_LOG, table_fid, index_fid, size, name);
}

inline static uint32_t log_secondary_index(FID table_fid, FID index_fid, uint32_t size, const char* name) {
  return flush_ddl_log(ddl_log::log_type::SECONDARY_INDEX_LOG, table_fid, index_fid, size, name);
}

}

}  // namespace ermia
