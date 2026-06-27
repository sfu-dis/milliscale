#pragma once

#include "io-manager.h"
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <glog/logging.h>

#include "dbcore/sm-config.h"

namespace ermia {

class CheckpointManager;

extern CheckpointManager* chkptmgr;

class CheckpointManager {
public:
  CheckpointManager(size_t buffer_size, IOEngine* io_engine)
      : buffer_size_(buffer_size), io_engine_(io_engine), counter_(0),
        current_buffer_idx_(0), buffer_offset_(0), current_fd_(-1),
        file_offset_(0), last_io_id_(-1), has_last_io_(false) {

    buffers_[0].resize(buffer_size);
    buffers_[1].resize(buffer_size);
    if (config::enable_s3) {
      counter_ = next_s3_checkpoint_id();
    }
  }

  ~CheckpointManager() {
    if (buffer_offset_ > 0) {
      flush_current_buffer();
    }
    wait_last_io();
    close_current_file();
  }

  static void create(uint64_t buff_size, IOEngine* engine) {
    chkptmgr = new CheckpointManager(buff_size, engine);
  }

  void write_buffer(const void *data, uint32_t size) {
    const uint8_t *src = static_cast<const uint8_t *>(data);
    uint32_t remaining_size = size;

    while (remaining_size > 0) {
      size_t available_space = buffer_size_ - buffer_offset_;

      if (available_space == 0) {
        flush_current_buffer();
        available_space = buffer_size_;
      }

      size_t to_copy =
          std::min(static_cast<size_t>(remaining_size), available_space);
      std::memcpy(buffers_[current_buffer_idx_].data() + buffer_offset_, src,
                  to_copy);

      buffer_offset_ += to_copy;
      src += to_copy;
      remaining_size -= to_copy;
    }
  }

  void sync_buffer() {
    flush_current_buffer();
    wait_last_io();
    close_current_file();
  }

private:
  static bool parse_checkpoint_key(const std::string &key, uint64_t &id) {
    std::filesystem::path path(key);
    if (path.extension() != ".chkpt") {
      return false;
    }
    try {
      id = std::stoull(path.stem().string());
    } catch (...) {
      return false;
    }
    return true;
  }

  uint64_t next_s3_checkpoint_id() {
    if (config::s3_bucket_names.empty()) {
      LOG(FATAL) << "S3 checkpoint requested without an S3 bucket";
    }

    Aws::S3::Model::ListObjectsV2Request request;
    request.SetBucket(config::s3_bucket_names[0]);
    auto outcome = s3_client_.ListObjectsV2(request);
    if (!outcome.IsSuccess()) {
      LOG(FATAL) << "Failed to list S3 checkpoints: "
                 << outcome.GetError().GetMessage();
    }

    uint64_t best_id = 0;
    bool found = false;
    for (const auto &object : outcome.GetResult().GetContents()) {
      uint64_t id = 0;
      if (!parse_checkpoint_key(object.GetKey(), id)) {
        continue;
      }
      if (!found || id > best_id) {
        best_id = id;
        found = true;
      }
    }
    return found ? best_id + 1 : 0;
  }

  void flush_current_buffer() {
    wait_last_io();

    if (buffer_offset_ > 0) {
      if (config::enable_s3) {
        flush_current_buffer_to_s3();
        current_buffer_idx_ = 1 - current_buffer_idx_;
        buffer_offset_ = 0;
        return;
      }

      if (current_fd_ == -1) {
        std::filesystem::path filename =
            std::filesystem::path(config::log_dir) /
            (std::to_string(counter_) + ".chkpt");
        current_fd_ =
            ::open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        file_offset_ = 0;
        if (current_fd_ < 0) {
          std::cerr << "Failed to open file: " << filename << std::endl;
          return;
        }
      }

      IORequest req;
      req.fd = current_fd_;
      req.buf = buffers_[current_buffer_idx_].data();
      req.size = buffer_offset_;
      req.offset = file_offset_;
      req.user_data = nullptr;
      req.callback = [](int res) {
        if (res < 0) {
          std::cerr << "Async IO write failed with error code: " << res
                    << std::endl;
        }
      };

      last_io_id_ = io_engine_->write(req);
      has_last_io_ = true;

      file_offset_ += buffer_offset_;
    }

    // switch buffer idx
    current_buffer_idx_ = 1 - current_buffer_idx_;
    buffer_offset_ = 0;
  }

  void flush_current_buffer_to_s3() {
    if (config::s3_bucket_names.empty()) {
      LOG(FATAL) << "S3 checkpoint requested without an S3 bucket";
    }
    if (current_key_.empty()) {
      current_key_ = std::to_string(counter_) + ".chkpt";
      file_offset_ = 0;
    }

    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(config::s3_bucket_names[0]);
    request.SetKey(current_key_);

    auto buffer = const_cast<unsigned char *>(
        reinterpret_cast<const unsigned char *>(
            buffers_[current_buffer_idx_].data()));
    auto psb = Aws::MakeShared<Aws::Utils::Stream::PreallocatedStreamBuf>(
        "chkpt-upload", buffer, buffer_offset_);
    auto buffer_stream =
        Aws::MakeShared<Aws::IOStream>("chkpt-upload", psb.get());

    request.SetBody(buffer_stream);
    request.SetContentLength(buffer_offset_);
    if (file_offset_ > 0) {
      request.SetWriteOffsetBytes(file_offset_);
    }

    auto outcome = s3_client_.PutObject(request);
    if (!outcome.IsSuccess()) {
      LOG(FATAL) << "Failed to write S3 checkpoint " << current_key_ << ": "
                 << outcome.GetError().GetMessage();
    }
    file_offset_ += buffer_offset_;
  }

  void wait_last_io() {
    if (!has_last_io_)
      return;

    while (!io_engine_->check(last_io_id_)) {
      // std::this_thread::yield();
    }
    has_last_io_ = false;
  }

  void close_current_file() {
    if (config::enable_s3) {
      if (!current_key_.empty()) {
        current_key_.clear();
        file_offset_ = 0;
        counter_++;
      }
      return;
    }

    if (current_fd_ != -1) {
      ::close(current_fd_);
      current_fd_ = -1;
      file_offset_ = 0;
      counter_++;
    }
  }

private:
  size_t buffer_size_;
  IOEngine* io_engine_;
  uint64_t counter_;

  std::vector<uint8_t> buffers_[2];
  int current_buffer_idx_;
  size_t buffer_offset_;

  int current_fd_;
  uint64_t file_offset_;
  std::string current_key_;

  // Tracking async IO
  int32_t last_io_id_;
  bool has_last_io_;

  Aws::S3::S3Client s3_client_;
};

}
