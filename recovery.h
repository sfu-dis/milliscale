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
#include <optional>
#include <regex>
#include <filesystem>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/GetObjectRequest.h>

#include "tuple.h"
#include "dbcore/dlog-tx.h"


namespace fs = std::filesystem;

struct mfile {
  uint64_t id;
  int fd; // for ssd
  std::string filename;
  std::string bucket_name;
  int64_t size;

  uint32_t log_id;
  uint32_t seg_num;
};

int32_t read_page_from_file(const mfile& f, size_t page_size, off_t offset, void* pointer) {
  if (!ermia::config::enable_s3) {
    ssize_t bytes_read = pread(f.fd, pointer, page_size, offset);

    if (bytes_read == -1) {
        std::cerr << "Read error: " << std::strerror(errno) 
                  << " (Offset: " << offset << ")" << std::endl;
        return -1;
    } 
    return bytes_read;
  } else {
    Aws::S3::S3ClientConfiguration config;
    Aws::S3::S3Client s3_client(config);

    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(f.bucket_name); // TODO
    request.SetKey(f.filename);

    long long end_offset = offset + page_size - 1;
    
    std::stringstream range_stream;
    range_stream << "bytes=" << offset << "-" << end_offset;
    request.SetRange(range_stream.str().c_str());
    auto outcome = s3_client.GetObject(request);
    uint64_t bytes_read = 0;
    if (outcome.IsSuccess()) {
      const auto& retrieved_object = outcome.GetResultWithOwnership();
      auto& result_stream = retrieved_object.GetBody();

      result_stream.read((char*)pointer, page_size);
      bytes_read = result_stream.gcount();
    }
    return bytes_read;
  }
}

std::optional<std::pair<uint64_t, uint64_t>> parseTLogFormat(const std::string& str) {
  std::regex pattern("^tlog-([0-9a-fA-F]{8})-([0-9a-fA-F]{8})$");
  std::smatch matches;

  if (std::regex_match(str, matches, pattern)) {
    uint64_t val1 = std::stoull(matches[1].str(), nullptr, 16);
    uint64_t val2 = std::stoull(matches[2].str(), nullptr, 16);
    return std::pair<uint64_t, uint64_t>{val1, val2};
  }

  return std::nullopt;
}

void* parse_log_block_records(const void* block_ptr, uint64_t offset, std::function<void(ermia::dlog::log_record*, uint64_t)> callback) {
    const auto* lb = static_cast<const ermia::dlog::log_block*>(block_ptr);

    uint32_t current_offset = 0;
    int record_count = 0;
    while (current_offset + sizeof(ermia::dlog::log_record) <= lb->payload_size) {
        
        auto* rec = (ermia::dlog::log_record*)(lb->payload + current_offset);
        // const auto* tuple = reinterpret_cast<const ermia::dbtuple*>(rec->data);
        callback(rec, offset + sizeof(ermia::dlog::log_record) + current_offset);

        // std::cout << "  Record #" << ++record_count << ":" << std::endl;
        // std::cout << "    Type: " << static_cast<int>(rec->type) 
        //           << " (Size: " << rec->size << ", FID: " << rec->fid 
        //           << ", OID: " << rec->oid << ", CSN: " << rec->csn << ")" << std::endl;

        // // Check if record size exceed payload size
        // assert (current_offset + rec->size <= lb->payload_size);

        // // Read tuple
        // if (current_offset + sizeof(log_record) + sizeof(dbtuple) <= lb->payload_size) {
        //     const auto* tuple = reinterpret_cast<const dbtuple*>(rec->data);
        //     // std::cout << "    Tuple Size: " << tuple->size << std::endl;
        //     // std::cout << "    current offset: " << current_offset << std::endl;
        // }

        uint32_t step = ermia::align_up(rec->size + sizeof(ermia::dlog::log_record));
        current_offset += step;

        if (current_offset >= lb->payload_size) {
            break;
        }
    }
    
    return (void*) (lb->payload + lb->capacity);
}

void parse_page(char* buff, uint64_t offset,
  std::function<void(ermia::dlog::log_block*)> block_callback=[](ermia::dlog::log_block*){}, 
  std::function<void(ermia::dlog::log_record*, uint64_t)> rec_callback=[](ermia::dlog::log_record*, uint64_t){}) {

  uint64_t block_num = *(uint64_t*) buff;
  char* next_lb = buff + sizeof(uint64_t);

  while (block_num) {
    block_callback((ermia::dlog::log_block*) next_lb);
    next_lb = static_cast<char*>(parse_log_block_records(next_lb, offset + (uint64_t)(next_lb - buff), rec_callback));
    block_num --;
  }
}

void parse_log(const mfile& file, char* buff, uint64_t page_size,
  std::function<void(ermia::dlog::log_block*)> block_callback=[](ermia::dlog::log_block*){}, 
  std::function<void(ermia::dlog::log_record*, uint64_t)> rec_callback=[](ermia::dlog::log_record*, uint64_t){}) {
  
  uint64_t offset = 0;
  while (offset < file.size){
    uint64_t bytes = read_page_from_file(file, page_size, offset, buff);
    parse_page(buff, offset, block_callback, rec_callback);
    offset += bytes;
  }
}


void parse_filenames(std::vector<mfile>& file_list, 
    std::map<uint64_t, std::vector<std::pair<uint64_t, uint64_t>>>& out_map) {
  for (auto& f: file_list) {
    auto result = parseTLogFormat(f.filename);
    if (result) {
      f.log_id = result->first;
      f.seg_num = result->second;
      out_map[result->first].push_back({result->second, f.id});
    }
  }
  for (auto& kv: out_map) {
    std::sort(kv.second.begin(), kv.second.end());
  }
}

void getFiles(std::string& dir, std::string& bucket, std::vector<mfile>& files) {
  if (!ermia::config::enable_s3){
    fs::path dir_path = dir; 
    assert(fs::exists(dir_path) && fs::is_directory(dir_path));
    for (const auto& entry : fs::directory_iterator(dir_path)) {
      if (fs::is_regular_file(entry.status())) {
        int64_t size = entry.file_size();
        int fd = open(entry.path().c_str(), O_RDONLY);
        files.push_back({files.size(), fd, entry.path().filename(), "", size});
      }
    }
  } else {
    Aws::S3::S3ClientConfiguration config;
    Aws::S3::S3Client s3_client(config);
    Aws::S3::Model::ListObjectsV2Request request;
    request.SetBucket(bucket);
    auto outcome = s3_client.ListObjectsV2(request);

    if (outcome.IsSuccess()) {
      const auto& result = outcome.GetResult();
      const auto& objects = result.GetContents();
      for (const auto& object : objects) {
        std::string key = object.GetKey();
        files.push_back({files.size(), -1, key, bucket, object.GetSize()});
      }
    }
  }

}