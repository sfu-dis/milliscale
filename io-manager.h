#pragma once

#include <functional>
#include <liburing.h>
#include <stdexcept>
#include <thread>

struct IORequest {
  int fd;
  void *buf;
  uint64_t size;
  uint64_t offset;
  void *user_data;
  std::function<void(int)> callback;
};

class IOEngine {
public:
  virtual ~IOEngine() = default;
  virtual int32_t read(const IORequest &req) = 0;
  virtual int32_t write(const IORequest &req) = 0;
  virtual bool check(int32_t id) = 0;
};

class SyncIOEngine : public IOEngine {
public:
  // sync interface will return success directly as user data
  int32_t read(const IORequest &req) override {
    ssize_t res = pread(req.fd, req.buf, req.size, req.offset);
    if (req.callback)
      req.callback(static_cast<int>(res));
    return res == req.size;
  }

  int32_t write(const IORequest &req) override {
    ssize_t res = pwrite(req.fd, req.buf, req.size, req.offset);
    if (req.callback)
      req.callback(static_cast<int>(res));
    return res == req.size;
  }

  bool check(int32_t data) { return data; }
};

class ThreadLocalUringEngine : public IOEngine {

private:
  static constexpr int MAX_IO = 1024;
  static constexpr int INVALID_IDX = -1;

  struct Slot {
    IORequest req;
    int next;
    bool is_done = false;
    int res = 0;
  };

  struct ThreadContext {
    io_uring ring;
    std::array<Slot, MAX_IO> pool;
    int free_head = 0;
    bool initialized = false;

    ThreadContext() = default;

    void ensure_init() {
      if (initialized)
        return;

      io_uring_params params{};
      // uring 2.2
      // params.flags = IORING_SETUP_SINGLE_ISSUER;
      io_uring_queue_init_params(MAX_IO, &ring, &params);

      for (int i = 0; i < MAX_IO - 1; ++i) {
        pool[i].next = i + 1;
      }
      pool[MAX_IO - 1].next = INVALID_IDX;
      free_head = 0;
      initialized = true;
    }

    ~ThreadContext() {
      if (initialized) {
        io_uring_queue_exit(&ring);
      }
    }
  };

  static ThreadContext &get_ctx() {
    static thread_local ThreadContext ctx;
    ctx.ensure_init();
    return ctx;
  }

public:
  ThreadLocalUringEngine() = default;

  int32_t read(const IORequest &req) override {
    auto &ctx = get_ctx();
    int32_t idx = alloc_idx(ctx);
    if (idx == INVALID_IDX)
      return INVALID_IDX;

    ctx.pool[idx].req = req;
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ctx.ring);
    io_uring_prep_read(sqe, req.fd, req.buf, req.size, req.offset);
    io_uring_sqe_set_data(sqe, reinterpret_cast<void *>((uintptr_t)idx));

    io_uring_submit(&ctx.ring);
    return idx;
  }

  int32_t write(const IORequest &req) override {
    auto &ctx = get_ctx();
    int idx = alloc_idx(ctx);
    if (idx == INVALID_IDX)
      return INVALID_IDX;

    ctx.pool[idx].req = req;
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ctx.ring);
    io_uring_prep_write(sqe, req.fd, req.buf, req.size, req.offset);
    io_uring_sqe_set_data(sqe, reinterpret_cast<void *>((uintptr_t)idx));

    io_uring_submit(&ctx.ring);
    return idx;
  }

  bool check(int32_t target_idx) override {
    auto &ctx = get_ctx();
    struct io_uring_cqe *cqe;
    unsigned head;
    int count = 0;

    // Put everything into pool
    io_uring_for_each_cqe(&ctx.ring, head, cqe) {
      int idx = static_cast<int>(
          reinterpret_cast<uintptr_t>(io_uring_cqe_get_data(cqe)));

      ctx.pool[idx].res = cqe->res;
      ctx.pool[idx].is_done = true;

      count++;
    }

    if (count > 0) {
      io_uring_cq_advance(&ctx.ring, count);
    }

    if (ctx.pool[target_idx].is_done) {
      bool success = ctx.pool[target_idx].res == ctx.pool[target_idx].req.size;
      if (ctx.pool[target_idx].req.callback) {
        ctx.pool[target_idx].req.callback(ctx.pool[target_idx].res);
      }

      ctx.pool[target_idx].is_done = false;
      free_idx(ctx, target_idx);
      return success;
    }

    return false;
  }

private:
  inline int alloc_idx(ThreadContext &ctx) {
    int idx = ctx.free_head;
    if (idx == INVALID_IDX) [[unlikely]]
      return INVALID_IDX;
    ctx.free_head = ctx.pool[idx].next;
    return idx;
  }

  inline void free_idx(ThreadContext &ctx, int idx) {
    ctx.pool[idx].next = ctx.free_head;
    ctx.free_head = idx;
  }
};
