#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <sched.h>
#include <pthread.h>
#include <condition_variable>
#include <boost/context/continuation.hpp>

enum class TaskState { Ready, Running, Finished };

struct ThreadTask {
  ThreadTask();

  void init(std::function<void(void *)> func);
  TaskState start(void *arg);
  TaskState check();
  void close();
  void set_affinity(uint32_t cpu);

  void *arg_;
  std::function<void(void *)> func_;
  std::atomic<TaskState> state_;
  std::thread *t_;
  std::atomic<bool> stop_;

private:
  std::mutex mtx_;
  std::condition_variable cv_;
};

struct CoroTask {

  static CoroTask *current();
  
  CoroTask();

  void init(std::function<void(void *)> func);
  TaskState start(void *arg);
  TaskState resume();
  TaskState check() { return resume(); }
  bool try_resume(TaskState *state);

  // Called inside the coroutine, resume to the source context
  void yield();

  void *arg_;
  std::function<void(void *)> func_;
  boost::context::continuation source_;
  boost::context::continuation sink_;
  TaskState state_;
  std::mutex ctx_lock_;

  static thread_local CoroTask *current_coro_task_;
};