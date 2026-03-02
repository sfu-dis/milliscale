#include <stdexcept>
#include <thread>
#include "task.h"

ThreadTask::ThreadTask(): arg_(nullptr), state_(TaskState::Ready), stop_(false) {}

void ThreadTask::init(std::function<void(void *)> func) {
  func_ = func;
  t_ = new std::thread([this]{
    std::unique_lock<std::mutex> lock(mtx_);
    while (!stop_) {
      // wait blocking until start()
      // while (state_ != TaskState::Running) {
      //   std::this_thread::yield();
      //   continue;
      // }
      cv_.wait(lock, [this]{ return state_ == TaskState::Running || stop_; });
      if (stop_) break;
      lock.unlock();
      func_(arg_);
      arg_ = nullptr;
      lock.lock();
      state_ = TaskState::Finished;
    }
  });
}

TaskState ThreadTask::start(void *arg) {
  std::unique_lock<std::mutex> lock(mtx_);
  arg_ = arg;
  if (state_ == TaskState::Running) {
    throw std::runtime_error(
        "start() can only be called when state is Ready or Finished");
  }
  // run the actural function
  state_.store(TaskState::Running);
  cv_.notify_one();
  return state_;
}

TaskState ThreadTask::check() {
  return state_;
}

void ThreadTask::close() {
  {
    std::lock_guard<std::mutex> lock(mtx_);
    stop_ = true;
    cv_.notify_one();
  }
  if (t_ && t_->joinable()) t_->join();
  delete t_;
  t_ = nullptr;
}

void ThreadTask::set_affinity(uint32_t cpu) {
  pthread_t handle = t_->native_handle();
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu, &cpuset);
  int rc = pthread_setaffinity_np(handle, sizeof(cpu_set_t), &cpuset);
}

thread_local CoroTask *CoroTask::current_coro_task_ = nullptr;

CoroTask *CoroTask::current() { return current_coro_task_; }

CoroTask::CoroTask(): arg_(nullptr), state_(TaskState::Ready) {}

void CoroTask::init(std::function<void(void *)> func) {
  func_ = std::move(func);
  source_ = boost::context::callcc([this](boost::context::continuation &&c) {
    sink_ = std::move(c);
    sink_ = sink_.resume();
    while (true) {
      state_ = TaskState::Running;
      func_(arg_);
      state_ = TaskState::Finished;
      arg_ = nullptr;
      sink_ = sink_.resume();
    }
    return std::move(sink_);
  });
}

TaskState CoroTask::start(void *arg) {
  std::lock_guard<std::mutex> lock(ctx_lock_);
  arg_ = arg;
  if (state_ == TaskState::Running) {
    throw std::runtime_error(
        "start() can only be called when state is Ready or Finished");
  }
  current_coro_task_ = this;
  source_ = source_.resume();
  return state_;
}

TaskState CoroTask::resume() {
  std::lock_guard<std::mutex> lock(ctx_lock_);
  if (state_ == TaskState::Finished) {
    return state_;
  }
  current_coro_task_ = this;
  source_ = source_.resume();
  return state_;
}

bool CoroTask::try_resume(TaskState *state) {
  bool success = ctx_lock_.try_lock();
  if (success) {
    if (state_ == TaskState::Finished) {
      *state = state_;
    } else {
      current_coro_task_ = this;
      source_ = source_.resume();
      *state = state_;
    }
    ctx_lock_.unlock();
  }
  return success;
}

void CoroTask::yield() {
  if (current() != this) {
    throw std::runtime_error(
        "yield must be called from inside coroutine");
  }
  current_coro_task_ = nullptr;
  sink_ = sink_.resume();
}
