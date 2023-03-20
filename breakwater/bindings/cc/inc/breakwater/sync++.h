#pragma once

extern "C" {
#include <base/lock.h>
#include <base/stddef.h>
#include <runtime/sync.h>
#include <runtime/thread.h>
#include <breakwater/sync.h>
}

namespace rpc {

// Pthread-like mutex support.
class Mutex {
 friend class CondVar;

 public:
  Mutex() { mutex_init(&mu_); }
  ~Mutex() { assert(!mutex_held(&mu_)); }

  // Locks the mutex.
  void Lock() { mutex_lock(&mu_); }

  bool LockIfUncongested() { return mutex_lock_if_uncongested(&mu_); }

  bool IsCongested() { return mutex_lock_is_congested(&mu_); }

  // Unlocks the mutex.
  void Unlock() { mutex_unlock(&mu_); }

  uint64_t QueueUS() { return mutex_queue_us(&mu_); }

  // Locks the mutex only if it is currently unlocked. Returns true if
  // successful.
  bool TryLock() { return mutex_try_lock(&mu_); }

  // Returns true if the mutex is currently held.
  bool IsHeld() { return mutex_held(&mu_); }

 private:
  mutex_t mu_;

  Mutex(const Mutex&) = delete;
  Mutex& operator=(const Mutex&) = delete;
};

// Pthread-like condition variable support.
class CondVar {
 public:
  CondVar() { condvar_init(&cv_); };
  ~CondVar() {}

  // Block until the condition variable is signaled. Recheck the condition
  // after wakeup, as no guarantees are made about preventing spurious wakeups.
  void Wait(Mutex *mu) { condvar_wait(&cv_, &mu->mu_); }

  bool WaitIfUncongested(Mutex *mu) {
    return condvar_wait_if_uncongested(&cv_, &mu->mu_);
  }

  void TimedWait(Mutex *mu, uint64_t timeout_us) {
    condvar_timed_wait(&cv_, &mu->mu_, timeout_us);
  }

  // Wake up one waiter.
  void Signal() { condvar_signal(&cv_); }

  // Wake up all waiters.
  void SignalAll() { condvar_broadcast(&cv_); }

  uint64_t QueueUS() { return condvar_queue_us(&cv_); }

 private:
  condvar_t cv_;

  CondVar(const CondVar&) = delete;
  CondVar& operator=(const CondVar&) = delete;
};
} // namespace rpc
