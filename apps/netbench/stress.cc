extern "C" {
#include <base/log.h>
}

#include "runtime.h"
#include "sync.h"
#include "synthetic_worker.h"
#include "thread.h"
#include "timer.h"
#include <sys/shm.h>
#include <unistd.h>
#include <errno.h>
#include <chrono>
#include <iostream>
#include <iomanip>

barrier_t barrier;

bool synth_barrier_wait() { return barrier_wait(&barrier); }

namespace {

int SHM_KEY;
int threads;
uint64_t n;
std::string worker_spec;

void MainHandler(void *arg) {
  uint64_t* cnt;
  log_info("SHM_KEY: %d, threads: %d\n", SHM_KEY, threads);
  int shm_id = shmget((key_t) SHM_KEY, threads*sizeof(uint64_t), 0);
  if (shm_id == -1) {
    log_err("failed to get shm_id from key: %d in stress.cc", SHM_KEY);
    printf("Something went wrong with shmget()! %s\n", strerror(errno));
    printf("actual errno: %d\n", errno);
    exit(1);
  }
  void* shm_ptr = shmat(shm_id, 0, 0);
  if (shm_ptr == (void*) -1) {
    log_err("failed to get shm_id in stress.cc");
    exit(1);
  }
  cnt = (uint64_t*) shm_ptr;
  shmctl(shm_id, IPC_RMID, NULL); // mark for removal early, because we will kill this process

  rt::WaitGroup wg(1);

  barrier_init(&barrier, threads);

  // rt::Spawn([&]() {
  //   uint64_t last_total = 0;
  //   double track_time = 0;
  //   std::chrono::duration<double> one_second_interval(1);
  //   auto last = std::chrono::steady_clock::now();
    
  //   while (1) {
  //     // rt::Sleep(rt::kSeconds);
  //     auto now = std::chrono::steady_clock::now();
  //     if (now - last < one_second_interval) {
  //       continue;
  //     }
  //     uint64_t total = 0;
  //     double duration =
  //         std::chrono::duration_cast<std::chrono::duration<double>>(now - last)
  //             .count();
  //     for (int i = 0; i < threads; i++) total += cnt[i];
  //     preempt_disable();
  //     log_info("%f,%f", track_time, static_cast<double>(total - last_total) / duration);
  //     fflush(stdout);
  //     // std::cout << track_time << "," << static_cast<double>(total - last_total) / duration) << std::endl;
  //     preempt_enable();
  //     track_time += duration;
  //     last_total = total;
  //     last = now;
  //   }
  // });

  for (int i = 0; i < threads; ++i) {
    rt::Spawn([&, i]() {
      auto *w = SyntheticWorkerFactory(worker_spec);
      if (w == nullptr) {
        std::cerr << "Failed to create worker." << std::endl;
        exit(1);
      }

      while (true) {
        w->Work(n);
        cnt[i]++;
        rt::Yield();
      }
    });
  }

  

  // never returns
  wg.Wait();
}

}  // anonymous namespace

int main(int argc, char *argv[]) {
  int ret;

  if (argc != 6) {
    std::cerr << "usage: [config_file] [SHM_KEY] [#threads] [#n] [worker_spec]"
              << std::endl;
    return -EINVAL;
  }

  SHM_KEY = std::stoi(argv[2], nullptr, 0);
  threads = std::stoi(argv[3], nullptr, 0);
  n = std::stoul(argv[4], nullptr, 0);
  worker_spec = std::string(argv[5]);

  std::cout << std::setprecision(8) << std::fixed;

  ret = runtime_init(argv[1], MainHandler, NULL);
  if (ret) {
    printf("failed to start runtime\n");
    return ret;
  }

  return 0;
}
