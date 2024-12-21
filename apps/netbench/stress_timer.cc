/*
    couple things to do

    1. copy the basic stuff, like how to init a runtime and such from stress.cc
    2. figure out how to make this compile an executable like the other things in netbench do
    3. use shmget and such to make a shared memory segment for the counts
    4. probably pass in as an arg or something: a shm_key to use in both apps
    5. generate a config for this timer in the run script (and start it appropriately)
        probably going to just ensure it has 1 spinning core and that's it.
    6. Honestly, maybe I can get swaptions running without the garbage collection?
        understanding it might be less daunting for me now than it used to be

*/

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

#include <chrono>
#include <iostream>
#include <iomanip>

namespace {

int threads;
// uint64_t n;
// std::string worker_spec;

int SHM_KEY;

void MainHandler(void *arg) {
//   uint64_t cnt[threads] = {}; // do in shm
  uint64_t* cnt;
  log_info("SHM_KEY: %d, threads: %d\n", SHM_KEY, threads);
  fflush(stdout);
  int shm_id = shmget((key_t) SHM_KEY, threads*sizeof(uint64_t), IPC_CREAT | 0777);
  log_info("shm id: %d\n", shm_id);
  if (shm_id == -1) {
    log_err("failed to get shm_id in stress_timer");
    exit(1);
  }
  void* shm_ptr = shmat(shm_id, 0, 0);
  if (shm_ptr == (void*) -1) {
    log_err("failed to get shm_id in stress_timer");
    exit(1);
  }
  cnt = (uint64_t*) shm_ptr;
  // DO NOT call the rmid here, since it means no other process can shmget successfully

  uint64_t last_total = 0;
  double track_time = 0;
  std::chrono::duration<double> one_second_interval(1);
  auto last = std::chrono::steady_clock::now();
  
  while (1) {
    // rt::Sleep(rt::kSeconds);
    auto now = std::chrono::steady_clock::now();
    if (now - last < one_second_interval) {
      continue;
    }
    uint64_t total = 0;
    double duration =
        std::chrono::duration_cast<std::chrono::duration<double>>(now - last)
            .count();
    for (int i = 0; i < threads; i++) total += cnt[i];
    preempt_disable();
    log_info("%f,%f", track_time, static_cast<double>(total - last_total) / duration);
    fflush(stdout);
    // std::cout << track_time << "," << static_cast<double>(total - last_total) / duration) << std::endl;
    preempt_enable();
    track_time += duration;
    last_total = total;
    last = now;
  }
}

}  // anonymous namespace

int main(int argc, char *argv[]) {
  int ret;

  if (argc != 4) {
    std::cerr << "usage: [config_file] [SHM_KEY] [threads]"
              << std::endl;
    return -EINVAL;
  }

  std::cout << std::setprecision(8) << std::fixed;

  SHM_KEY = std::stoi(argv[2], nullptr, 0);
  threads = std::stoi(argv[3], nullptr, 0);
  log_info("before runtime_init\n");
  ret = runtime_init(argv[1], MainHandler, NULL);
  log_info("after runtime_init, ret=%d\n", ret);
  if (ret) {
    printf("failed to start runtime\n");
    return ret;
  }

  return 0;
}
