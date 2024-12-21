extern "C" {
#include <base/log.h>
#include <base/time.h>
#include <net/ip.h>
#include <unistd.h>
#include <breakwater/breakwater.h>
#include <breakwater/seda.h>
#include <breakwater/dagor.h>
#include <breakwater/nocontrol.h>
#include <breakwater/sync.h>
}

#include "cc/net.h"
#include "cc/runtime.h"
#include "cc/sync.h"
#include "cc/thread.h"
#include "cc/timer.h"
#include "breakwater/rpc++.h"

#include "synthetic_worker.h"
#include "loadbalancer.h"
#include "fanouter.h"

#include <atomic>
#include <algorithm>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <ctime>
std::time_t timex;

barrier_t barrier;

constexpr uint16_t kBarrierPort = 41;

const struct crpc_ops *crpc_ops;
const struct srpc_ops *srpc_ops;

#define ENABLE_DOWNLOAD_ALL_TASKS			 false

namespace {

using namespace std::chrono;
using sec = duration<double, std::micro>;

// <- ARGUMENTS FOR EXPERIMENT ->
// the number of worker threads to spawn.
int threads;
// the remote UDP address of the server.
int num_servers;
int nconn[16];
netaddr raddr[16];
netaddr master;
// the mean service time in us.
double st;
// service time distribution type
// 1: exponential
// 2: constant
// 3: bimodal
int st_type;
// RPC service level objective (in us)
int slo;

std::ofstream json_out;
std::ofstream csv_out;

int total_agents = 1;
// number of iterations required for 1us on target server
constexpr uint64_t kIterationsPerUS = 69;  // 83
// Total duration of the experiment in us
constexpr uint64_t kWarmUpTime = 4000000;
constexpr uint64_t kExperimentTime = 8000000;
// RTT
constexpr uint64_t kRTT = 10;
constexpr uint64_t kNumDupClient = 32;

std::vector<double> offered_loads;
double offered_load;

// for shorter exp duration, 4 seconds total
std::vector<std::pair<double, uint64_t>> rates = {{400000, 2500000}, {1400000, 500000}, {850000, 500000}, 
                                                  {1400000, 500000}};

// std::vector<std::pair<double, uint64_t>> rates = {{875000, 4000000}};

// std::vector<std::pair<double, uint64_t>> rates = {{400000, 4500000}, {1400000, 500000}, {850000, 1500000}, 
//                                                   {1400000, 500000}, {400000, 1000000}};

/// ERIC
// 0: steady state
// 1: load shift
int experiment_type = 0;
/// END ERIC

static SyntheticWorker *workers[NCPU];

struct payload {
  uint64_t work_iterations;
  uint64_t index;
  uint64_t tsc_end;
  uint32_t cpu;
  uint64_t server_queue;
  uint64_t hash;
};
constexpr int PAYLOAD_ID_OFF = offsetof(payload, index);

rpc::LoadBalancer<payload, PAYLOAD_ID_OFF> *load_balancer[16];
rpc::FanOuter<payload, PAYLOAD_ID_OFF> *fan_outer;

/* server-side stat */
constexpr uint64_t kRPCSStatPort = 8002;
constexpr uint64_t kRPCSStatMagic = 0xDEADBEEF;
struct sstat_raw {
  uint64_t idle;
  uint64_t busy;
  unsigned int num_cores;
  unsigned int max_cores;
  uint64_t cupdate_rx;
  uint64_t ecredit_tx;
  uint64_t credit_tx;
  uint64_t req_rx;
  uint64_t req_dropped;
  uint64_t resp_tx;
};

constexpr uint64_t kShenangoStatPort = 40;
constexpr uint64_t kShenangoStatMagic = 0xDEADBEEF;
struct shstat_raw {
  uint64_t rx_pkts;
  uint64_t tx_pkts;
  uint64_t rx_bytes;
  uint64_t tx_bytes;
  uint64_t drops;
  uint64_t rx_tcp_ooo;
};

struct sstat {
  double cpu_usage;
  double rx_pps;
  double tx_pps;
  double rx_bps;
  double tx_bps;
  double rx_drops_pps;
  double rx_ooo_pps;
  double cupdate_rx_pps;
  double ecredit_tx_pps;
  double credit_tx_cps;
  double req_rx_pps;
  double req_drop_rate;
  double resp_tx_pps;
};

/* client-side stat */
struct cstat_raw {
  double offered_rps;
  double rps;
  double goodput;
  double min_percli_tput;
  double max_percli_tput;
  uint64_t ecredit_rx;
  uint64_t cupdate_tx;
  uint64_t resp_rx;
  uint64_t req_tx;
  uint64_t credit_expired;
  uint64_t req_dropped;
};

struct cstat {
  double offered_rps;
  double rps;
  double goodput;
  double min_percli_tput;
  double max_percli_tput;
  double ecredit_rx_pps;
  double cupdate_tx_pps;
  double resp_rx_pps;
  double req_tx_pps;
  double credit_expired_cps;
  double req_dropped_rps;
};

struct work_unit {
  double start_us, work_us, duration_us;
  int hash;
  bool success;
  bool is_monster;
  uint64_t credit;
  uint64_t tsc;
  uint32_t cpu;
  uint64_t server_queue;
  uint64_t server_time;
  uint64_t timing;
};

class NetBarrier {
 public:
  static constexpr uint64_t npara = 10;
  NetBarrier(int npeers) {
    threads /= total_agents;

    is_leader_ = true;
    std::unique_ptr<rt::TcpQueue> q(
        rt::TcpQueue::Listen({0, kBarrierPort}, 4096));
    aggregator_ = std::move(std::unique_ptr<rt::TcpQueue>(
        rt::TcpQueue::Listen({0, kBarrierPort + 1}, 4096)));
    for (int i = 0; i < npeers; i++) {
      rt::TcpConn *c = q->Accept();
      if (c == nullptr) panic("couldn't accept a connection");
      conns.emplace_back(c);
      BUG_ON(c->WriteFull(&threads, sizeof(threads)) <= 0);
      BUG_ON(c->WriteFull(&st, sizeof(st)) <= 0);
      BUG_ON(c->WriteFull(&total_agents, sizeof(total_agents)) <= 0);
      BUG_ON(c->WriteFull(&st_type, sizeof(st_type)) <= 0);
      BUG_ON(c->WriteFull(&slo, sizeof(slo)) <= 0);
      BUG_ON(c->WriteFull(&offered_load, sizeof(offered_load)) <= 0);
      BUG_ON(c->WriteFull(&num_servers, sizeof(num_servers)) <= 0);
      BUG_ON(c->WriteFull(raddr, sizeof(netaddr) * num_servers) <= 0);
      BUG_ON(c->WriteFull(nconn, sizeof(int) * num_servers) <= 0);
      for (size_t j = 0; j < npara; j++) {
        rt::TcpConn *c = aggregator_->Accept();
        if (c == nullptr) panic("couldn't accept a connection");
        agg_conns_.emplace_back(c);
      }
    }
  }

  NetBarrier(netaddr leader) {
    auto c = rt::TcpConn::Dial({0, 0}, {leader.ip, kBarrierPort});
    if (c == nullptr) panic("barrier");
    conns.emplace_back(c);
    is_leader_ = false;
    BUG_ON(c->ReadFull(&threads, sizeof(threads)) <= 0);
    BUG_ON(c->ReadFull(&st, sizeof(st)) <= 0);
    BUG_ON(c->ReadFull(&total_agents, sizeof(total_agents)) <= 0);
    BUG_ON(c->ReadFull(&st_type, sizeof(st_type)) <= 0);
    BUG_ON(c->ReadFull(&slo, sizeof(slo)) <= 0);
    BUG_ON(c->ReadFull(&offered_load, sizeof(offered_load)) <= 0);
    BUG_ON(c->ReadFull(&num_servers, sizeof(num_servers)) <= 0);
    BUG_ON(c->ReadFull(raddr, sizeof(netaddr) * num_servers) <= 0);
    BUG_ON(c->ReadFull(nconn, sizeof(int) * num_servers) <= 0);
    for (size_t i = 0; i < npara; i++) {
      auto c = rt::TcpConn::Dial({0, 0}, {master.ip, kBarrierPort + 1});
      BUG_ON(c == nullptr);
      agg_conns_.emplace_back(c);
    }
  }

  bool Barrier() {
    char buf[1];
    if (is_leader_) {
      for (auto &c : conns) {
        if (c->ReadFull(buf, 1) != 1) return false;
      }
      for (auto &c : conns) {
        if (c->WriteFull(buf, 1) != 1) return false;
      }
    } else {
      if (conns[0]->WriteFull(buf, 1) != 1) return false;
      if (conns[0]->ReadFull(buf, 1) != 1) return false;
    }
    return true;
  }

  bool StartExperiment() { return Barrier(); }

  bool EndExperiment(std::vector<work_unit> &w, struct cstat_raw *csr) {
    if (is_leader_) {
      for (auto &c : conns) {
        struct cstat_raw rem_csr;
        BUG_ON(c->ReadFull(&rem_csr, sizeof(rem_csr)) <= 0);
        csr->offered_rps += rem_csr.offered_rps;
        csr->rps += rem_csr.rps;
        csr->goodput += rem_csr.goodput;
        csr->min_percli_tput =
            MIN(rem_csr.min_percli_tput, csr->min_percli_tput);
        csr->max_percli_tput =
            MAX(rem_csr.max_percli_tput, csr->max_percli_tput);
        csr->ecredit_rx += rem_csr.ecredit_rx;
        csr->cupdate_tx += rem_csr.cupdate_tx;
        csr->resp_rx += rem_csr.resp_rx;
        csr->req_tx += rem_csr.req_tx;
        csr->credit_expired += rem_csr.credit_expired;
        csr->req_dropped += rem_csr.req_dropped;
      }
    } else {
      BUG_ON(conns[0]->WriteFull(csr, sizeof(*csr)) <= 0);
    }
    GatherSamples(w);
    BUG_ON(!Barrier());
    return is_leader_;
  }

  bool IsLeader() { return is_leader_; }

 private:
  std::vector<std::unique_ptr<rt::TcpConn>> conns;
  std::unique_ptr<rt::TcpQueue> aggregator_;
  std::vector<std::unique_ptr<rt::TcpConn>> agg_conns_;
  bool is_leader_;

  void GatherSamples(std::vector<work_unit> &w) {
    std::vector<rt::Thread> th;
    if (is_leader_) {
      std::unique_ptr<std::vector<work_unit>> samples[agg_conns_.size()];
      for (size_t i = 0; i < agg_conns_.size(); ++i) {
        th.emplace_back(rt::Thread([&, i] {
          size_t nelem;
          BUG_ON(agg_conns_[i]->ReadFull(&nelem, sizeof(nelem)) <= 0);

          if (likely(nelem > 0)) {
            work_unit *wunits = new work_unit[nelem];
            BUG_ON(agg_conns_[i]->ReadFull(wunits, sizeof(work_unit) * nelem) <=
                   0);
            std::vector<work_unit> v(wunits, wunits + nelem);
            delete[] wunits;

            samples[i].reset(new std::vector<work_unit>(std::move(v)));
          } else {
            samples[i].reset(new std::vector<work_unit>());
          }
        }));
      }

      for (auto &t : th) t.Join();
      for (size_t i = 0; i < agg_conns_.size(); ++i) {
        auto &v = *samples[i];
        w.insert(w.end(), v.begin(), v.end());
      }
    } else {
      for (size_t i = 0; i < agg_conns_.size(); ++i) {
        th.emplace_back(rt::Thread([&, i] {
          size_t elems = w.size() / npara;
          work_unit *start = w.data() + elems * i;
          if (i == npara - 1) elems += w.size() % npara;
          BUG_ON(agg_conns_[i]->WriteFull(&elems, sizeof(elems)) <= 0);
          if (likely(elems > 0))
            BUG_ON(agg_conns_[i]->WriteFull(start, sizeof(work_unit) * elems) <=
                   0);
        }));
      }
      for (auto &t : th) t.Join();
    }
  }
};

static NetBarrier *b;

void RPCSStatWorker(std::unique_ptr<rt::TcpConn> c) {
  while (true) {
    // Receive an uptime request.
    uint64_t magic;
    ssize_t ret = c->ReadFull(&magic, sizeof(magic));
    if (ret != static_cast<ssize_t>(sizeof(magic))) {
      if (ret == 0 || ret == -ECONNRESET) break;
      log_err("read failed, ret = %ld", ret);
      break;
    }

    // Check for the right magic value.
    if (ntoh64(magic) != kRPCSStatMagic) break;

    // Calculate the current uptime.
    std::ifstream file("/proc/stat");
    std::string line;
    std::getline(file, line);
    std::istringstream ss(line);
    std::string tmp;
    uint64_t user, nice, system, idle, iowait, irq, softirq, steal, guest,
        guest_nice;
    ss >> tmp >> user >> nice >> system >> idle >> iowait >> irq >> softirq >>
        steal >> guest >> guest_nice;
    sstat_raw u = {idle + iowait,
                   user + nice + system + irq + softirq + steal,
                   rt::RuntimeMaxCores(),
                   static_cast<unsigned int>(sysconf(_SC_NPROCESSORS_ONLN)),
                   rpc::RpcServerStatCupdateRx(),
                   rpc::RpcServerStatEcreditTx(),
                   rpc::RpcServerStatCreditTx(),
                   rpc::RpcServerStatReqRx(),
                   rpc::RpcServerStatReqDropped(),
                   rpc::RpcServerStatRespTx()};

    // Send an uptime response.
    ssize_t sret = c->WriteFull(&u, sizeof(u));
    if (sret != sizeof(u)) {
      if (sret == -EPIPE || sret == -ECONNRESET) break;
      log_err("write failed, ret = %ld", sret);
      break;
    }
  }
}

void RPCSStatServer() {
  std::unique_ptr<rt::TcpQueue> q(
      rt::TcpQueue::Listen({0, kRPCSStatPort}, 4096));
  if (q == nullptr) panic("couldn't listen for connections");

  while (true) {
    rt::TcpConn *c = q->Accept();
    if (c == nullptr) panic("couldn't accept a connection");
    rt::Thread([=] { RPCSStatWorker(std::unique_ptr<rt::TcpConn>(c)); })
        .Detach();
  }
}

sstat_raw ReadRPCSStat() {
  std::unique_ptr<rt::TcpConn> c(
      rt::TcpConn::Dial({0, 0}, {raddr[0].ip, kRPCSStatPort}));
  uint64_t magic = hton64(kRPCSStatMagic);
  ssize_t ret = c->WriteFull(&magic, sizeof(magic));
  if (ret != static_cast<ssize_t>(sizeof(magic)))
    panic("sstat request failed, ret = %ld", ret);
  sstat_raw u;
  ret = c->ReadFull(&u, sizeof(u));
  if (ret != static_cast<ssize_t>(sizeof(u)))
    panic("sstat response failed, ret = %ld", ret);
  return sstat_raw{u.idle, u.busy, u.num_cores, u.max_cores, u.cupdate_rx,
                   u.ecredit_tx, u.credit_tx, u.req_rx, u.req_dropped, u.resp_tx};
}

shstat_raw ReadShenangoStat() {
  char *buf_;
  std::string buf;
  std::map<std::string, uint64_t> smap;
  std::unique_ptr<rt::TcpConn> c(
      rt::TcpConn::Dial({0, 0}, {raddr[0].ip, kShenangoStatPort}));
  uint64_t magic = hton64(kShenangoStatMagic);
  ssize_t ret = c->WriteFull(&magic, sizeof(magic));
  if (ret != static_cast<ssize_t>(sizeof(magic)))
    panic("Shenango stat request failed, ret = %ld", ret);

  size_t resp_len;
  ret = c->ReadFull(&resp_len, sizeof(resp_len));
  if (ret != static_cast<ssize_t>(sizeof(resp_len)))
    panic("Shenango stat response failed, ret = %ld", ret);

  buf_ = (char *)malloc(resp_len);

  ret = c->ReadFull(buf_, resp_len);
  if (ret != static_cast<ssize_t>(resp_len))
    panic("Shenango stat response failed, ret = %ld", ret);

  buf = std::string(buf_);

  size_t pos_com = 0;
  size_t pos_col = 0;
  std::string token;
  std::string key;
  uint64_t value;

  while ((pos_com = buf.find(",")) != std::string::npos) {
    token = buf.substr(0, pos_com);
    pos_col = token.find(":");
    if (pos_col == std::string::npos) continue;

    key = token.substr(0, pos_col);
    value = std::stoull(token.substr(pos_col + 1, pos_com));

    smap[key] = value;

    buf.erase(0, pos_com + 1);
  }

  free(buf_);

  return shstat_raw{smap["rx_packets"], smap["tx_packets"],
                    smap["rx_bytes"],   smap["tx_bytes"],
                    smap["drops"],      smap["rx_tcp_out_of_order"]};
}

constexpr uint64_t kNetbenchPort = 8001;


// The maximum lateness to tolerate before dropping egress samples.
constexpr uint64_t kMaxCatchUpUS = 5;

#define BOTTLENECK_N 2

//static rt::Mutex shared_mutex;
static mutex_t shared_mutex[BOTTLENECK_N];
static spinlock_t shared_spin[BOTTLENECK_N];
static rt::CondVar shared_cv;
static int running;

void RpcServer(struct srpc_ctx *ctx) {
  // Validate and parse the request.
  if (unlikely(ctx->req_len != sizeof(payload))) {
    log_err("got invalid RPC len %ld", ctx->req_len);
    return;
  }
  const payload *in = reinterpret_cast<const payload *>(ctx->req_buf);

  // Perform the synthetic work.
  uint64_t workn = ntoh64(in->work_iterations);
  // uint64_t hash = ntoh64(in->hash);  // unused without mutex stuff
  int core_id = get_current_affinity();
  SyntheticWorker *w = workers[core_id];
  // int stype = ctx->s->session_type; // seems unused

  // int midx;

  // if (hash % 1000 < 200) midx = 0;
  // else midx = 1;

  if (workn != 0) {
    w->Work(workn);
    // if (mutex_lock_if_uncongested(&shared_mutex[midx])) {
    //   workers[get_current_affinity()]->Work(workn);
    //   mutex_unlock(&shared_mutex[midx]);
    // } else {
    //   ctx->drop = true;
    //   return;
    // }
  }

  // Craft a response.
  ctx->resp_len = sizeof(payload);
  payload *out = reinterpret_cast<payload *>(ctx->resp_buf);
  memcpy(out, in, sizeof(*out));
  out->tsc_end = hton64(rdtscp(&out->cpu));
  out->cpu = hton32(out->cpu);
  out->server_queue = hton64(rt::RuntimeQueueUS());
}

void ServerHandler(void *arg) {
  rt::Thread([] { RPCSStatServer(); }).Detach();
  int num_cores = rt::RuntimeMaxCores();

  for (int i = 0; i < num_cores; ++i) {
    workers[i] = SyntheticWorkerFactory("stridedmem:3200:64");
    if (workers[i] == nullptr) panic("cannot create worker");
  }

  running = 0;

  int ret = rpc::RpcServerEnable(RpcServer);
  if (ret) panic("couldn't enable RPC server");

  //sbw_register_delay_source(0, get_mutex_delay);
  //sbw_register_delay_source(1, get_mutex_delay);

  // waits forever.
  rt::WaitGroup(1).Wait();
}

void LoadBalancer(struct srpc_ctx *ctx) {
  // Validate and parse the request
  if (unlikely(ctx->req_len != sizeof(payload))) {
    log_err("got invalid RPC len %ld", ctx->req_len);
    return;
  }
  const payload *in = reinterpret_cast<const payload *>(ctx->req_buf);

  LBCTX<payload> *resp_ctx = load_balancer[0]->Send(
			(void *)ctx->req_buf, sizeof(payload), in->hash);
  resp_ctx->Wait();

  // Craft a response
  ctx->resp_len = sizeof(payload);
  ctx->ds_credit = load_balancer[0]->Credit();
  ctx->drop = resp_ctx->IsDropped();

  payload *out = reinterpret_cast<payload *>(ctx->resp_buf);
  memcpy(out, in, sizeof(*out));

  out->tsc_end = hton64(rdtscp(&out->cpu));
  out->cpu = hton32(out->cpu);
  out->server_queue = hton64(rt::RuntimeQueueUS());

  delete resp_ctx;
}

void LBLocalDropHandler(struct crpc_ctx *c) {
  LBCTX<payload> *ctx =
	  rpc::LoadBalancer<payload, PAYLOAD_ID_OFF>::GetCTX(c->buf);

  memcpy(&ctx->resp, c->buf, c->len);
  ctx->dropped = true;
  ctx->Done();
}

void LBRemoteDropHandler(void *buf, size_t len, void *arg) {
  assert(len == sizeof(payload));
  LBCTX<payload> *ctx =
	  rpc::LoadBalancer<payload, PAYLOAD_ID_OFF>::GetCTX((char *)buf);

  ctx->dropped = true;
  ctx->Done();
}

void LBHandler(void *arg) {
  rt::Thread([] { RPCSStatServer(); }).Detach();

  load_balancer[0] = new rpc::LoadBalancer<payload, PAYLOAD_ID_OFF>(raddr,
			num_servers, kNumDupClient, nconn[0], LBLocalDropHandler,
			LBRemoteDropHandler);

  /* Start Server */
  int ret = rpc::RpcServerEnable(LoadBalancer);
  if (ret) panic("couldn't start LB server");
  // waits forever.
  rt::WaitGroup(1).Wait();
}

void FanOut(struct srpc_ctx *ctx) {
  // Validate and parse the request
  if (unlikely(ctx->req_len != sizeof(payload))) {
    log_err("got invalid RPC len %ld", ctx->req_len);
    return;
  }
  const payload *in = reinterpret_cast<const payload *>(ctx->req_buf);

  FOCTX<payload> *resp_ctx = fan_outer->Send(
			(void *)ctx->req_buf, sizeof(payload), in->hash);

  resp_ctx->Wait();

  // Craft a response
  ctx->resp_len = sizeof(payload);
  ctx->ds_credit = fan_outer->Credit();
  ctx->drop = resp_ctx->IsDropped();

  payload *out = reinterpret_cast<payload *>(ctx->resp_buf);
  memcpy(out, in, sizeof(*out));

  out->tsc_end = hton64(rdtscp(&out->cpu));
  out->cpu = hton32(out->cpu);
  out->server_queue = hton64(rt::RuntimeQueueUS());

  delete resp_ctx;
}

void FOLocalDropHandler(struct crpc_ctx *c) {
  FOCTX<payload> *ctx = rpc::FanOuter<payload, PAYLOAD_ID_OFF>::GetCTX(c->buf);
  int idx = ctx->num_resp++;

  memcpy(&ctx->resp[idx], c->buf, c->len);
  ctx->dropped[idx] = true;
  ctx->Done();
}

void FORemoteDropHandler(void *buf, size_t len, void *arg) {
  assert(len == sizeof(payload));
  FOCTX<payload> *ctx =
	  rpc::FanOuter<payload, PAYLOAD_ID_OFF>::GetCTX((char *)buf);
  int idx = ctx->num_resp++;

  ctx->dropped[idx] = true;
  ctx->Done();
}

void FOHandler(void *arg) {
  rt::Thread([] { RPCSStatServer(); }).Detach();

  fan_outer = new rpc::FanOuter<payload, PAYLOAD_ID_OFF>(raddr,
			num_servers, kNumDupClient, nconn[0],
			FOLocalDropHandler, FORemoteDropHandler);

  /* Start server */
  int ret = rpc::RpcServerEnable(FanOut);
  if (ret) panic("couldn't start FO server");
  // waits forever.
  rt::WaitGroup(1).Wait();
}

void Sequential(struct srpc_ctx *ctx) {
  uint64_t ds_credit;
  bool success = true;
  // Validate and parse the request
  if (unlikely(ctx->req_len != sizeof(payload))) {
    log_err("got invalid RPC len %ld", ctx->req_len);
    return;
  }
  const payload *in = reinterpret_cast<const payload *>(ctx->req_buf);

  LBCTX<payload> *resp_ctx;
  for(int i = 0; i < num_servers; ++i) {
    resp_ctx = load_balancer[i]->Send(
			(void *)ctx->req_buf, sizeof(payload), in->hash);
    resp_ctx->Wait();

    if (resp_ctx->IsDropped()) {
      success = false;
      break;
    }
  }

  ds_credit = load_balancer[0]->Credit();
  for (int i = 1; i < num_servers; ++i) {
    ds_credit = MIN(ds_credit, load_balancer[i]->Credit());
  }

  // Craft a response
  ctx->resp_len = sizeof(payload);
  ctx->ds_credit = ds_credit;
  ctx->drop = !success;

  payload *out = reinterpret_cast<payload *>(ctx->resp_buf);
  memcpy(out, in, sizeof(*out));

  out->tsc_end = hton64(rdtscp(&out->cpu));
  out->cpu = hton32(out->cpu);
  out->server_queue = hton64(rt::RuntimeQueueUS());

  delete resp_ctx;
}


void SEQHandler(void *arg) {
  rt::Thread([] { RPCSStatServer(); }).Detach();

  for(int i = 0; i < num_servers; ++i) {
    load_balancer[i] = new rpc::LoadBalancer<payload, PAYLOAD_ID_OFF>(
			raddr + i, 1, kNumDupClient, nconn[i],
			LBLocalDropHandler, LBRemoteDropHandler);
  }

  /* Start server */
  int ret = rpc::RpcServerEnable(Sequential);
  if (ret) panic("couldn't start FO server");
  // waits forever.
  rt::WaitGroup(1).Wait();
}

template <class Arrival, class Service>
std::vector<work_unit> GenerateWork(Arrival a, Service s, double cur_us,
                                    double last_us, bool is_monster) {
  std::vector<work_unit> w;
  double st_us;
  while (true) {
/*
    if (cur_us < 3000000)
      cur_us += a();
    else
      cur_us += a() / 5.0;
*/
    cur_us += a();
    if (cur_us > last_us) break;
    switch (st_type) {
      case 1: // exponential
	st_us = s();
        break;
      case 2: // constant
	st_us = st;
	break;
      case 3: // bimodal
	if (rand() % 10 < 2) {
          st_us = st * 4.0;
	} else {
          st_us = st * 0.25;
	}
	break;
      default:
	panic("unknown service time distribution");
    }
    w.emplace_back(work_unit{cur_us, st_us, 0, rand(), false, is_monster});
  }

  return w;
}

std::vector<work_unit> ClientWorker(
    rpc::RpcClient *c, rt::WaitGroup *starter, rt::WaitGroup *starter2,
    std::function<std::vector<work_unit>()> wf) {
  std::vector<work_unit> w(wf());

  std::vector<rt::Thread> ths;

  // Start the receiver thread.
  for(int i = 0; i < c->NumConns(); ++i) {
    ths.emplace_back(rt::Thread([&, i] {
      payload rp;

      while (true) {
        ssize_t ret = c->Recv(&rp, sizeof(rp), i, (void *)w.data());
        if (ret != static_cast<ssize_t>(sizeof(rp))) {
          if (ret == 0 || ret < 0) break;
          panic("read failed, ret = %ld", ret);
        }

        uint64_t idx = ntoh64(rp.index);

        w[idx].duration_us = microtime() - w[idx].timing;
	w[idx].success = true;
        w[idx].credit = c->Credit();
        w[idx].tsc = ntoh64(rp.tsc_end);
        w[idx].cpu = ntoh32(rp.cpu);
        w[idx].server_queue = ntoh64(rp.server_queue);
        w[idx].server_time = w[idx].work_us + w[idx].server_queue;
      }
    }));
  }

  // Synchronized start of load generation.
  starter->Done();
  starter2->Wait();

  barrier();
  auto expstart = steady_clock::now();
  barrier();

  payload p;
  auto wsize = w.size();

  for (unsigned int i = 0; i < wsize; ++i) {
    barrier();
    auto now = steady_clock::now();
    barrier();
    if (duration_cast<sec>(now - expstart).count() < w[i].start_us) {
      rt::Sleep(w[i].start_us - duration_cast<sec>(now - expstart).count());
    }

    if (i > 1 && w[i-1].start_us <= kWarmUpTime &&
	w[i].start_us >= kWarmUpTime)
      c->StatClear();

    if (duration_cast<sec>(now - expstart).count() - w[i].start_us >
        kMaxCatchUpUS)
      continue;

    w[i].timing = microtime();

    // Send an RPC request.
    p.work_iterations = hton64(w[i].work_us * kIterationsPerUS);
    p.index = hton64(i);
    p.hash = hton64(w[i].hash);
    ssize_t ret = c->Send(&p, sizeof(p), w[i].hash, (void *)w.data());
    if (ret == -ENOBUFS) continue;
    if (ret != static_cast<ssize_t>(sizeof(p)))
      panic("write failed, ret = %ld", ret);
  }

  // rt::Sleep(1 * rt::kSeconds);
  rt::Sleep((int)(kRTT + 2 * st));
  //c->Abort();
  BUG_ON(c->Shutdown(SHUT_RDWR));

  for (auto &th : ths)
    th.Join();

  return w;
}

void ClientLocalDropHandler(struct crpc_ctx *c) {
  payload *req = reinterpret_cast<payload *>(c->buf);
  uint64_t idx = ntoh64(req->index);
  work_unit *w = reinterpret_cast<work_unit *>(c->arg);

  w[idx].duration_us = 0;
  w[idx].success = false;
}

void ClientRemoteDropHandler(void *buf, size_t len, void *arg) {
  assert(len == sizeof(payload));
  payload *req = (payload *)buf;
  uint64_t idx = ntoh64(req->index);
  work_unit *w = reinterpret_cast<work_unit *>(arg);

  w[idx].duration_us = microtime() - w[idx].timing;
  w[idx].success = false;
}

std::vector<work_unit> RunExperiment(
    int threads, struct cstat_raw *csr, struct sstat *ss, double *elapsed,
    std::function<std::vector<work_unit>()> wf,
    std::function<std::vector<work_unit>()> wf2) {
  // Create one TCP connection per thread.
  std::vector<std::unique_ptr<rpc::RpcClient>> clients;
  sstat_raw s1, s2;
  shstat_raw sh1, sh2;

  int server_idx;
  int conn_idx;

  for (int i = 0; i < threads; ++i) {
    //struct rpc_session_info info = {.session_type = (i % 10 < 1 ? 1 : 0)};
    struct rpc_session_info info = {.session_type = 0};
    std::unique_ptr<rpc::RpcClient> outc(rpc::RpcClient::Dial(raddr[0], i + 1,
					ClientLocalDropHandler,
					ClientRemoteDropHandler,
					&info));
    if (unlikely(outc == nullptr)) panic("couldn't connect to raddr.");

    if (nconn[0] > 1) {
      server_idx = 0;
      conn_idx = 1;
    } else {
      server_idx = 1;
      conn_idx = 0;
    }

    while (server_idx < num_servers) {
      outc->AddConnection(raddr[server_idx]);
      if (++conn_idx >= nconn[server_idx]) {
        server_idx++;
	conn_idx = 0;
      }
    }
    clients.emplace_back(std::move(outc));
  }

  // Launch a worker thread for each connection.
  rt::WaitGroup starter(threads);
  rt::WaitGroup starter2(1);

  std::vector<rt::Thread> th;
  std::unique_ptr<std::vector<work_unit>> samples[threads];

  th.emplace_back(rt::Thread([&] {
    auto v = ClientWorker(clients[0].get(), &starter, &starter2, wf2);
    samples[0].reset(new std::vector<work_unit>(std::move(v)));
  }));

  for (int i = 1; i < threads; ++i) {
    th.emplace_back(rt::Thread([&, i] {
      srand(time(NULL) * (i+1));
      auto v = ClientWorker(clients[i].get(), &starter, &starter2, wf);
      samples[i].reset(new std::vector<work_unit>(std::move(v)));
    }));
  }

  if (!b || b->IsLeader()) {
    s1 = ReadRPCSStat();
    sh1 = ReadShenangoStat();
  }

  // Give the workers time to initialize, then start recording.
  starter.Wait();
  if (b && !b->StartExperiment()) {
    exit(0);
  }
  starter2.Done();

  // |--- start experiment duration timing ---|
  barrier();
  timex = std::time(nullptr);
  auto start = steady_clock::now();
  barrier();

  // Clear the stat after warmup time
  rt::Sleep(kWarmUpTime);
  if (!b || b->IsLeader()) {
    s1 = ReadRPCSStat();
    sh1 = ReadShenangoStat();
  }
  for (auto &c : clients) {
    c->StatClear();
  }

  // Wait for the workers to finish.
  for (auto &t : th) {
    t.Join();
  }

  // |--- end experiment duration timing ---|
  barrier();
  auto finish = steady_clock::now();
  barrier();

  if (!b || b->IsLeader()) {
    s2 = ReadRPCSStat();
    sh2 = ReadShenangoStat();
  }

  // Force the connections to close.
  for (auto &c : clients) c->Abort();

  double elapsed_ = duration_cast<sec>(finish - start).count();
  elapsed_ -= kWarmUpTime;

  // Aggregate client stats
  if (csr) {
    for (auto &c : clients) {
      csr->ecredit_rx += c->StatEcreditRx();
      csr->cupdate_tx += c->StatCupdateTx();
      csr->resp_rx += c->StatRespRx();
      csr->req_tx += c->StatReqTx();
      csr->credit_expired += c->StatCreditExpired();
      c->Close();
    }
  }

  // Aggregate all the samples together.
  std::vector<work_unit> w;
  double min_throughput = 0.0;
  double max_throughput = 0.0;
  uint64_t good_resps = 0;
  uint64_t resps = 0;
  uint64_t offered = 0;
  uint64_t client_drop = 0;

  // ERIC
  std::vector<work_unit> client_drop_tasks;
  // END ERIC

  for (int i = 0; i < threads; ++i) {
    auto &v = *samples[i];
    double throughput;
    int slo_success;
    int resp_success;

    // Remove requests arrived during warm-up periods
    v.erase(std::remove_if(v.begin(), v.end(),
                        [](const work_unit &s) {
                          return ((s.start_us + s.duration_us) < kWarmUpTime);
                        }),
            v.end());

    offered += v.size();
    client_drop += std::count_if(v.begin(), v.end(), [](const work_unit &s) {
      return (s.duration_us == 0);
    });
    // ERIC
    std::copy_if(v.begin(), v.end(), std::back_inserter(client_drop_tasks), [](const work_unit &s) {
                          return (s.duration_us == 0);
                        });
    // END ERIC
    // Remove local drops
    v.erase(std::remove_if(v.begin(), v.end(),
                        [](const work_unit &s) {
                          return (s.duration_us == 0);
                        }),
            v.end());
    resp_success = std::count_if(v.begin(), v.end(), [](const work_unit &s) {
      return s.success;
    });
    slo_success = std::count_if(v.begin(), v.end(), [](const work_unit &s) {
      return s.success && s.duration_us < slo;
    });
    throughput = static_cast<double>(resp_success) / elapsed_ * 1000000;

    resps += resp_success;
    good_resps += slo_success;
    if (i == 0) {
      min_throughput = throughput;
      max_throughput = throughput;
    } else {
      min_throughput = MIN(throughput, min_throughput);
      max_throughput = MAX(throughput, max_throughput);
    }

    w.insert(w.end(), v.begin(), v.end());
  }

  // ERIC
  // sort my vector (if possible), and outfile it
  #if ENABLE_DOWNLOAD_ALL_TASKS
  std::ofstream client_drop_tasks_file;
  client_drop_tasks_file.open ("client_drop_tasks.csv");
  client_drop_tasks_file << "start_us,work_us,duration_us,tsc,server_queue,server_time" << std::endl;
  client_drop_tasks_file << std::setprecision(8) << std::fixed;
  for (unsigned int i = 0; i < client_drop_tasks.size(); ++i) {
    client_drop_tasks_file << client_drop_tasks[i].start_us << "," << client_drop_tasks[i].work_us << ","
                           << int(client_drop_tasks[i].duration_us) << "," << client_drop_tasks[i].tsc << ","
                           << client_drop_tasks[i].server_queue << "," << client_drop_tasks[i].server_time << std::endl;
  }
  client_drop_tasks_file.close();
  std::cout << "offered: " << offered << std::endl;
  std::cout << "resps: " << resps << std::endl;
  std::cout << "elapsed: " << elapsed_ << std::endl;
  #endif
  // END ERIC

  // Report results.
  if (csr) {
    csr->offered_rps = static_cast<double>(offered) / elapsed_ * 1000000;
    csr->rps = static_cast<double>(resps) / elapsed_ * 1000000;
    csr->goodput = static_cast<double>(good_resps) / elapsed_ * 1000000;
    csr->req_dropped = client_drop;
    csr->min_percli_tput = min_throughput;
    csr->max_percli_tput = max_throughput;
  }

  if ((!b || b->IsLeader()) && ss) {
    uint64_t idle = s2.idle - s1.idle;
    uint64_t busy = s2.busy - s1.busy;
    ss->cpu_usage =
        static_cast<double>(busy) / static_cast<double>(idle + busy);

    ss->cpu_usage =
        (ss->cpu_usage - 1 / static_cast<double>(s1.max_cores)) /
        (static_cast<double>(s1.num_cores) / static_cast<double>(s1.max_cores));

    uint64_t cupdate_rx_pkts = s2.cupdate_rx - s1.cupdate_rx;
    uint64_t ecredit_tx_pkts = s2.ecredit_tx - s1.ecredit_tx;
    uint64_t credit_tx = s2.credit_tx - s1.credit_tx;
    uint64_t req_rx_pkts = s2.req_rx - s1.req_rx;
    uint64_t req_drop_pkts = s2.req_dropped - s1.req_dropped;
    uint64_t resp_tx_pkts = s2.resp_tx - s1.resp_tx;
    ss->cupdate_rx_pps = static_cast<double>(cupdate_rx_pkts) / elapsed_ * 1000000;
    ss->ecredit_tx_pps = static_cast<double>(ecredit_tx_pkts) / elapsed_ * 1000000;
    ss->credit_tx_cps = static_cast<double>(credit_tx) / elapsed_ * 1000000;
    ss->req_rx_pps = static_cast<double>(req_rx_pkts) / elapsed_ * 1000000;
    ss->req_drop_rate =
        static_cast<double>(req_drop_pkts) / static_cast<double>(req_rx_pkts);
    ss->resp_tx_pps = static_cast<double>(resp_tx_pkts) / elapsed_ * 1000000;

    uint64_t rx_pkts = sh2.rx_pkts - sh1.rx_pkts;
    uint64_t tx_pkts = sh2.tx_pkts - sh1.tx_pkts;
    uint64_t rx_bytes = sh2.rx_bytes - sh1.rx_bytes;
    uint64_t tx_bytes = sh2.tx_bytes - sh1.tx_bytes;
    uint64_t drops = sh2.drops - sh1.drops;
    uint64_t rx_tcp_ooo = sh2.rx_tcp_ooo - sh1.rx_tcp_ooo;
    ss->rx_pps = static_cast<double>(rx_pkts) / elapsed_ * 1000000;
    ss->tx_pps = static_cast<double>(tx_pkts) / elapsed_ * 1000000;
    ss->rx_bps = static_cast<double>(rx_bytes) / elapsed_ * 8000000;
    ss->tx_bps = static_cast<double>(tx_bytes) / elapsed_ * 8000000;
    ss->rx_drops_pps = static_cast<double>(drops) / elapsed_ * 1000000;
    ss->rx_ooo_pps = static_cast<double>(rx_tcp_ooo) / elapsed_ * 1000000;
  }

  *elapsed = elapsed_;

  return w;
}

void PrintHeader(std::ostream &os) {
  os << "num_threads,"
     << "offered_load,"
     << "throughput,"
     << "goodput,"
     << "cpu,"
     << "min,"
     << "mean,"
     << "p50,"
     << "p90,"
     << "p99,"
     << "p999,"
     << "p9999,"
     << "max,"
     << "reject_min"
     << "reject_mean"
     << "reject_p50"
     << "reject_p99"
     << "p1_credit,"
     << "mean_credit,"
     << "p99_credit,"
     << "p1_q,"
     << "mean_q,"
     << "p99_q,"
     << "mean_stime,"
     << "p99_stime,"
     << "server:rx_pps,"
     << "server:tx_pps,"
     << "server:rx_bps,"
     << "server:tx_bps,"
     << "server:rx_drops_pps,"
     << "server:rx_ooo_pps,"
     << "server:cupdate_rx_pps,"
     << "server:ecredit_tx_pps,"
     << "server:credit_tx_cps,"
     << "server:req_rx_pps,"
     << "server:req_drop_rate,"
     << "server:resp_tx_pps,"
     << "client:min_tput,"
     << "client:max_tput,"
     << "client:ecredit_rx_pps,"
     << "client:cupdate_tx_pps,"
     << "client:resp_rx_pps,"
     << "client:req_tx_pps,"
     << "client:credit_expired_cps,"
     << "client:req_dropped_rps" << std::endl;
}

void PrintStatResults(std::vector<work_unit> w, struct cstat *cs,
                      struct sstat *ss) {
  if (w.size() == 0) {
    std::cout << std::setprecision(4) << std::fixed << threads * total_agents
              << "," << cs->offered_rps << ","
              << "-" << std::endl;
    return;
  }

  std::vector<work_unit> rejected;

  std::copy_if(w.begin(), w.end(), std::back_inserter(rejected), [](work_unit &s) {
    return !s.success;
  });

  uint64_t reject_cnt = rejected.size();
  uint64_t reject_min = 0;
  uint64_t reject_p50 = 0;
  double reject_mean = 0.0;
  uint64_t reject_p99 = 0;

  if (reject_cnt > 0) {
    double sum;

    std::sort(rejected.begin(), rejected.end(),
	      [](const work_unit &s1, const work_unit &s2) {
        return s1.duration_us < s2.duration_us;
    });
    sum = std::accumulate(rejected.begin(), rejected.end(), 0.0,
			  [](double s, const work_unit &c) {
			      return s + c.duration_us;
			  });

    reject_min = rejected[0].duration_us;
    reject_mean = static_cast<double>(sum) / reject_cnt;
    reject_p50 = rejected[(reject_cnt - 1) * 0.5].duration_us;
    reject_p99 = rejected[(reject_cnt - 1) * 0.99].duration_us;
  }
  // ERIC
  std::vector<work_unit> server_drop_tasks;
  std::copy_if(w.begin(), w.end(), std::back_inserter(server_drop_tasks), [](const work_unit &s) {
                        return !s.success;
                      });
  // sort this once I know the the other fields are valid
  // std::sort(server_drop_tasks.begin(), server_drop_tasks.end(), [](const work_unit &s1, const work_unit &s2) {
  //   return s1.start_us < s2.start_us;
  // });
  #if ENABLE_DOWNLOAD_ALL_TASKS
  std::ofstream server_drop_tasks_file;
  server_drop_tasks_file.open ("server_drop_tasks.csv");
  server_drop_tasks_file << "start_us,work_us,duration_us,tsc,server_queue,server_time" << std::endl;
  server_drop_tasks_file << std::setprecision(8) << std::fixed;
  for (unsigned int i = 0; i < server_drop_tasks.size(); ++i) {
    server_drop_tasks_file << server_drop_tasks[i].start_us << "," << server_drop_tasks[i].work_us << ","
                           << int(server_drop_tasks[i].duration_us) << "," << server_drop_tasks[i].tsc << ","
                           << server_drop_tasks[i].server_queue << "," << server_drop_tasks[i].server_time << std::endl;
  }
  server_drop_tasks_file.close();
  #endif
  // END ERIC

  w.erase(std::remove_if(w.begin(), w.end(),
			 [](const work_unit &s) {
			   return !s.success;
	}), w.end());

  ///// ERIC
  // ordering by start times
  std::sort(w.begin(), w.end(), [](const work_unit &s1, const work_unit &s2) {
    return s1.start_us < s2.start_us;
  });
  
  /*
  work unit struct:
    double start_us, work_us, duration_us;
    // duration_us is calculated from two ints, so it is always an int
    int hash;
    bool success;
    bool is_monster;
    uint64_t credit;
    uint64_t tsc;
    uint32_t cpu;
    uint64_t server_queue;
    uint64_t server_time;
    uint64_t timing;
  */
  #if ENABLE_DOWNLOAD_ALL_TASKS
  std::ofstream all_tasks_file;
  all_tasks_file.open ("all_tasks.csv");
  all_tasks_file << "start_us,work_us,duration_us,tsc,server_queue,server_time" << std::endl;
  all_tasks_file << std::setprecision(8) << std::fixed;
  for (unsigned int i = 0; i < w.size(); ++i) {
    all_tasks_file << w[i].start_us << "," << w[i].work_us << "," << int(w[i].duration_us) << ","
                   << w[i].tsc << "," << w[i].server_queue << "," << w[i].server_time << std::endl;
  }
  all_tasks_file.close();
  #endif
  ///// END ERIC

  std::sort(w.begin(), w.end(), [](const work_unit &s1, const work_unit &s2) {
    return s1.duration_us < s2.duration_us;
  });
  double sum = std::accumulate(
      w.begin(), w.end(), 0.0,
      [](double s, const work_unit &c) { return s + c.duration_us; });
  double mean = sum / w.size();
  double count = static_cast<double>(w.size());
  double p50 = w[count * 0.5].duration_us;
  double p90 = w[count * 0.9].duration_us;
  double p99 = w[count * 0.99].duration_us;
  double p999 = w[count * 0.999].duration_us;
  double p9999 = w[count * 0.9999].duration_us;
  double min = w[0].duration_us;
  double max = w[w.size() - 1].duration_us;

  std::sort(w.begin(), w.end(), [](const work_unit &s1, const work_unit &s2) {
    return s1.credit < s2.credit;
  });
  double sum_credit = std::accumulate(
      w.begin(), w.end(), 0.0,
      [](double s, const work_unit &c) { return s + c.credit; });
  double mean_credit = sum_credit / w.size();
  double p1_credit = w[count * 0.01].credit;
  double p99_credit = w[count * 0.99].credit;

  std::sort(w.begin(), w.end(), [](const work_unit &s1, const work_unit &s2) {
    return s1.server_queue < s2.server_queue;
  });
  double sum_que = std::accumulate(
      w.begin(), w.end(), 0.0,
      [](double s, const work_unit &c) { return s + c.server_queue; });
  double mean_que = sum_que / w.size();
  double p1_que = w[count * 0.01].server_queue;
  double p99_que = w[count * 0.99].server_queue;

  std::sort(w.begin(), w.end(), [](const work_unit &s1, const work_unit &s2) {
    return s1.server_time < s2.server_time;
  });
  double sum_stime = std::accumulate(
      w.begin(), w.end(), 0.0,
      [](double s, const work_unit &c) { return s + c.server_time; });
  double mean_stime = sum_stime / w.size();
  double p99_stime = w[count * 0.99].server_time;

  std::cout << std::setprecision(4) << std::fixed << threads * total_agents << ","
	    << cs->offered_rps << "," << cs->rps << "," << cs->goodput << ","
	    << ss->cpu_usage << ","
	    << min << "," << mean << "," << p50 << "," << p90 << "," << p99 << ","
	    << p999 << "," << p9999 << "," << max << ","
	    << reject_min << "," << reject_mean << "," << reject_p50 << ","
	    << reject_p99 << ","
	    << p1_credit << "," << mean_credit << "," << p99_credit << ","
	    << p1_que << ","
	    << mean_que << "," << p99_que << "," << mean_stime << ","
	    << p99_stime << "," << ss->rx_pps << "," << ss->tx_pps << ","
	    << ss->rx_bps << "," << ss->tx_bps << "," << ss->rx_drops_pps << ","
	    << ss->rx_ooo_pps << "," << ss->cupdate_rx_pps << ","
	    << ss->ecredit_tx_pps << "," << ss->credit_tx_cps << ","
	    << ss->req_rx_pps << "," << ss->req_drop_rate << ","
	    << ss->resp_tx_pps << ","
	    << cs->min_percli_tput << "," << cs->max_percli_tput << ","
	    << cs->ecredit_rx_pps << "," << cs->cupdate_tx_pps << ","
	    << cs->resp_rx_pps << "," << cs->req_tx_pps << ","
	    << cs->credit_expired_cps << "," << cs->req_dropped_rps << std::endl;

  csv_out << std::setprecision(4) << std::fixed << threads * total_agents << ","
          << cs->offered_rps << "," << cs->rps << "," << cs->goodput << ","
          << ss->cpu_usage << ","
	  << min << "," << mean << "," << p50 << "," << p90 << "," << p99 << ","
	  << p999 << "," << p9999 << "," << max << ","
	  << reject_min << "," << reject_mean << "," << reject_p50 << ","
	  << reject_p99 << ","
	  << p1_credit << "," << mean_credit << "," << p99_credit << ","
	  << p1_que << ","
	  << mean_que << "," << p99_que << "," << mean_stime << ","
	  << p99_stime << "," << ss->rx_pps << "," << ss->tx_pps << ","
	  << ss->rx_bps << "," << ss->tx_bps << "," << ss->rx_drops_pps << ","
	  << ss->rx_ooo_pps << "," << ss->cupdate_rx_pps << ","
	  << ss->ecredit_tx_pps << "," << ss->credit_tx_cps << ","
	  << ss->req_rx_pps << "," << ss->req_drop_rate << ","
	  << ss->resp_tx_pps << ","
	  << cs->min_percli_tput << "," << cs->max_percli_tput << ","
	  << cs->ecredit_rx_pps << "," << cs->cupdate_tx_pps << ","
	  << cs->resp_rx_pps << "," << cs->req_tx_pps << ","
	  << cs->credit_expired_cps << "," << cs->req_dropped_rps
	  << std::endl << std::flush;

  json_out << "{"
           << "\"num_threads\":" << threads * total_agents << ","
           << "\"offered_load\":" << cs->offered_rps << ","
           << "\"throughput\":" << cs->rps << ","
           << "\"goodput\":" << cs->goodput << ","
           << "\"cpu\":" << ss->cpu_usage << ","
           << "\"min\":" << min << ","
           << "\"mean\":" << mean << ","
           << "\"p50\":" << p50 << ","
           << "\"p90\":" << p90 << ","
           << "\"p99\":" << p99 << ","
           << "\"p999\":" << p999 << ","
           << "\"p9999\":" << p9999 << ","
           << "\"max\":" << max << ","
	   << "\"reject_min\":" << reject_min << ","
	   << "\"reject_mean\":" << reject_mean << ","
	   << "\"reject_p50\":" << reject_p50 << ","
	   << "\"reject_p99\":" << reject_p99 << ","
           << "\"p1_credit\":" << p1_credit << ","
           << "\"mean_credit\":" << mean_credit << ","
           << "\"p99_credit\":" << p99_credit << ","
           << "\"p1_q\":" << p1_que << ","
           << "\"mean_q\":" << mean_que << ","
           << "\"p99_q\":" << p99_que << ","
           << "\"mean_stime\":" << mean_stime << ","
           << "\"p99_stime\":" << p99_stime << ","
           << "\"server:rx_pps\":" << ss->rx_pps << ","
           << "\"server:tx_pps\":" << ss->tx_pps << ","
           << "\"server:rx_bps\":" << ss->rx_bps << ","
           << "\"server:tx_bps\":" << ss->tx_bps << ","
           << "\"server:rx_drops_pps\":" << ss->rx_drops_pps << ","
           << "\"server:rx_ooo_pps\":" << ss->rx_ooo_pps << ","
           << "\"server:cupdate_rx_pps\":" << ss->cupdate_rx_pps << ","
           << "\"server:ecredit_tx_pps\":" << ss->ecredit_tx_pps << ","
           << "\"server:credit_tx_cps\":" << ss->credit_tx_cps << ","
           << "\"server:req_rx_pps\":" << ss->req_rx_pps << ","
           << "\"server:req_drop_rate\":" << ss->req_drop_rate << ","
           << "\"server:resp_tx_pps\":" << ss->resp_tx_pps << ","
           << "\"client:min_tput\":" << cs->min_percli_tput << ","
           << "\"client:max_tput\":" << cs->max_percli_tput << ","
           << "\"client:ecredit_rx_pps\":" << cs->ecredit_rx_pps << ","
           << "\"client:cupdate_tx_pps\":" << cs->cupdate_tx_pps << ","
           << "\"client:resp_rx_pps\":" << cs->resp_rx_pps << ","
           << "\"client:req_tx_pps\":" << cs->req_tx_pps << ","
           << "\"client:credit_expired_cps\":" << cs->credit_expired_cps << ","
           << "\"client:req_dropped_rps\":" << cs->req_dropped_rps << "},"
           << std::endl
           << std::flush;
}

void LoadShiftExperiment(int threads, double service_time) {
  struct sstat ss;
  struct cstat_raw csr;
  struct cstat cs;
  double elapsed;

  memset(&csr, 0, sizeof(csr));

  std::vector<work_unit> w = RunExperiment(threads, &csr, &ss, &elapsed,[=] {
    std::mt19937 rg(rand());
    std::mt19937 wg(rand());
    std::exponential_distribution<double> wd(1.0 / service_time);
    std::vector<work_unit> w_temp;
    uint64_t last_us = 0;
    for (auto &r : rates) {
      double rate = r.first / (double) total_agents;
      std::exponential_distribution<double> rd(
          1.0 / (1000000.0 / (rate / static_cast<double>(threads))));
      auto work = GenerateWork(std::bind(rd, rg), std::bind(wd, wg), last_us,
                               last_us + r.second, false);
      last_us = work.back().start_us;
      w_temp.insert(w_temp.end(), work.begin(), work.end());
    }
    return w_temp;
  },
  [=] {
    std::mt19937 rg(rand());
    std::mt19937 wg(rand());
    std::exponential_distribution<double> wd(1.0 / service_time);
    std::vector<work_unit> w_temp;
    uint64_t last_us = 0;
    for (auto &r : rates) {
      double rate = r.first / (double) total_agents;
      std::exponential_distribution<double> rd(
          1.0 / (1000000.0 / (rate / static_cast<double>(threads))));
      auto work = GenerateWork(std::bind(rd, rg), std::bind(wd, wg), last_us,
                               last_us + r.second, true);
      last_us = work.back().start_us;
      w_temp.insert(w_temp.end(), work.begin(), work.end());
    }
    return w_temp;
  });

  if (b) {
    if (!b->EndExperiment(w, &csr)) return;
  }

  cs = cstat{csr.offered_rps,
             csr.rps,
             csr.goodput,
             csr.min_percli_tput,
             csr.max_percli_tput,
             static_cast<double>(csr.ecredit_rx) / elapsed * 1000000,
             static_cast<double>(csr.cupdate_tx) / elapsed * 1000000,
             static_cast<double>(csr.resp_rx) / elapsed * 1000000,
             static_cast<double>(csr.req_tx) / elapsed * 1000000,
             static_cast<double>(csr.credit_expired) / elapsed * 1000000,
             static_cast<double>(csr.req_dropped) / elapsed * 1000000};

  // Print the results.
  PrintStatResults(w, &cs, &ss);
}

void SteadyStateExperiment(int threads, double offered_rps,
                           double service_time) {
  struct sstat ss;
  struct cstat_raw csr;
  struct cstat cs;
  double elapsed;

  memset(&csr, 0, sizeof(csr));

  std::vector<work_unit> w = RunExperiment(threads, &csr, &ss, &elapsed,
					   [=] {
    std::mt19937 rg(rand());
    std::mt19937 dg(rand());
    std::exponential_distribution<double> rd(
        1.0 / (1000000.0 / (offered_rps / static_cast<double>(threads))));
    std::exponential_distribution<double> wd(1.0 / service_time);
    return GenerateWork(std::bind(rd, rg), std::bind(wd, dg), 0,
                        kExperimentTime, false);
  },
  [=] {
    std::mt19937 rg(rand());
    std::mt19937 dg(rand());
    std::exponential_distribution<double> rd(
        1.0 / (1000000.0 / (offered_rps / static_cast<double>(threads))));
    std::exponential_distribution<double> wd(1.0 / service_time);
    return GenerateWork(std::bind(rd, rg), std::bind(wd, dg), 0,
                        kExperimentTime, true);
  });

  if (b) {
    if (!b->EndExperiment(w, &csr)) return;
  }

  cs = cstat{csr.offered_rps,
             csr.rps,
             csr.goodput,
             csr.min_percli_tput,
             csr.max_percli_tput,
             static_cast<double>(csr.ecredit_rx) / elapsed * 1000000,
             static_cast<double>(csr.cupdate_tx) / elapsed * 1000000,
             static_cast<double>(csr.resp_rx) / elapsed * 1000000,
             static_cast<double>(csr.req_tx) / elapsed * 1000000,
             static_cast<double>(csr.credit_expired) / elapsed * 1000000,
             static_cast<double>(csr.req_dropped) / elapsed * 1000000};
  // Print the results.
  PrintStatResults(w, &cs, &ss);
}

int StringToAddr(const char *str, uint32_t *addr) {
  uint8_t a, b, c, d;

  if (sscanf(str, "%hhu.%hhu.%hhu.%hhu", &a, &b, &c, &d) != 4) return -EINVAL;

  *addr = MAKE_IP_ADDR(a, b, c, d);
  return 0;
}

void calculate_rates() {
  offered_loads.push_back(offered_load / (double)total_agents);
}

void AgentHandler(void *arg) {
  master.port = kBarrierPort;
  b = new NetBarrier(master);
  BUG_ON(!b);

  calculate_rates();

  if (experiment_type == 1) {
    LoadShiftExperiment(threads, st);
  } else {
    for (double i : offered_loads) {
      SteadyStateExperiment(threads, i, st);
    }
  }
}

void ClientHandler(void *arg) {
  int pos;

  if (total_agents > 1) {
    b = new NetBarrier(total_agents - 1);
    BUG_ON(!b);
  }

  calculate_rates();

  json_out.open("output.json");
  csv_out.open("output.csv", std::fstream::out | std::fstream::app);
  json_out << "[";

  /* Print Header */
  PrintHeader(std::cout);

  // for (double i : offered_loads) {
  //   SteadyStateExperiment(threads, i, st);
  //   rt::Sleep(1000000);
  // }
  if (experiment_type == 1) {
    LoadShiftExperiment(threads, st);
    rt::Sleep(1000000);
  } else {
    for (double i : offered_loads) {
      SteadyStateExperiment(threads, i, st);
      rt::Sleep(1000000);
    }
  }

  pos = json_out.tellp();
  json_out.seekp(pos - 2);
  json_out << "]";
  json_out.close();
  csv_out.close();
}

}  // anonymous namespace

void print_lb_usage() {
  std::cerr << "usage: [alg] [cfg_file] lb [server_ip #1] [nconn #1]\n"
	    << "\talg: overload control algorithms (breakwater/seda/dagor)\n"
	    << "\tcfg_file: Shenango configuration file\n"
	    << "\tserver_ip: server IP address\n"
	    << "\tnconn: the number of parallel connection to the server"
	    << std:: endl;
}

void print_fo_usage() {
  std::cerr << "usage: [alg] [cfg_file] fo [server_ip #1] [nconn #1]\n"
	    << "\talg: overload control algorithms (breakwater/seda/dagor)\n"
	    << "\tcfg_file: Shenango configuration file\n"
	    << "\tserver_ip: server IP address\n"
	    << "\tnconn: the number of parallel connection to the server"
	    << std:: endl;
}

void print_seq_usage() {
  std::cerr << "usage: [alg] [cfg_file] seq [server_ip #1] [nconn #1]\n"
	    << "\talg: overload control algorithms (breakwater/seda/dagor)\n"
	    << "\tcfg_file: Shenango configuration file\n"
	    << "\tserver_ip: server IP address\n"
	    << "\tnconn: the number of parallel connection to the server"
	    << std:: endl;
}

void print_client_usage() {
  std::cerr << "usage: [alg] [cfg_file] client [nclients] "
      << "[service_us] [service_dist] [slo] [nagents] "
      << "[offered_load] [experiment_type] [server_ip #1] [nconn #1] ...\n"
      << "\talg: overload control algorithms (breakwater/seda/dagor)\n"
      << "\tcfg_file: Shenango configuration file\n"
      << "\tnclients: the number of client connections\n"
      << "\tservice_us: average request processing time (in us)\n"
      << "\tservice_dist: request processing time distribution (exp/const/bimod)\n"
      << "\tslo: RPC service level objective (in us)\n"
      << "\tnagents: the number of agents\n"
      << "\toffered_load: load geneated by client and agents in requests per second\n"
      << "\texperiment_type: 0 for steady state, 1 for load shift\n"
      << "\tserver_ip: server IP address\n"
      << "\tnconn: the number of parallel connection to the server"
      << std::endl;
}

int main(int argc, char *argv[]) {
  int ret, i;

  if (argc < 4) {
    std::cerr << "usage: [alg] [cfg_file] [cmd] ...\n"
	      << "\talg: overload control algorithms (breakwater/seda/dagor)\n"
	      << "\tcfg_file: Shenango configuration file\n"
	      << "\tcmd: netbenchd command (server/client/agent)" << std::endl;
    return -EINVAL;
  }

  for (int i = 0; i < BOTTLENECK_N; ++i) {
    mutex_init(&shared_mutex[i]);
    spin_lock_init(&shared_spin[i]);
  }

  std::string olc = argv[1]; // overload control
  if (olc.compare("breakwater") == 0) {
    crpc_ops = &cbw_ops;
    srpc_ops = &sbw_ops;
  } else if (olc.compare("protego") == 0) {
    crpc_ops = &cbw_ops;
    srpc_ops = &sbw2_ops;
  } else if (olc.compare("seda") == 0) {
    crpc_ops = &csd_ops;
    srpc_ops = &ssd_ops;
  } else if (olc.compare("dagor") == 0) {
    crpc_ops = &cdg_ops;
    srpc_ops = &sdg_ops;
  } else if (olc.compare("nocontrol") == 0) {
    crpc_ops = &cnc_ops;
    srpc_ops = &snc_ops;
  } else {
    std::cerr << "invalid algorithm: " << olc << std::endl;
    std::cerr << "usage: [alg] [cfg_file] [cmd] ...\n"
	      << "\talg: overload control algorithms (breakwater/seda/dagor)\n"
	      << "\tcfg_file: Shenango configuration file\n"
	      << "\tcmd: netbenchd command (server/client/agent)" << std::endl;
    return -EINVAL;
  }

  std::string cmd = argv[3];
  if (cmd.compare("server") == 0) {
    // Server
    ret = runtime_init(argv[2], ServerHandler, NULL);
    if (ret) {
      printf("failed to start runtime\n");
      return ret;
    }
  } else if (cmd.compare("agent") == 0) {
    // Agent
    if (argc < 6 || StringToAddr(argv[4], &master.ip)) {
      std::cerr << "usage: [alg] [cfg_file] agent [client_ip] [experiment_type]\n"
	        << "\talg: overload control algorithms (breakwater/seda/dagor)\n"
		<< "\tcfg_file: Shenango configuration file\n"
		<< "\tclient_ip: Client IP address\n"
    << "\texperiment_type: 0 for steady state, 1 for load shift" << std::endl;
      return -EINVAL;
    }
    experiment_type = std::stoi(argv[5], nullptr, 0);

    ret = runtime_init(argv[2], AgentHandler, NULL);
    if (ret) {
      printf("failed to start runtime\n");
      return ret;
    }
  } else if (cmd.compare("lb") == 0) {
    // Load-Balancer
    if (argc < 6) {
      print_lb_usage();
      return -EINVAL;
    }

    num_servers = argc - 4;
    if (num_servers % 2 != 0) {
      print_lb_usage();
      return -EINVAL;
    }
    num_servers /= 2;

    for (i = 0; i < num_servers; ++i) {
      ret = StringToAddr(argv[4+2*i], &raddr[i].ip);
      if (ret) {
        std::cerr << "[Error] Cannot parse server IP: " << argv[4+2*i]
		  << std::endl;
	return -EINVAL;
      }
      raddr[i].port = kNetbenchPort;
      nconn[i] = std::stoi(argv[5+2*i], nullptr, 0);
    }

    ret = runtime_init(argv[2], LBHandler, NULL);
    if (ret) {
      std::cerr << "[Error] Failed to start runtime" << std::endl;
      return ret;
    }
  } else if (cmd.compare("fo") == 0) {
    // Load-Balancer
    if (argc < 6) {
      print_fo_usage();
      return -EINVAL;
    }

    num_servers = argc - 4;
    if (num_servers % 2 != 0) {
      print_fo_usage();
      return -EINVAL;
    }
    num_servers /= 2;

    for (i = 0; i < num_servers; ++i) {
      ret = StringToAddr(argv[4+2*i], &raddr[i].ip);
      if (ret) {
        std::cerr << "[Error] Cannot parse server IP: " << argv[4+2*i]
		  << std::endl;
	return -EINVAL;
      }
      raddr[i].port = kNetbenchPort;
      nconn[i] = std::stoi(argv[5+2*i], nullptr, 0);
    }

    ret = runtime_init(argv[2], FOHandler, NULL);
    if (ret) {
      std::cerr << "[Error] Failed to start runtime" << std::endl;
      return ret;
    }
  } else if (cmd.compare("seq") == 0) {
    // Load-Balancer
    if (argc < 6) {
      print_seq_usage();
      return -EINVAL;
    }

    num_servers = argc - 4;
    if (num_servers % 2 != 0) {
      print_seq_usage();
      return -EINVAL;
    }
    num_servers /= 2;

    for (i = 0; i < num_servers; ++i) {
      ret = StringToAddr(argv[4+2*i], &raddr[i].ip);
      if (ret) {
        std::cerr << "[Error] Cannot parse server IP: " << argv[4+2*i]
		  << std::endl;
	return -EINVAL;
      }
      raddr[i].port = kNetbenchPort;
      nconn[i] = std::stoi(argv[5+2*i], nullptr, 0);
    }

    ret = runtime_init(argv[2], SEQHandler, NULL);
    if (ret) {
      std::cerr << "[Error] Failed to start runtime" << std::endl;
      return ret;
    }
  } else if (cmd.compare("client") != 0) {
    std::cerr << "invalid command: " << cmd << std::endl;
    std::cerr << "usage: [alg] [cfg_file] [cmd] ...\n"
	      << "\talg: overload control algorithms (breakwater/seda/dagor)\n"
	      << "\tcfg_file: Shenango configuration file\n"
	      << "\tcmd: netbenchd command (server/client/agent)" << std::endl;
    return -EINVAL;
  }

  if (argc < 12) {
    print_client_usage();
    return -EINVAL;
  }

  threads = std::stoi(argv[4], nullptr, 0);
  st = std::stod(argv[5], nullptr);

  std::string st_dist = argv[6];
  if (st_dist.compare("exp") == 0) {
    st_type = 1;
  } else if (st_dist.compare("const") == 0) {
    st_type = 2;
  } else if (st_dist.compare("bimod") == 0) {
    st_type = 3;
  } else {
    std::cerr << "invalid service time distribution: " << st_dist << std::endl;
    print_client_usage();
    return -EINVAL;
  }

  slo = std::stoi(argv[7], nullptr, 0);
  total_agents += std::stoi(argv[8], nullptr, 0);
  offered_load = std::stod(argv[9], nullptr);
  experiment_type = std::stoi(argv[10], nullptr, 0);

  /*
    ERIC
    going to modify this to 11, and insert my loadshift param BEFORE this weird server_ip + conns listing

    hopefully keeps behavior the same in case I ever want to use this feature. 
  */
  num_servers = argc - 11; // does this make any sense?
  if (num_servers % 2 != 0) {
    print_client_usage();
    return -EINVAL;
  }
  num_servers /= 2;

  if (num_servers > 16) {
    std::cerr << "[Warning] the number of server exceeds 16."
	      << std::endl;
    num_servers = 16;
  }
/// ERIC
  if (num_servers != 1) {
    std::cerr << "[Error] num_servers is not equal to 1. Unsure if what happens if it isn't equal to 1"
              << std::endl;
    return -EINVAL;
  }
/// END ERIC
  for(i = 0; i < num_servers; ++i) {
    int nconn_;

    ret = StringToAddr(argv[11+2*i], &raddr[i].ip);
    if (ret) {
      std::cerr << "[Error] Cannot parse server IP:" << argv[11+2*i]
	        << std::endl;
      return -EINVAL;
    }
    raddr[i].port = kNetbenchPort;

    nconn_ = std::stoi(argv[12+2*i], nullptr, 0);
    if (nconn_ > 16) {
      std::cerr << "[Warning] the number of parallel connection exceeds 16."
	        << std::endl;
      nconn_ = 16;
    }
    nconn[i] = nconn_;
  }

  ret = runtime_init(argv[2], ClientHandler, NULL);
  if (ret) {
    printf("failed to start runtime\n");
    return ret;
  }

  return 0;
}
