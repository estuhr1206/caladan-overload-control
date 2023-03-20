// loadbalancer.h - RPC downstream load balancer abstraction

#pragma once

#include "cc/sync.h"
#include "cc/thread.h"
#include "breakwater/rpc++.h"

template <typename T>
class LBCTX {
public:
  LBCTX() {
    waiter = new rt::WaitGroup(1);
    dropped = false;
  }

  ~LBCTX() {
    delete waiter;
  }

  void Wait() { waiter->Wait(); }
  void Done() { waiter->Done(); }
  bool IsDropped() { return dropped; }

  // request from upstream
  T req;
  // response from downstream
  T resp;
  // whether the request has been dropped in the downstream
  bool dropped;
  // waiter for synchronization
  rt::WaitGroup *waiter;
};

namespace rpc {
// T: a request struct or class
// ID_OFF: offset to the request ID in T
// (request ID should be uint64)
template <typename T, int ID_OFF>
class LoadBalancer {
public:
  // Disable move and copy.
  LoadBalancer(const LoadBalancer&) = delete;
  LoadBalancer& operator=(const LoadBalancer&) = delete;

  // raddrs: array of remote servers
  // nserver: number of downstream load balancers to connect
  // nclients: number of duplicate clients (scale out)
  // nconns: number of connections for each server (scale in)
  LoadBalancer(netaddr *raddrs, int nserver, int nclients, int nconns,
	       crpc_ldrop_fn_t ldrop_handler, crpc_rdrop_fn_t rdrop_handler);

  // Send a request to downstream
  // buf: RPC request buffer
  // len: RPC request size
  // hash: (optional) hash for randomized operation
  // returns load balancing context (LBCTX)
  LBCTX<T> *Send(const void *buf, size_t len, int hash);

  // Returns downstream window size
  uint32_t Credit();

  // Convert request/response buffer to LBCTX
  static LBCTX<T> *GetCTX(char *buf);

private:
  // array of pointers to clients
  RpcClient **clients_;
  // for iterating over multiple clients
  int next_cidx_;
  // the number of clients
  int nclients_;
}; // class LoadBalancer

template <typename T, int ID_OFF>
LoadBalancer<T, ID_OFF>::LoadBalancer(netaddr *raddrs, int nserver,
			int nclients, int nconns, crpc_ldrop_fn_t ldrop_handler,
			crpc_rdrop_fn_t rdrop_handler) {
  // Initialize member variables
  nclients_ = nclients;
  next_cidx_ = 0;
  clients_ = (RpcClient **)malloc(sizeof(RpcClient*) * nclients);

  int sidx, cidx;
  struct rpc_session_info info = {.session_type = 0};
  for (int i = 0; i < nclients; ++i) {
    // Dial to the first connection (Note Dial returns a new Client)
    // TODO: DropHandler
    clients_[i] = RpcClient::Dial(raddrs[0], 1, ldrop_handler, rdrop_handler, &info);

    // Add connections to the replicas
    if (nconns > 1) {
      sidx = 0;
      cidx = 1;
    } else {
      sidx = 1;
      cidx = 0;
    }

    while (sidx < nserver) {
      clients_[i]->AddConnection(raddrs[sidx]);
      if (++cidx >= nconns) {
        sidx++;
	cidx = 0;
      }
    }

    // Start the receiver thread for downstream
    for(int j = 0; j < clients_[i]->NumConns(); ++j) {
      rt::Thread([&, i, j] {
        T rp;

	while (true) {
	  ssize_t ret = clients_[i]->Recv(&rp, sizeof(rp), j, nullptr);
	  if (ret != static_cast<ssize_t>(sizeof(rp))) {
	    if (ret == 0 || ret < 0) break;
	    panic("read response from downstream failed, ret = %ld", ret);
	  }

	  LBCTX<T> *ctx = GetCTX((char *)&rp);
	  // copy response
	  memcpy(&ctx->resp, &rp, sizeof(rp));
	  ctx->dropped = false;

	  // wake up the waiter
	  ctx->waiter->Done();
	}
      }).Detach();
    }
  }
}

// Send(): Send the request to downstream
template <typename T, int ID_OFF>
LBCTX<T> *LoadBalancer<T, ID_OFF>::Send(const void *buf, size_t len, int hash) {
  // Create downstream context
  LBCTX<T> *ctx = new LBCTX<T>();
  T *req = &ctx->req;

  // copy request
  memcpy(req, buf, len);

  // Downstream request ID is the pointer address of LBCTX (overriden)
  *(uint64_t *)((char *)req + ID_OFF) = reinterpret_cast<uint64_t>(ctx);

  // Find a client to send a request
  int idx = next_cidx_;
  for (int i = 0; i < nclients_; ++i) {
    if (clients_[idx]->Credit() > 0) break;
    idx = (idx + 1) % nclients_;
  }

  next_cidx_ = (idx + 1) % nclients_;

  // Send the request
  clients_[idx]->Send(req, len, hash, nullptr);

  return ctx;
}

// Credit(): Returns downstream credit
// TODO: Better to optimize this function. Instaed of sum of windows
// over the loop, we can maintain a single atomic variable.
template <typename T, int ID_OFF>
uint32_t LoadBalancer<T, ID_OFF>::Credit() {
  uint32_t ds_win = 0;

  for(int i = 0; i < nclients_; ++i) {
    ds_win += clients_[i]->Credit();
  }

  return ds_win;
}

template <typename T, int ID_OFF>
LBCTX<T> *LoadBalancer<T, ID_OFF>::GetCTX(char *buf) {
  T *rp = reinterpret_cast<T *>(buf);
  uint64_t req_id = *(uint64_t *)((const char *)rp + ID_OFF);
  LBCTX<T> *ctx = reinterpret_cast<LBCTX<T> *>(req_id);

  return ctx;
}
} // namespace rpc
