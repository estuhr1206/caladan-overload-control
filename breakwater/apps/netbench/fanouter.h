// fanouter.h - RPC downstream fanouter abstraction

#pragma once

#include "cc/sync.h"
#include "cc/thread.h"
#include "breakwater/rpc++.h"

template <typename T>
class FOCTX {
public:
  FOCTX(int _degree) {
    degree = _degree;
    resp = (T *)malloc(_degree * sizeof(T));
    dropped = (bool *)malloc(_degree * sizeof(bool));
    waiter = new rt::WaitGroup(_degree);
    num_resp = 0;
  }

  ~FOCTX() {
    delete waiter;
    free(resp);
    free(dropped);
  }

  void Wait() { waiter->Wait(); }
  void Done() { waiter->Done(); }
  int Degree() { return degree; }
  bool Dropped(int i) { return dropped[i]; }
  int NumResp() { return num_resp; }
  bool IsComplete() { return (num_resp == degree); }
  bool IsDropped() {
    for (int i = 0; i < num_resp; ++i) {
      if (dropped[i]) return true;
    }
    return false;
  }

  // request from upstream
  T req;
  // responses from downstream
  T *resp;
  // whether request has been dropped in the downstream
  bool *dropped;
  // waiter for synchronization
  rt::WaitGroup *waiter;
  // degree of fanout
  int degree;
  // the number of response available
  int num_resp;
};

namespace rpc {
// T: a request struct or class
// ID_OFF: offset to the request ID in T
// (request ID should be uint64)
template <typename T, int ID_OFF>
class FanOuter {
public:
  // Disable move and copy.
  FanOuter(const FanOuter&) = delete;
  FanOuter& operator=(const FanOuter&) = delete;

  // raddrs: array of remote servers
  // nserver: number of downstream servers to connect
  //          (=degree of fanout)
  // nclients: number of duplicate clients PER SERVER
  // nconns: number of connections for each server (scale in)
  FanOuter(netaddr *raddrs, int nserver, int nclients, int nconns,
	       crpc_ldrop_fn_t ldrop_handler, crpc_rdrop_fn_t rdrop_handler);

  // Send a request to downstream
  // buf: RPC request buffer
  // len: RPC request size
  // hash: (optional) hash for randomized operation
  // returns fanout context (FOCTX)
  FOCTX<T> *Send(const void *buf, size_t len, int hash);

  // Returns downstream window size
  uint32_t Credit();

  // Convert request/response buffer to FOCTX
  static FOCTX<T> *GetCTX(char *buf);

private:
  // array of pointers to clients
  RpcClient **clients_;
  // for iterating over multiple clients
  int *next_cidx_;
  // the number of clients
  int nclients_;
  // degree of fanout
  int degree_;
}; // class Fanouter

template <typename T, int ID_OFF>
FanOuter<T, ID_OFF>::FanOuter(netaddr *raddrs, int nserver,
		int nclients, int nconns, crpc_ldrop_fn_t ldrop_handler,
		crpc_rdrop_fn_t rdrop_handler) {
  // Initialize member variables
  nclients_ = nclients;
  degree_ = nserver;
  next_cidx_ = (int *)malloc(sizeof(int) * nserver);
  for (int i = 0; i < nserver; ++i)
    next_cidx_[i] = 0;
  clients_ = (RpcClient **)malloc(sizeof(RpcClient*) * nserver * nclients);

  int sidx, cidx;
  struct rpc_session_info info = {.session_type = 0};
  for (int i = 0; i < nserver * nclients; ++i) {
    // server index
    sidx = i / nclients;
    // client index
    cidx = i % nclients;

    // Dial to the first connection
    clients_[i] = RpcClient::Dial(raddrs[sidx], i + 1, ldrop_handler,
				  rdrop_handler, &info);

    // Add connection
    for (int j = 0; j < nconns - 1; ++j) {
      clients_[i]->AddConnection(raddrs[sidx]);
    }

    // Start the receiver thread for downstream
    for(int j = 0; j < clients_[i]->NumConns(); ++j) {
      rt::Thread([&, i, j] {
        T rp;
	int idx;

	while (true) {
	  ssize_t ret = clients_[i]->Recv(&rp, sizeof(rp), j, nullptr);
	  if (ret != static_cast<ssize_t>(sizeof(rp))) {
	    if (ret == 0 || ret < 0) break;
	    panic("read response from downstream failed, ret = %ld", ret);
	  }

	  FOCTX<T> *ctx = GetCTX((char *)&rp);
	  idx = ctx->num_resp++;

	  // copy response
	  memcpy(&ctx->resp[idx], &rp, sizeof(rp));
	  ctx->dropped[idx] = false;

	  if (unlikely(ctx->num_resp > ctx->degree))
	    panic("received more response than the level of degree.");

	  // wake up the waiter
	  ctx->waiter->Done();
	}
      }).Detach();
    }
  }
}

// Send(): Send the request to downstream
template <typename T, int ID_OFF>
FOCTX<T> *FanOuter<T, ID_OFF>::Send(const void *buf, size_t len, int hash) {
  // Create downstream context
  FOCTX<T> *ctx = new FOCTX<T>(degree_);
  T *req = &ctx->req;

  // copy request
  memcpy(req, buf, len);

  // Downstream request ID is the pointer address of FOCTX (overriden)
  *(uint64_t *)((char *)req + ID_OFF) = reinterpret_cast<uint64_t>(ctx);

  for (int i = 0; i < degree_; ++i) {
    // find a client to send a request
    int idx = next_cidx_[i];

    for (int j = 0; j < nclients_; ++j) {
      if (clients_[nclients_*i + idx]->Credit() > 0) break;
      idx = (idx + 1) % nclients_;
    }

    next_cidx_[i] = (idx + 1) % nclients_;

    // Send the request to the i-th server
    clients_[nclients_*i + idx]->Send(req, len, hash, reinterpret_cast<void *>(i));
  }

  return ctx;
}

// Credit(): Returns downstream credit
// TODO: Better to optimize this function. Instaed of sum of windows
// over the loop, we can maintain a single atomic variable.
template <typename T, int ID_OFF>
uint32_t FanOuter<T, ID_OFF>::Credit() {
  uint32_t ds_win = 0;
  uint32_t win;

  // Window of the first server
  for (int i = 0; i < nclients_; ++i) {
    ds_win += clients_[i]->Credit();
  }

  // Window of n-th server (n >= 2)
  for (int i = 1; i < degree_; ++i) {
    win = 0;
    for (int j = 0; j < nclients_; ++j) {
      win += clients_[i*nclients_ + j]->Credit();
    }
    ds_win = MIN(ds_win, win);
  }

  return ds_win;
}

template <typename T, int ID_OFF>
FOCTX<T> *FanOuter<T, ID_OFF>::GetCTX(char *buf) {
  T *rp = reinterpret_cast<T *>(buf);
  uint64_t req_id = *(uint64_t *)((const char *)rp + ID_OFF);
  FOCTX<T> *ctx = reinterpret_cast<FOCTX<T> *>(req_id);

  return ctx;
}
} // namespace rpc
