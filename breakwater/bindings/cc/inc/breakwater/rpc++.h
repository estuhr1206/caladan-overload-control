// rpc++.h - support for remote procedure calls (RPCs)

#pragma once

extern "C" {
#include <base/stddef.h>
#include <breakwater/rpc.h>
}

#include <functional>

namespace rpc {

class RpcClient {
 public:
  // The maximum size of an RPC request payload.
  static constexpr size_t kMaxPayloadSize = SRPC_BUF_SIZE;

  // Disable move and copy.
  RpcClient(const RpcClient&) = delete;
  RpcClient& operator=(const RpcClient&) = delete;

  // Creates an RPC session.
  static RpcClient *Dial(netaddr raddr, int id,
			 crpc_ldrop_fn_t ldrop_handler,
			 crpc_rdrop_fn_t rdrop_handler,
			 struct rpc_session_info *info);

  int AddConnection(netaddr raddr);

  // Sends an RPC request.
  ssize_t Send(const void *buf, size_t len, int hash,
	       void *arg = nullptr);

  // Receives an RPC request.
  ssize_t Recv(void *buf, size_t len, int conn_idx = 0,
	       void *arg = nullptr);

  int NumConns();

  uint32_t Credit();

  void StatClear();

  uint64_t StatEcreditRx();

  uint64_t StatCupdateTx();

  uint64_t StatRespRx();

  uint64_t StatReqTx();

  uint64_t StatCreditExpired();

  uint64_t StatReqDropped();

  // Shuts down the RPC connection.
  int Shutdown(int how);
  // Aborts the RPC connection.
  void Abort();

  void Close();

 private:
  RpcClient(struct crpc_session *s) : s_(s) { }

  // The client session object.
  struct crpc_session *s_;
};

// Enables the RPC server, listening for new sessions.
// Can only be called once.
int RpcServerEnable(std::function<void(struct srpc_ctx *)> f);

uint64_t RpcServerStatCupdateRx();
uint64_t RpcServerStatEcreditTx();
uint64_t RpcServerStatCreditTx();
uint64_t RpcServerStatReqRx();
uint64_t RpcServerStatReqDropped();
uint64_t RpcServerStatRespTx();
} // namespace rpc
