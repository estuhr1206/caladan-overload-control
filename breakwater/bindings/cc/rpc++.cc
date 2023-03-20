#include <breakwater/rpc++.h>

namespace rpc {

namespace {

std::function<void(struct srpc_ctx *)> handler;

void RpcServerTrampoline(struct srpc_ctx *arg) {
  handler(arg);
}

} // namespace

RpcClient *RpcClient::Dial(netaddr raddr, int id,
			   crpc_ldrop_fn_t ldrop_handler,
			   crpc_rdrop_fn_t rdrop_handler,
			   struct rpc_session_info *info) {
  crpc_session *s;
  raddr.port = SRPC_PORT;
  int ret = crpc_ops->crpc_open(raddr, &s, id, ldrop_handler,
				rdrop_handler, info);
  if (ret) return nullptr;
  return new RpcClient(s);
}

int RpcClient::AddConnection(netaddr raddr) {
  raddr.port = SRPC_PORT;
  return crpc_ops->crpc_add_connection(s_, raddr);
}

ssize_t RpcClient::Send(const void *buf, size_t len, int hash, void *arg) {
  return crpc_ops->crpc_send_one(s_, buf, len, hash, arg);
}

ssize_t RpcClient::Recv(void *buf, size_t len, int conn_idx, void *arg) {
  return crpc_ops->crpc_recv_one(s_->c[conn_idx], buf, len, arg);
}

int RpcClient::NumConns() {
  return s_->nconns;
}

uint32_t RpcClient::Credit() {
  return crpc_ops->crpc_credit(s_);
}

void RpcClient::StatClear() {
  return crpc_ops->crpc_stat_clear(s_);
}

uint64_t RpcClient::StatEcreditRx() {
  return crpc_ops->crpc_stat_ecredit_rx(s_);
}

uint64_t RpcClient::StatCupdateTx() {
  return crpc_ops->crpc_stat_cupdate_tx(s_);
}

uint64_t RpcClient::StatRespRx() {
  return crpc_ops->crpc_stat_resp_rx(s_);
}

uint64_t RpcClient::StatReqTx() {
  return crpc_ops->crpc_stat_req_tx(s_);
}

uint64_t RpcClient::StatCreditExpired() {
  return crpc_ops->crpc_stat_credit_expired(s_);
}

uint64_t RpcClient::StatReqDropped() {
  return crpc_ops->crpc_stat_req_dropped(s_);
}

int RpcClient::Shutdown(int how) {
  int ret = 0;

  for(int i = 0; i < s_->nconns; ++i) {
    ret |= tcp_shutdown(s_->c[i]->c, how);
  }

  return ret;
}

void RpcClient::Abort() {
  for(int i = 0; i < s_->nconns; ++i) {
    tcp_abort(s_->c[i]->c);
  }
}

void RpcClient::Close() {
  crpc_ops->crpc_close(s_);
}

int RpcServerEnable(std::function<void(struct srpc_ctx *)> f) {
  handler = f;
  int ret = srpc_ops->srpc_enable(RpcServerTrampoline);
  BUG_ON(ret == -EBUSY);
  return ret;
}

uint64_t RpcServerStatCupdateRx() {
  return srpc_ops->srpc_stat_cupdate_rx();
}

uint64_t RpcServerStatEcreditTx() {
  return srpc_ops->srpc_stat_ecredit_tx();
}

uint64_t RpcServerStatCreditTx() {
  return srpc_ops->srpc_stat_credit_tx();
}

uint64_t RpcServerStatReqRx() {
  return srpc_ops->srpc_stat_req_rx();
}

uint64_t RpcServerStatReqDropped() {
  return srpc_ops->srpc_stat_req_dropped();
}

uint64_t RpcServerStatRespTx() {
  return srpc_ops->srpc_stat_resp_tx();
}

} // namespace rpc
