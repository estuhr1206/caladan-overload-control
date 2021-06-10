#include <breakwater/rpc++.h>

namespace rpc {

namespace {

std::function<void(struct srpc_ctx *)> handler;

void RpcServerTrampoline(struct srpc_ctx *arg) {
  handler(arg);
}

} // namespace

RpcClient *RpcClient::Dial(netaddr raddr, int id,
			   crpc_fn_t drop_handler) {
  crpc_session *s;
  raddr.port = SRPC_PORT;
  int ret = crpc_ops->crpc_open(raddr, &s, id, drop_handler);
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

ssize_t RpcClient::Recv(void *buf, size_t len, int conn_idx,
			bool *dropped) {
  return crpc_ops->crpc_recv_one(s_->c[conn_idx], buf, len, dropped);
}

int RpcClient::NumConns() {
  return s_->nconns;
}

uint32_t RpcClient::WinAvail() {
  return crpc_ops->crpc_win_avail(s_);
}

void RpcClient::StatClear() {
  return crpc_ops->crpc_stat_clear(s_);
}

uint64_t RpcClient::StatWinuRx() {
  return crpc_ops->crpc_stat_winu_rx(s_);
}

uint64_t RpcClient::StatWinuTx() {
  return crpc_ops->crpc_stat_winu_tx(s_);
}

uint64_t RpcClient::StatRespRx() {
  return crpc_ops->crpc_stat_resp_rx(s_);
}

uint64_t RpcClient::StatReqTx() {
  return crpc_ops->crpc_stat_req_tx(s_);
}

uint64_t RpcClient::StatWinExpired() {
  return crpc_ops->crpc_stat_win_expired(s_);
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

uint64_t RpcServerStatWinuRx() {
  return srpc_ops->srpc_stat_winu_rx();
}

uint64_t RpcServerStatWinuTx() {
  return srpc_ops->srpc_stat_winu_tx();
}

uint64_t RpcServerStatWinTx() {
  return srpc_ops->srpc_stat_win_tx();
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
