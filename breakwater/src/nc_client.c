/*
 * RPC client-side support
 */

#include <stdio.h>
#include <base/time.h>
#include <base/stddef.h>
#include <base/list.h>
#include <base/log.h>
#include <base/atomic.h>
#include <runtime/smalloc.h>
#include <runtime/sync.h>
#include <runtime/timer.h>

#include <breakwater/nocontrol.h>

#include "util.h"
#include "nc_proto.h"
#include "nc_config.h"

#define CNC_TRACK_FLOW			false
#define CNC_TRACK_FLOW_ID		1

static ssize_t crpc_send_raw(struct cnc_conn *cc,
			     const void *buf, size_t len,
			     uint64_t id)
{
	struct iovec vec[2];
	struct cnc_hdr chdr;
	ssize_t ret;

	/* header */
	chdr.magic = NC_REQ_MAGIC;
	chdr.op = NC_OP_CALL;
	chdr.id = id;
	chdr.len = len;
	chdr.ts = microtime();

	/* SG vector */
	vec[0].iov_base = &chdr;
	vec[0].iov_len = sizeof(chdr);
	vec[1].iov_base = (void *)buf;
	vec[1].iov_len = len;

	/* send the request */
	ret = tcp_writev_full(cc->cmn.c, vec, 2);
	if (unlikely(ret < 0))
		return ret;
	assert(ret == sizeof(chdr) + len);
	cc->req_tx_++;

	return len;
}

int cnc_add_connection(struct crpc_session *s_, struct netaddr raddr)
{
	struct cnc_session *s = (struct cnc_session *)s_;
	struct netaddr laddr;
	struct cnc_conn *cc;
	tcpconn_t *c;
	int ret;

	if (s->cmn.nconns >= CRPC_MAX_REPLICA)
		return -ENOMEM;

	/* set up ephemeral IP and port */
	laddr.ip = 0;
	laddr.port = 0;

	if (raddr.port != SRPC_PORT)
		return -EINVAL;

	/* dial */
	ret = tcp_dial(laddr, raddr, &c);
	if (ret)
		return ret;

	/* alloc conn */
	cc = smalloc(sizeof(*cc));
	if (!cc)
		goto fail;
	memset(cc, 0, sizeof(*cc));

	/* init conn */
	cc->cmn.c = c;
	cc->session = s;

	/* update session */
	mutex_lock(&s->lock);
	s->cmn.c[s->cmn.nconns++] = (struct crpc_conn *)cc;
	mutex_unlock(&s->lock);

	return 0;
fail:
	tcp_close(c);
	return -ENOMEM;
}

ssize_t cnc_send_one(struct crpc_session *s_,
		     const void *buf, size_t len, int hash, void *arg)
{
	struct cnc_session *s = (struct cnc_session *)s_;
	struct cnc_conn *cc;
	ssize_t ret;
	int conn_idx;

	/* implementation is currently limited to a maximum payload size */
	if (unlikely(len > SRPC_BUF_SIZE))
		return -E2BIG;

	mutex_lock(&s->lock);

	switch (CNC_LB_POLICY) {
	case CNC_LB_RR:
		conn_idx = s->next_conn_idx;
		s->next_conn_idx = (s->next_conn_idx + 1) % s->cmn.nconns;
		break;
	case CNC_LB_RAND:
		conn_idx = hash % s->cmn.nconns;
		break;
	default:
		log_warn("crpc: invalid CNC_LB_POLICY");
		return -EINVAL;
	}

	/* send request */
	cc = (struct cnc_conn *)s->cmn.c[conn_idx];
	ret = crpc_send_raw(cc, buf, len, s->req_id++);
	mutex_unlock(&s->lock);

	return ret;
}

ssize_t cnc_recv_one(struct crpc_conn *cc_, void *buf, size_t len, void *arg)
{
	struct cnc_conn *cc = (struct cnc_conn *)cc_;
	struct snc_hdr shdr;
	ssize_t ret;

again:
	/* read the server header */
	ret = tcp_read_full(cc->cmn.c, &shdr, sizeof(shdr));
	if (unlikely(ret <= 0))
		return ret;
	assert(ret == sizeof(shdr));

	/* parse the server header */
	if (unlikely(shdr.magic != NC_RESP_MAGIC)) {
		log_warn("crpc: got invalid magic %x", shdr.magic);
		return -EINVAL;
	}
	if (unlikely(shdr.len > MIN(SRPC_BUF_SIZE, len))) {
		log_warn("crpc: request len %ld too large (limit %ld)",
			 shdr.len, MIN(SRPC_BUF_SIZE, len));
		return -EINVAL;
	}

	switch (shdr.op) {
	case NC_OP_CALL:
		if (shdr.len == 0)
			goto again;

		/* read the payload */
		ret = tcp_read_full(cc->cmn.c, buf, shdr.len);
		if (unlikely(ret <= 0))
			return ret;
		assert(ret == shdr.len);
		cc->resp_rx_++;

#if CNC_TRACK_FLOW
		if (cc->session->id == CNC_TRACK_FLOW_ID) {
			printf("[%lu] ===> response: id=%lu\n",
			       microtime(), shdr.id);
		}
#endif
		break;
	default:
		log_warn("crpc: got invalid op %d", shdr.op);
		return -EINVAL;
	}

	return shdr.len;
}

int cnc_open(struct netaddr raddr, struct crpc_session **sout, int id,
	     crpc_ldrop_fn_t ldrop_handler, crpc_rdrop_fn_t rdrop_handler,
	     struct rpc_session_info *info)
{
	struct netaddr laddr;
	struct cnc_session *s;
	struct cnc_conn *cc;
	tcpconn_t *c;
	int ret;

	/* set up ephemeral IP and port */
	laddr.ip = 0;
	laddr.port = 0;

	if (raddr.port != SRPC_PORT)
		return -EINVAL;

	/* dial */
	ret = tcp_dial(laddr, raddr, &c);
	if (ret)
		return ret;

	/* send session info */
	ret = tcp_write_full(c, info, sizeof(*info));
	if (unlikely(ret < 0))
		return ret;

	/* alloc session */
	s = smalloc(sizeof(*s));
	if (!s) {
		tcp_close(c);
		return -ENOMEM;
	}
	memset(s, 0, sizeof(*s));

	/* alloc conn */
	cc = smalloc(sizeof(*cc));
	if (!cc) {
		goto fail;
	}
	memset(cc, 0, sizeof(*cc));

	/* init conn */
	cc->cmn.c = c;
	cc->session = s;

	/* init session */
	s->cmn.nconns = 1;
	s->cmn.c[0] = (struct crpc_conn *)cc;
	s->cmn.session_type = info->session_type;
	s->id = id;
	s->req_id = 1;

	mutex_init(&s->lock);

	*sout = (struct crpc_session *)s;

	return 0;
fail:
	tcp_close(c);
	sfree(s);
	return -ENOMEM;
}

void cnc_close(struct crpc_session *s_)
{
	struct cnc_session *s = (struct cnc_session *)s_;
	int i;

	/* free resources */
	for (i = 0; i < s->cmn.nconns; ++i) {
		tcp_close(s->cmn.c[i]->c);
		sfree(s->cmn.c[i]);
	}
	sfree(s);
}

/* client stats */
uint32_t cnc_credit(struct crpc_session *s_)
{
	return 0;
}

void cnc_stat_clear(struct crpc_session *s_)
{
	return;
}

uint64_t cnc_stat_credit_expired(struct crpc_session *s_)
{
	return 0;
}

uint64_t cnc_stat_ecredit_rx(struct crpc_session *s_)
{
	return 0;
}

uint64_t cnc_stat_cupdate_tx(struct crpc_session *s_)
{
	return 0;
}

uint64_t cnc_conn_stat_resp_rx(struct crpc_conn *cc_)
{
	struct cnc_conn *cc = (struct cnc_conn *)cc_;
	return cc->resp_rx_;
}

uint64_t cnc_sess_stat_resp_rx(struct crpc_session *s_)
{
	struct cnc_session *s = (struct cnc_session *)s_;
	uint64_t ret = 0;

	for(int i = 0; i < s->cmn.nconns; ++i) {
		ret += cnc_conn_stat_resp_rx(s->cmn.c[i]);
	}

	return ret;
}

uint64_t cnc_conn_stat_req_tx(struct crpc_conn *cc_)
{
	struct cnc_conn *cc = (struct cnc_conn *)cc_;
	return cc->req_tx_;
}

uint64_t cnc_sess_stat_req_tx(struct crpc_session *s_)
{
	struct cnc_session *s = (struct cnc_session *)s_;
	uint64_t ret = 0;

	for(int i = 0; i < s->cmn.nconns; ++i) {
		ret += cnc_conn_stat_req_tx(s->cmn.c[i]);
	}

	return ret;
}

uint64_t cnc_stat_req_dropped(struct crpc_session *s_)
{
	return 0;
}

struct crpc_ops cnc_ops = {
	.crpc_add_connection		= cnc_add_connection,
	.crpc_send_one			= cnc_send_one,
	.crpc_recv_one			= cnc_recv_one,
	.crpc_open			= cnc_open,
	.crpc_close			= cnc_close,
	.crpc_credit			= cnc_credit,
	.crpc_stat_clear		= cnc_stat_clear,
	.crpc_stat_ecredit_rx		= cnc_stat_ecredit_rx,
	.crpc_stat_credit_expired	= cnc_stat_credit_expired,
	.crpc_stat_cupdate_tx		= cnc_stat_cupdate_tx,
	.crpc_stat_resp_rx		= cnc_sess_stat_resp_rx,
	.crpc_stat_req_tx		= cnc_sess_stat_req_tx,
	.crpc_stat_req_dropped		= cnc_stat_req_dropped,
};
