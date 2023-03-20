/*
 * RPC client-side support
 */

#include <time.h>
#include <stdio.h>
#include <base/time.h>
#include <base/stddef.h>
#include <base/list.h>
#include <base/log.h>
#include <base/atomic.h>
#include <runtime/smalloc.h>
#include <runtime/sync.h>
#include <runtime/timer.h>

#include <breakwater/dagor.h>

#include "util.h"
#include "dg_proto.h"
#include "dg_config.h"

#define CDG_TRACK_FLOW			false
#define CDG_TRACK_FLOW_ID		1

static ssize_t crpc_send_request_vector(struct cdg_conn *cc)
{
	struct cdg_hdr chdr[CRPC_QLEN];
	struct iovec v[CRPC_QLEN * 2];
	int nriov = 0;
	int nrhdr = 0;
	ssize_t ret;
	uint64_t now = microtime();

	assert_mutex_held(&cc->lock);

	while (cc->head != cc->tail) {
		struct cdg_ctx *c = cc->qreq[cc->tail++ % CRPC_QLEN];

		chdr[nrhdr].magic = DG_REQ_MAGIC;
		chdr[nrhdr].op = DG_OP_CALL;
		chdr[nrhdr].id = c->cmn.id;
		chdr[nrhdr].len = c->cmn.len;
		chdr[nrhdr].prio = c->prio;
		chdr[nrhdr].ts_sent = now;

		v[nriov].iov_base = &chdr[nrhdr];
		v[nriov].iov_len = sizeof(struct cdg_hdr);
		nrhdr++;
		nriov++;

		if (c->cmn.len > 0) {
			v[nriov].iov_base = c->cmn.buf;
			v[nriov++].iov_len = c->cmn.len;
		}
	}

	if (nriov == 0)
		return 0;
	ret = tcp_writev_full(cc->cmn.c, v, nriov);

	cc->req_tx_ += nrhdr;

	cc->head = 0;
	cc->tail = 0;

	if (unlikely(ret < 0))
		return ret;
	return 0;
}

static bool crpc_enqueue_one(struct cdg_session *s,
			     const void *buf, size_t len, int hash)
{
	int pos;
	struct cdg_ctx *c;
	uint64_t now = microtime();
	int prio = hash % DG_MAX_PRIO;
	int conn_idx;
	struct cdg_conn *cc;
	int i;

	assert_mutex_held(&s->lock);

	/* choose the connection */
	for(i = 0; i < s->cmn.nconns; ++i) {
		conn_idx = (s->next_conn_idx + i) % s->cmn.nconns;
		cc = (struct cdg_conn *)s->cmn.c[conn_idx];
		if (cc->head - cc->tail < CRPC_QLEN && prio <= cc->local_prio) {
			s->next_conn_idx = (conn_idx + 1) % s->cmn.nconns;
			break;
		}
		cc = NULL;
	}

	/* every connection is busy */
	if (!cc) {
		s->req_dropped_++;
		return false;
	}

	mutex_lock(&cc->lock);

	if (cc->head - cc->tail >= CRPC_QLEN || prio > cc->local_prio) {
		s->req_dropped_++;
		mutex_unlock(&cc->lock);
		return false;
	}

	pos = cc->head++ % CRPC_QLEN;
	c = cc->qreq[pos];
	memcpy(c->cmn.buf, buf, len);
	c->cmn.id = s->req_id++;
	c->cmn.len = len;
	c->cmn.ts = now;
	c->prio = prio;

	if (cc->head - cc->tail == 1)
		condvar_signal(&cc->sender_cv);

	mutex_unlock(&cc->lock);

	return true;
}

static void crpc_sender(void *arg)
{
	struct cdg_conn *cc = (struct cdg_conn *)arg;

	mutex_lock(&cc->lock);
	while(true) {
		while (cc->running && cc->head == cc->tail)
			condvar_wait(&cc->sender_cv, &cc->lock);

		if (!cc->running)
			goto done;

		// Wait for batching
		if (cc->head - cc->tail < CRPC_QLEN) {
			mutex_unlock(&cc->lock);
			timer_sleep(CDG_BATCH_WAIT_US);
			mutex_lock(&cc->lock);
		}

		// Batch sending
		crpc_send_request_vector(cc);
	}

done:
	mutex_unlock(&cc->lock);
	waitgroup_done(&cc->sender_waiter);
}

int cdg_add_connection(struct crpc_session *s_, struct netaddr raddr)
{
	struct cdg_session *s = (struct cdg_session *)s_;
	struct netaddr laddr;
	struct cdg_conn *cc;
	tcpconn_t *c;
	int ret, i;

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
	if (!cc) {
		tcp_close(c);
		return -ENOMEM;
	}
	memset(cc, 0, sizeof(*cc));

	for(i = 0; i < CRPC_QLEN; ++i) {
		cc->qreq[i] = smalloc(sizeof(struct cdg_ctx));
		if (!cc->qreq[i])
			goto fail;
	}

	/* init conn */
	cc->cmn.c = c;
	cc->local_prio = rand() % 128;
	cc->running = true;
	cc->session = s;

	mutex_init(&cc->lock);
	condvar_init(&cc->sender_cv);
	waitgroup_init(&cc->sender_waiter);
	waitgroup_add(&cc->sender_waiter, 1);

	/* update session */
	mutex_lock(&s->lock);
	s->cmn.c[s->cmn.nconns++] = (struct crpc_conn *)cc;
	mutex_unlock(&s->lock);

	/* spawn sender thread */
	ret = thread_spawn(crpc_sender, cc);
	BUG_ON(ret);

	return 0;
fail:
	tcp_close(c);
	for (i = i - 1; i >= 0; i--)
		sfree(cc->qreq[i]);
	sfree(cc);
	return -ENOMEM;
}

ssize_t cdg_send_one(struct crpc_session *s_,
		      const void *buf, size_t len, int hash, void *arg)
{
	struct cdg_session *s = (struct cdg_session *)s_;

	/* implementation is currently limited to a maximum payload size */
	if (unlikely(len > SRPC_BUF_SIZE))
		return -E2BIG;

	mutex_lock(&s->lock);

	/* hot path, just send */
	crpc_enqueue_one(s, buf, len, hash);
	mutex_unlock(&s->lock);

	return len;
}

ssize_t cdg_recv_one(struct crpc_conn *cc_, void *buf, size_t len, void *arg)
{
	struct cdg_conn *cc = (struct cdg_conn *)cc_;
	struct cdg_session *s = cc->session;
	struct sdg_hdr shdr;
	ssize_t ret;

again:

	/* read the server header */
	ret = tcp_read_full(cc->cmn.c, &shdr, sizeof(shdr));
	if (unlikely(ret <= 0))
		return ret;
	assert(ret == sizeof(shdr));

	/* parse the server header */
	if (unlikely(shdr.magic != DG_RESP_MAGIC)) {
		log_warn("crpc: got invalid magic %x", shdr.magic);
		return -EINVAL;
	}
	if (unlikely(shdr.len > MIN(SRPC_BUF_SIZE, len))) {
		log_warn("crpc: request len %ld too large (limit %ld)",
			 shdr.len, MIN(SRPC_BUF_SIZE, len));
		return -EINVAL;
	}

	switch (shdr.op) {
	case DG_OP_CALL:
		/* read the payload */
		if (shdr.len > 0) {
			ret = tcp_read_full(cc->cmn.c, buf, shdr.len);
			if (unlikely(ret <= 0))
				return ret;
			assert(ret == shdr.len);
			cc->resp_rx_++;
		}

		mutex_lock(&cc->lock);
		cc->local_prio = shdr.prio;
		mutex_unlock(&cc->lock);

		if (shdr.flags & DG_SFLAG_DROP) {
			if (s->cmn.rdrop_handler)
				s->cmn.rdrop_handler(buf, ret, arg);
			goto again;
		}

#if CDG_TRACK_FLOW
		if (s->id == CDG_TRACK_FLOW_ID) {
			printf("[%lu] ===> response: id=%lu, prio=%d\n",
			       microtime(), shdr.id, shdr.prio);
		}
#endif

		break;
	default:
		log_warn("crpc: got invalid op %d", shdr.op);
		return -EINVAL;
	}

	return shdr.len;
}

int cdg_open(struct netaddr raddr, struct crpc_session **sout, int id,
	     crpc_ldrop_fn_t ldrop_handler, crpc_rdrop_fn_t rdrop_handler,
	     struct rpc_session_info *info)
{
	struct netaddr laddr;
	struct cdg_session *s;
	struct cdg_conn *cc;
	tcpconn_t *c;
	int i, ret;

	srand(time(NULL));

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
	if (!cc){
		tcp_close(c);
		sfree(s);
		return -ENOMEM;
	}
	memset(cc, 0, sizeof(*cc));

	for (i = 0; i < CRPC_QLEN; ++i) {
		cc->qreq[i] = smalloc(sizeof(struct cdg_ctx));
		if (!cc->qreq[i])
			goto fail;
	}

	/* init conn */
	cc->cmn.c = c;
	cc->local_prio = rand() % 128;
	cc->running = true;
	cc->session = s;

	mutex_init(&cc->lock);
	condvar_init(&cc->sender_cv);
	waitgroup_init(&cc->sender_waiter);
	waitgroup_add(&cc->sender_waiter, 1);

	/* init session */
	s->cmn.nconns = 1;
	s->cmn.c[0] = (struct crpc_conn *)cc;
	s->cmn.session_type = info->session_type;
	s->id = id;
	s->req_id = 1;
	mutex_init(&s->lock);

	*sout = (struct crpc_session *)s;

	/* spawn sender thread */
	ret = thread_spawn(crpc_sender, cc);
	BUG_ON(ret);

	return 0;

fail:
	tcp_close(c);
	for (i = i - 1; i >= 0; i--)
		sfree(cc->qreq[i]);
	sfree(cc);
	sfree(s);
	return -ENOMEM;
}

void cdg_close(struct crpc_session *s_)
{
	struct cdg_session *s = (struct cdg_session *)s_;
	struct cdg_conn *cc;
	int i, j;

	/* terminate client and wait for sender thread */
	for (i = 0; i < s->cmn.nconns; ++i) {
		cc = (struct cdg_conn *)s->cmn.c[i];
		mutex_lock(&cc->lock);
		cc->running = false;
		condvar_signal(&cc->sender_cv);
		mutex_unlock(&cc->lock);
	}
	for (i = 0; i < s->cmn.nconns; ++i) {
		cc = (struct cdg_conn *)s->cmn.c[i];
		waitgroup_wait(&cc->sender_waiter);
	}

	/* free resources */
	for (i = 0; i < s->cmn.nconns; ++i) {
		cc = (struct cdg_conn *)s->cmn.c[i];
		tcp_close(cc->cmn.c);
		for (j = 0; j < CRPC_QLEN; ++j)
			sfree(cc->qreq[j]);
		sfree(cc);
	}
	sfree(s);
}

/* client-side stats */
uint32_t cdg_credit(struct crpc_session *s_)
{
	return 0;
}

void cdg_stat_clear(struct crpc_session *s_)
{
	return;
}

uint64_t cdg_stat_credit_expired(struct crpc_session *s_)
{
	return 0;
}

uint64_t cdg_stat_ecredit_rx(struct crpc_session *s_)
{
	return 0;
}

uint64_t cdg_stat_cupdate_tx(struct crpc_session *s_)
{
	return 0;
}

uint64_t cdg_conn_stat_resp_rx(struct crpc_conn *cc_)
{
	struct cdg_conn *cc = (struct cdg_conn *)cc_;
	return cc->resp_rx_;
}

uint64_t cdg_sess_stat_resp_rx(struct crpc_session *s_)
{
	struct cdg_session *s = (struct cdg_session *)s_;
	uint64_t ret = 0;

	for(int i = 0; i < s->cmn.nconns; ++i) {
		ret += cdg_conn_stat_resp_rx(s->cmn.c[i]);
	}

	return ret;
}

uint64_t cdg_conn_stat_req_tx(struct crpc_conn *cc_)
{
	struct cdg_conn *cc = (struct cdg_conn *)cc_;
	return cc->req_tx_;
}

uint64_t cdg_sess_stat_req_tx(struct crpc_session *s_)
{
	struct cdg_session *s = (struct cdg_session *)s_;
	uint64_t ret = 0;

	for(int i = 0; i < s->cmn.nconns; ++i) {
		ret += cdg_conn_stat_req_tx(s->cmn.c[i]);
	}

	return ret;
}

uint64_t cdg_stat_req_dropped(struct crpc_session *s_)
{
	struct cdg_session *s = (struct cdg_session *)s_;
	return s->req_dropped_;
}

struct crpc_ops cdg_ops = {
	.crpc_add_connection		= cdg_add_connection,
	.crpc_send_one			= cdg_send_one,
	.crpc_recv_one			= cdg_recv_one,
	.crpc_open			= cdg_open,
	.crpc_close			= cdg_close,
	.crpc_credit			= cdg_credit,
	.crpc_stat_clear		= cdg_stat_clear,
	.crpc_stat_ecredit_rx		= cdg_stat_ecredit_rx,
	.crpc_stat_credit_expired	= cdg_stat_credit_expired,
	.crpc_stat_cupdate_tx		= cdg_stat_cupdate_tx,
	.crpc_stat_resp_rx		= cdg_sess_stat_resp_rx,
	.crpc_stat_req_tx		= cdg_sess_stat_req_tx,
	.crpc_stat_req_dropped		= cdg_stat_req_dropped,
};
