/*
 * RPC client-side support
 */

#include <base/time.h>
#include <base/stddef.h>
#include <base/list.h>
#include <base/log.h>
#include <base/atomic.h>
#include <runtime/smalloc.h>
#include <runtime/sync.h>
#include <runtime/timer.h>

#include <breakwater/breakwater.h>

#include "util.h"
#include "bw_proto.h"
#include "bw_config.h"

#define CBW_TRACK_FLOW			false
#define CBW_TRACK_FLOW_ID		1

/**
 * crpc_send_cupdate - send CREDIT message to update credit
 * @s: the RPC session to update the credit
 *
 * On success, returns 0. On failure returns standard socket errors (< 0)
 */
static ssize_t crpc_send_cupdate(struct cbw_conn *cc)
{
	struct cbw_session *s = cc->session;
        struct cbw_hdr chdr;
        ssize_t ret;

	assert_mutex_held(&s->lock);

	/* construct the client header */
	chdr.magic = BW_REQ_MAGIC;
	chdr.op = BW_OP_CREDIT;
	chdr.id = 0;
	chdr.len = 0;
	chdr.demand = s->head - s->tail;
	chdr.flags = 0;

	/* send the request */
	ret = tcp_write_full(cc->cmn.c, &chdr, sizeof(chdr));
	if (unlikely(ret < 0))
		return ret;

	assert(ret == sizeof(chdr));
	cc->cupdate_tx_++;

#if CBW_TRACK_FLOW
	if (cc->session->id == CBW_TRACK_FLOW_ID) {
		printf("[%lu] <=== credit_update: demand = %lu, credit = %u/%u\n",
		       microtime(), chdr.demand, cc->credit_used, cc->credit);
	}
#endif
	return 0;
}

static ssize_t crpc_send_request_vector(struct cbw_conn *cc)
{
	struct cbw_session *s = cc->session;
	struct cbw_hdr chdr[CRPC_QLEN];
	struct iovec vec[CRPC_QLEN * 2];
	int nriov = 0;
	int nrhdr = 0;
	ssize_t ret;
	uint64_t now = microtime();

	assert_mutex_held(&s->lock);

	/* queue is empty or no available credits */
	if (s->head == s->tail || cc->credit_used >= cc->credit)
		return 0;

	/* while queue is not empty and there is available credits */
	while (s->head != s->tail && cc->credit_used < cc->credit) {
		struct crpc_ctx *c = s->qreq[s->tail++ % CRPC_QLEN];

		chdr[nrhdr].magic = BW_REQ_MAGIC;
		chdr[nrhdr].op = BW_OP_CALL;
		chdr[nrhdr].id = c->id;
		chdr[nrhdr].len = c->len;
		chdr[nrhdr].demand = s->head - s->tail;
		chdr[nrhdr].ts_sent = now;
		chdr[nrhdr].flags = 0;

		vec[nriov].iov_base = &chdr[nrhdr];
		vec[nriov].iov_len = sizeof(struct cbw_hdr);
		nrhdr++;
		nriov++;

		if (c->len > 0) {
			vec[nriov].iov_base = c->buf;
			vec[nriov++].iov_len = c->len;
		}

		cc->credit_used++;
	}

	if (s->head == s->tail) {
		s->head = 0;
		s->tail = 0;
	}

	ret = tcp_writev_full(cc->cmn.c, vec, nriov);

	cc->req_tx_ += nrhdr;

#if CBW_TRACK_FLOW
	if (s->id == CBW_TRACK_FLOW_ID) {
		printf("[%lu] <=== request (%d): qlen=%d credit=%d/%d\n",
		       microtime(), nrhdr, s->head-s->tail, cc->credit_used, cc->credit);
	}
#endif

	if (unlikely(ret < 0))
		return ret;
	return 0;
}

static ssize_t crpc_send_raw(struct cbw_conn *cc,
			     const void *buf, size_t len,
			     uint64_t id)
{
	struct cbw_session *s = cc->session;
	struct iovec vec[2];
	struct cbw_hdr chdr;
	ssize_t ret;
	uint64_t now = microtime();

	assert_mutex_held(&s->lock);

	/* initialize the header */
	chdr.magic = BW_REQ_MAGIC;
	chdr.op = BW_OP_CALL;
	chdr.id = id;
	chdr.len = len;
	chdr.demand = s->head - s->tail;
	chdr.ts_sent = now;
	chdr.flags = 0;

	/* initialize the SG vector */
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

#if CBW_TRACK_FLOW
	if (s->id == CBW_TRACK_FLOW_ID) {
		printf("[%lu] <=== request: id=%lu, demand = %lu, credit = %u/%u\n",
		       now, chdr.id, chdr.demand, cc->credit_used, cc->credit);
	}
#endif
	return len;
}

static void crpc_drain_queue(struct cbw_session *s)
{
	int pos;
	struct crpc_ctx *c;
	uint64_t now = microtime();
	int conn_idx;
	struct cbw_conn *cc;
	int i;

	assert_mutex_held(&s->lock);

	/* If queue is empty */
	if (s->head == s->tail)
		return;

	/* choose request to send from request queue */
	while (s->head != s->tail) {
		pos = s->tail % CRPC_QLEN;
		c = s->qreq[pos];
		if (CBW_MAX_CLIENT_DELAY_US == 0 ||
		    now - c->ts <= CBW_MAX_CLIENT_DELAY_US)
			break;

		if (s->cmn.ldrop_handler)
			s->cmn.ldrop_handler(c);
		s->tail++;
		s->req_dropped_++;
#if CBW_TRACK_FLOW
		if (s->id == CBW_TRACK_FLOW_ID) {
			printf("[%lu] request dropped: id=%lu, qlen = %d\n",
			       now, c->id, s->head - s->tail);
		}
#endif
	}

	/* find the connection to send */
	for (i = 0; i < s->cmn.nconns; ++i) {
		conn_idx = (s->next_conn_idx + i) % s->cmn.nconns;
		cc = (struct cbw_conn *)s->cmn.c[conn_idx];

		/* (1) not waiting for the first response
		 * (2) have available credit
		 */
		if (!cc->waiting_resp &&
		    cc->credit_used < cc->credit) {
			crpc_send_request_vector(cc);
			s->next_conn_idx = (conn_idx + 1) % s->cmn.nconns;
			break;
		}
	}
}

static bool crpc_enqueue_one(struct cbw_session *s,
			     const void *buf, size_t len, void *arg)
{
	int pos;
	struct crpc_ctx *c;
	uint64_t now = microtime();
	struct cbw_conn *cc;

	assert_mutex_held(&s->lock);

	/* if the queue is full, drop tail */
	if (s->head - s->tail >= CRPC_QLEN) {
		pos = s->tail % CRPC_QLEN;
		c = s->qreq[pos];

		if (s->cmn.ldrop_handler)
			s->cmn.ldrop_handler(c);

		s->tail++;
		s->req_dropped_++;
#if CBW_TRACK_FLOW
		if (s->id == CBW_TRACK_FLOW_ID) {
			printf("[%lu] queue full. drop the request\n",
			       now);
		}
#endif
	}

	pos = s->head++ % CRPC_QLEN;
	c = s->qreq[pos];
	memcpy(c->buf, buf, len);
	c->id = s->req_id++;
	c->ts = now;
	c->len = len;
	c->arg = arg;

#if CBW_TRACK_FLOW
	if (s->id == CBW_TRACK_FLOW_ID) {
		printf("[%lu] request enqueued: id=%lu, qlen = %d\n",
		       now, c->id, s->head - s->tail);
	}
#endif

	// very first message
	if (!s->init) {
		for(int i = 0; i < s->cmn.nconns; ++i) {
			cc = (struct cbw_conn *)s->cmn.c[i];
			crpc_send_cupdate(cc);
			cc->waiting_resp = true;
		}
		s->init = true;
	}

	// if queue become non-empty, start expiration loop
	if (s->head - s->tail == 1)
		condvar_signal(&s->timer_cv);

	return true;
}

int cbw_add_connection(struct crpc_session *s_, struct netaddr raddr)
{
	struct cbw_session *s = (struct cbw_session *)s_;
	struct netaddr laddr;
	struct cbw_conn *cc;
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

ssize_t cbw_send_one(struct crpc_session *s_, const void *buf, size_t len,
		     int hash, void *arg)
{
	struct cbw_session *s = (struct cbw_session *)s_;
	struct cbw_conn *cc;
	ssize_t ret;

	/* implementation is currently limited to a maximum payload size */
	if (unlikely(len > SRPC_BUF_SIZE))
		return -E2BIG;

	mutex_lock(&s->lock);

	/* hot path, just send */
	cc = (struct cbw_conn *)s->cmn.c[s->next_conn_idx];
	if (cc->credit_used < cc->credit && s->head == s->tail) {
		cc->credit_used++;
		ret = crpc_send_raw(cc, buf, len, s->req_id++);
		s->next_conn_idx = (s->next_conn_idx + 1) % s->cmn.nconns;
		mutex_unlock(&s->lock);
		return ret;
	}

	/* cold path, enqueue request and drain the queue */
	if (!crpc_enqueue_one(s, buf, len, arg)) {
		crpc_drain_queue(s);
		mutex_unlock(&s->lock);
		return -ENOBUFS;
	}
	crpc_drain_queue(s);
	mutex_unlock(&s->lock);

	return len;
}

ssize_t cbw_recv_one(struct crpc_conn *cc_, void *buf, size_t len, void *arg)
{
	struct cbw_conn *cc = (struct cbw_conn *)cc_;
	struct cbw_session *s = cc->session;
	struct sbw_hdr shdr;
	ssize_t ret;

again:
	/* read the server header */
	ret = tcp_read_full(cc->cmn.c, &shdr, sizeof(shdr));
	if (unlikely(ret <= 0))
		return ret;
	assert(ret == sizeof(shdr));

	/* parse the server header */
	if (unlikely(shdr.magic != BW_RESP_MAGIC)) {
		log_warn("crpc: got invalid magic %x", shdr.magic);
		return -EINVAL;
	}
	if (unlikely(shdr.len > MIN(SRPC_BUF_SIZE, len))) {
		log_warn("crpc: request len %ld too large (limit %ld)",
			 shdr.len, MIN(SRPC_BUF_SIZE, len));
		return -EINVAL;
	}

	switch (shdr.op) {
	case BW_OP_CALL:
		/* read the payload */
		if (shdr.len > 0) {
			ret = tcp_read_full(cc->cmn.c, buf, shdr.len);
			if (unlikely(ret <= 0))
				return ret;
			assert(ret == shdr.len);
			if (!(shdr.flags & BW_SFLAG_DROP))
				cc->resp_rx_++;
		}

		/* update the credit */
		mutex_lock(&s->lock);
		assert(cc->credit_used > 0);
		cc->credit_used--;
		cc->credit = shdr.credit;
		cc->waiting_resp = false;

#if CBW_TRACK_FLOW
		if (s->id == CBW_TRACK_FLOW_ID) {
			printf("[%lu] ===> response: id=%lu, shdr.credit=%lu, credit=%u/%u\n",
			       microtime(), shdr.id, shdr.credit, cc->credit_used, cc->credit);
		}
#endif

		if (cc->credit > 0) {
			crpc_drain_queue(s);
		}

		mutex_unlock(&s->lock);

		if (shdr.flags & BW_SFLAG_DROP) {
			if (s->cmn.rdrop_handler)
				s->cmn.rdrop_handler(buf, ret, arg);
			goto again;
		}

		break;
	case BW_OP_CREDIT:
		if (unlikely(shdr.len != 0)) {
			log_warn("crpc: winupdate has nonzero len");
			return -EINVAL;
		}
		assert(shdr.len == 0);

		/* update the credit */
		mutex_lock(&s->lock);
		cc->credit = shdr.credit;
		cc->waiting_resp = false;

#if CBW_TRACK_FLOW
		if (s->id == CBW_TRACK_FLOW_ID) {
			printf("[%lu] ===> Winupdate: shdr.credit=%lu, credit=%u/%u\n",
			       microtime(), shdr.credit, cc->credit_used, cc->credit);
		}
#endif

		if (cc->credit > 0) {
			crpc_drain_queue(s);
		}
		mutex_unlock(&s->lock);
		cc->ecredit_rx_++;

		goto again;
	default:
		log_warn("crpc: got invalid op %d", shdr.op);
		return -EINVAL;
	}

	return shdr.len;
}

static void crpc_timer(void *arg)
{
	struct cbw_session *s = (struct cbw_session *)arg;
	uint64_t now;
	int pos;
	struct crpc_ctx *c;
	int num_drops;

	mutex_lock(&s->lock);
	while(true) {
		while (s->running && s->head == s->tail)
			condvar_wait(&s->timer_cv, &s->lock);

		if (!s->running)
			goto done;

		num_drops = 0;
		now = microtime();

		// Drop requests if expired
		while (s->head != s->tail) {
			pos = s->tail % CRPC_QLEN;
			c = s->qreq[pos];
			if (now - c->ts <= CBW_MAX_CLIENT_DELAY_US)
				break;

			// handle drop
			if (s->cmn.ldrop_handler)
				s->cmn.ldrop_handler(c);

			// update stats
			s->tail++;
			s->req_dropped_++;
			num_drops++;
#if CBW_TRACK_FLOW
			if (s->id == CBW_TRACK_FLOW_ID) {
				printf("[%lu] request dropped: id=%lu, qlen = %d\n",
				       now, c->id, s->head - s->tail);
			}
#endif
		}

		// If queue becomes empty
		if (s->head == s->tail) {
			continue;
		}

		// caculate next wake up time
		pos = (s->head - 1) % CRPC_QLEN;
		c = s->qreq[pos];
		mutex_unlock(&s->lock);
		timer_sleep_until(c->ts + CBW_MAX_CLIENT_DELAY_US);
		mutex_lock(&s->lock);
	}
done:
	mutex_unlock(&s->lock);
	waitgroup_done(&s->timer_waiter);
}

int cbw_open(struct netaddr raddr, struct crpc_session **sout, int id,
	     crpc_ldrop_fn_t ldrop_handler, crpc_rdrop_fn_t rdrop_handler,
	     struct rpc_session_info *info)
{
	struct netaddr laddr;
	struct cbw_session *s;
	struct cbw_conn *cc;
	tcpconn_t *c;
	int i, ret;

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

	for (i = 0; i < CRPC_QLEN; ++i) {
		s->qreq[i] = smalloc(sizeof(struct crpc_ctx));
		if (!s->qreq[i])
			goto fail;
	}

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
	s->cmn.ldrop_handler = ldrop_handler;
	s->cmn.rdrop_handler = rdrop_handler;
	s->cmn.session_type = info->session_type;
	s->running = true;
	s->id = id;
	s->req_id = 1;

	mutex_init(&s->lock);
	condvar_init(&s->timer_cv);
	waitgroup_init(&s->timer_waiter);
	waitgroup_add(&s->timer_waiter, 1);

	*sout = (struct crpc_session *)s;

	/* spawn timer thread */
	if (CBW_MAX_CLIENT_DELAY_US > 0) {
		ret = thread_spawn(crpc_timer, s);
		BUG_ON(ret);
	} else {
		waitgroup_done(&s->timer_waiter);
	}

	return 0;

fail:
	tcp_close(c);
	for (i = i - 1; i >= 0; i--)
		sfree(s->qreq[i]);
	sfree(s);
	return -ENOMEM;
}

void cbw_close(struct crpc_session *s_)
{
	struct cbw_session *s = (struct cbw_session *)s_;
	int i;

	/* terminate client and wait for timer thread */
	mutex_lock(&s->lock);
	s->running = false;
	condvar_signal(&s->timer_cv);
	mutex_unlock(&s->lock);

	waitgroup_wait(&s->timer_waiter);

	/* free resources */
	for (i = 0; i < s->cmn.nconns; ++i) {
		tcp_close(s->cmn.c[i]->c);
		sfree(s->cmn.c[i]);
	}
	for(i = 0; i < CRPC_QLEN; ++i)
		sfree(s->qreq[i]);
	sfree(s);
}

/* client-side stats */
uint32_t cbw_conn_credit(struct crpc_conn *cc_)
{
	struct cbw_conn *cc = (struct cbw_conn *)cc_;
	return cc->credit;
}

uint32_t cbw_sess_credit(struct crpc_session *s_)
{
	struct cbw_session *s = (struct cbw_session *)s_;
	uint32_t ret = 0;

	for(int i = 0; i < s->cmn.nconns; ++i) {
		ret += cbw_conn_credit(s->cmn.c[i]);
	}

	return ret;
}

void cbw_conn_stat_clear(struct crpc_conn *cc_)
{
	struct cbw_conn *cc = (struct cbw_conn *)cc_;

	cc->credit_expired_ = 0;
	cc->ecredit_rx_ = 0;
	cc->cupdate_tx_ = 0;
	cc->resp_rx_ = 0;
	cc->req_tx_ = 0;
	return;
}

void cbw_sess_stat_clear(struct crpc_session *s_)
{
	struct cbw_session *s = (struct cbw_session *)s_;

	s->req_dropped_ = 0;
	for(int i = 0; i < s->cmn.nconns; ++i) {
		cbw_conn_stat_clear(s->cmn.c[i]);
	}
}

uint64_t cbw_conn_stat_credit_expired(struct crpc_conn *cc_)
{
	struct cbw_conn *cc = (struct cbw_conn *)cc_;
	return cc->credit_expired_;
}

uint64_t cbw_sess_stat_credit_expired(struct crpc_session *s_)
{
	struct cbw_session *s = (struct cbw_session *)s_;
	uint64_t ret = 0;

	for(int i = 0; i < s->cmn.nconns; ++i) {
		ret += cbw_conn_stat_credit_expired(s->cmn.c[i]);
	}

	return ret;
}

uint64_t cbw_conn_stat_ecredit_rx(struct crpc_conn *cc_)
{
	struct cbw_conn *cc = (struct cbw_conn *)cc_;
	return cc->ecredit_rx_;
}

uint64_t cbw_sess_stat_ecredit_rx(struct crpc_session *s_)
{
	struct cbw_session *s = (struct cbw_session *)s_;
	uint64_t ret = 0;

	for(int i = 0; i < s->cmn.nconns; ++i) {
		ret += cbw_conn_stat_ecredit_rx(s->cmn.c[i]);
	}

	return ret;
}

uint64_t cbw_conn_stat_cupdate_tx(struct crpc_conn *cc_)
{
	struct cbw_conn *cc = (struct cbw_conn *)cc_;
	return cc->cupdate_tx_;
}

uint64_t cbw_sess_stat_cupdate_tx(struct crpc_session *s_)
{
	struct cbw_session *s = (struct cbw_session *)s_;
	uint64_t ret = 0;

	for(int i = 0; i < s->cmn.nconns; ++i) {
		ret += cbw_conn_stat_cupdate_tx(s->cmn.c[i]);
	}

	return ret;
}

uint64_t cbw_conn_stat_resp_rx(struct crpc_conn *cc_)
{
	struct cbw_conn *cc = (struct cbw_conn *)cc_;
	return cc->resp_rx_;
}

uint64_t cbw_sess_stat_resp_rx(struct crpc_session *s_)
{
	struct cbw_session *s = (struct cbw_session *)s_;
	uint64_t ret = 0;

	for(int i = 0; i < s->cmn.nconns; ++i) {
		ret += cbw_conn_stat_resp_rx(s->cmn.c[i]);
	}

	return ret;
}

uint64_t cbw_conn_stat_req_tx(struct crpc_conn *cc_)
{
	struct cbw_conn *cc = (struct cbw_conn *)cc_;
	return cc->req_tx_;
}

uint64_t cbw_sess_stat_req_tx(struct crpc_session *s_)
{
	struct cbw_session *s = (struct cbw_session *)s_;
	uint64_t ret = 0;

	for(int i = 0; i < s->cmn.nconns; ++i) {
		ret += cbw_conn_stat_req_tx(s->cmn.c[i]);
	}

	return ret;
}

uint64_t cbw_sess_stat_req_dropped(struct crpc_session *s_)
{
	struct cbw_session *s = (struct cbw_session *)s_;
	return s->req_dropped_;
}

struct crpc_ops cbw_ops = {
	.crpc_add_connection		= cbw_add_connection,
	.crpc_send_one			= cbw_send_one,
	.crpc_recv_one			= cbw_recv_one,
	.crpc_open			= cbw_open,
	.crpc_close			= cbw_close,
	.crpc_credit			= cbw_sess_credit,
	.crpc_stat_clear		= cbw_sess_stat_clear,
	.crpc_stat_ecredit_rx		= cbw_sess_stat_ecredit_rx,
	.crpc_stat_credit_expired	= cbw_sess_stat_credit_expired,
	.crpc_stat_cupdate_tx		= cbw_sess_stat_cupdate_tx,
	.crpc_stat_resp_rx		= cbw_sess_stat_resp_rx,
	.crpc_stat_req_tx		= cbw_sess_stat_req_tx,
	.crpc_stat_req_dropped		= cbw_sess_stat_req_dropped,
};
