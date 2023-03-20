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

#include <breakwater/seda.h>

#include "util.h"
#include "sd_proto.h"
#include "sd_config.h"

#define CSD_TRACK_FLOW			false
#define CSD_TRACK_FLOW_ID		1

static void tb_refill_token(struct csd_conn *cc) {
	double new_token;
	uint64_t now = microtime();

	assert_mutex_held(&cc->lock);

	if (cc->tb_last_refresh == 0) {
		cc->tb_last_refresh = now;
		cc->tb_token = 0;
		return;
	}

	new_token = (now - cc->tb_last_refresh) * cc->tb_refresh_rate / 1000000.0;
	cc->tb_token += new_token;
	cc->tb_last_refresh = now;

	cc->tb_token = MIN(cc->tb_token, CSD_TB_MAX_TOKEN);
}

static void tb_set_rate(struct csd_conn *cc, double new_rate)
{
	assert_mutex_held(&cc->lock);
	tb_refill_token(cc);
	cc->tb_refresh_rate = MAX(new_rate, CSD_TB_MIN_RATE);
}

static void tb_sleep_until_next_token(struct csd_conn *cc)
{
	uint64_t sleep_until;

	assert_mutex_held(&cc->lock);

	sleep_until = cc->tb_last_refresh +
		(1.0 - cc->tb_token) * 1000000 / cc->tb_refresh_rate;

	mutex_unlock(&cc->lock);
	timer_sleep_until(sleep_until + 1);
	mutex_lock(&cc->lock);
	tb_refill_token(cc);
}

static ssize_t crpc_send_request_vector(struct csd_conn *cc)
{
	struct csd_hdr chdr[CRPC_QLEN];
	struct iovec v[CRPC_QLEN * 2];
	int nriov = 0;
	int nrhdr = 0;
	ssize_t ret;
	uint64_t now = microtime();

	assert_mutex_held(&cc->lock);

	if (cc->head == cc->tail || cc->tb_token < 1.0)
		return 0;

	while (cc->head != cc->tail && cc->tb_token >= 1.0) {
		struct crpc_ctx *c = cc->qreq[cc->tail++ % CRPC_QLEN];

		chdr[nrhdr].magic = SD_REQ_MAGIC;
		chdr[nrhdr].op = SD_OP_CALL;
		chdr[nrhdr].id = c->id;
		chdr[nrhdr].len = c->len;
		chdr[nrhdr].ts = now;

		v[nriov].iov_base = &chdr[nrhdr];
		v[nriov].iov_len = sizeof(struct csd_hdr);
		nrhdr++;
		nriov++;

		if (c->len > 0) {
			v[nriov].iov_base = c->buf;
			v[nriov++].iov_len = c->len;
		}

		cc->tb_token -= 1.0;
	}

	if (cc->head == cc->tail) {
		cc->head = 0;
		cc->tail = 0;
	}

	if (nriov == 0)
		return 0;
	ret = tcp_writev_full(cc->cmn.c, v, nriov);

	cc->req_tx_ += nrhdr;

	if (unlikely(ret < 0))
		return ret;
	return 0;
}

static ssize_t crpc_send_raw(struct csd_conn *cc,
			     const void *buf, size_t len,
			     uint64_t id)
{
	struct iovec vec[2];
	struct csd_hdr chdr;
	ssize_t ret;

	/* header */
	chdr.magic = SD_REQ_MAGIC;
	chdr.op = SD_OP_CALL;
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

static bool crpc_enqueue_one(struct csd_session *s,
			     const void *buf, size_t len)
{
	int pos;
	struct crpc_ctx *c;
	uint64_t now = microtime();
	int conn_idx;
	struct csd_conn *cc;
	int i;

	assert_mutex_held(&s->lock);

	/* choose the connection */
	for (i = 0; i < s->cmn.nconns; ++i) {
		conn_idx = (s->next_conn_idx + i) % s->cmn.nconns;
		cc = (struct csd_conn *)s->cmn.c[conn_idx];
		if (cc->head - cc->tail < CRPC_QLEN) {
			s->next_conn_idx = (conn_idx + 1) % s->cmn.nconns;
			break;
		}
		cc = NULL;
	}

	/* if every connection is busy */
	if (!cc) {
		s->req_dropped_++;
		return false;
	}

	mutex_lock(&cc->lock);

	if (cc->head - cc->tail >= CRPC_QLEN) {
		s->req_dropped_++;
		mutex_unlock(&cc->lock);
		return false;
	}

	pos = cc->head++ % CRPC_QLEN;
	c = cc->qreq[pos];
	memcpy(c->buf, buf, len);
	c->id = s->req_id++;
	c->ts = now;
	c->len = len;

	if (cc->head - cc->tail == 1) {
		condvar_signal(&cc->timer_cv);
		condvar_signal(&cc->sender_cv);
	}

	mutex_unlock(&cc->lock);

	return true;
}

static void crpc_timer(void *arg)
{
	struct csd_conn *cc = (struct csd_conn *)arg;
	uint64_t now;
	int pos;
	struct crpc_ctx *c;

	mutex_lock(&cc->lock);
	while(true) {
		/* If queue is empty, wait for the request */
		while (cc->running && cc->head == cc->tail)
			condvar_wait(&cc->timer_cv, &cc->lock);

		if (!cc->running)
			goto done;

		now = microtime();

		/* drop the old requests */
		while (cc->head != cc->tail) {
			pos = cc->tail % CRPC_QLEN;
			c = cc->qreq[pos];
			if (now - c->ts <= CSD_MAX_CLIENT_DELAY_US)
				break;

			cc->tail++;
			cc->req_dropped_++;
		}

		if (cc->head == cc->tail) {
			cc->head = 0;
			cc->tail = 0;
			continue;
		}

		// calculate next wake up time
		pos = (cc->head - 1) % CRPC_QLEN;
		c = cc->qreq[pos];
		mutex_unlock(&cc->lock);
		timer_sleep_until(c->ts + CSD_MAX_CLIENT_DELAY_US);
		mutex_lock(&cc->lock);
	}
done:
	mutex_unlock(&cc->lock);
	waitgroup_done(&cc->timer_waiter);
}

static void crpc_sender(void *arg)
{
	struct csd_conn *cc = (struct csd_conn *)arg;
	int pos;
	struct crpc_ctx *c;
	uint64_t now;

	mutex_lock(&cc->lock);
	while (true) {
		while(cc->running && cc->head == cc->tail)
			condvar_wait(&cc->sender_cv, &cc->lock);

		if (!cc->running)
			goto done;

		while (cc->tb_token < 1.0)
			tb_sleep_until_next_token(cc);

		now = microtime();
		while (cc->head != cc->tail) {
			pos = cc->tail % CRPC_QLEN;
			c = cc->qreq[pos];
			if (now - c->ts <= CSD_MAX_CLIENT_DELAY_US)
				break;

			cc->tail++;
			cc->req_dropped_++;
		}

		crpc_send_request_vector(cc);
	}

done:
	mutex_unlock(&cc->lock);
	waitgroup_done(&cc->sender_waiter);
}

int csd_add_connection(struct crpc_session *s_, struct netaddr raddr)
{
	struct csd_session *s = (struct csd_session *)s_;
	struct netaddr laddr;
	struct csd_conn *cc;
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
		cc->qreq[i] = smalloc(sizeof(struct crpc_ctx));
		if (!cc->qreq[i])
			goto fail;
	}

	/* init conn */
	cc->cmn.c = c;
	cc->running = true;
	cc->tb_token = 0.0;
	cc->tb_refresh_rate = (rand() / (double)RAND_MAX) *
		(CSD_TB_INIT_RATE - CSD_TB_MIN_RATE) + CSD_TB_MIN_RATE;
	cc->tb_last_refresh = 0;
	cc->res_idx = 0;
	cc->session = s;

	mutex_init(&cc->lock);
	condvar_init(&cc->timer_cv);
	condvar_init(&cc->sender_cv);
	waitgroup_init(&cc->timer_waiter);
	waitgroup_add(&cc->timer_waiter, 1);
	waitgroup_init(&cc->sender_waiter);
	waitgroup_add(&cc->sender_waiter, 1);

	/* update session */
	mutex_lock(&s->lock);
	s->cmn.c[s->cmn.nconns++] = (struct crpc_conn *)cc;
	mutex_unlock(&s->lock);

	/* spawn timer thread */
	ret = thread_spawn(crpc_timer, cc);
	BUG_ON(ret);

	/* spawn sender thread */
	ret = thread_spawn(crpc_sender, cc);
	BUG_ON(ret);

	return 0;
fail:
	tcp_close(c);
	for(i = i - 1; i >= 0; i--)
		sfree(cc->qreq[i]);
	sfree(cc);
	return -ENOMEM;
}

ssize_t csd_send_one(struct crpc_session *s_,
		      const void *buf, size_t len, int hash, void *arg)
{
	struct csd_session *s = (struct csd_session *)s_;
	struct csd_conn *cc;
	int conn_idx;
	ssize_t ret;
	int i;

	/* implementation is currently limited to a maximum payload size */
	if (unlikely(len > SRPC_BUF_SIZE))
		return -E2BIG;

	for (i = 0; i < s->cmn.nconns; ++i) {
		cc = (struct csd_conn *)s->cmn.c[i];
		mutex_lock(&cc->lock);
		tb_refill_token(cc);
		mutex_unlock(&cc->lock);
	}

	mutex_lock(&s->lock);

	for (i = 0; i < s->cmn.nconns; ++i) {
		conn_idx = (s->next_conn_idx + i) % s->cmn.nconns;
		cc = (struct csd_conn *)s->cmn.c[conn_idx];
		/* hot path, just send */
		mutex_lock(&cc->lock);
		if (cc->head == cc->tail && cc->tb_token >= 1.0) {
			cc->tb_token -= 1.0;
			ret = crpc_send_raw(cc, buf, len, s->req_id++);
			s->next_conn_idx = (conn_idx + 1) % s->cmn.nconns;
			mutex_unlock(&cc->lock);
			mutex_unlock(&s->lock);
			return ret;
		}
		mutex_unlock(&cc->lock);
	}

	/* cold path, enqueue request and drain the queue */
	crpc_enqueue_one(s, buf, len);
	mutex_unlock(&s->lock);

	return len;
}

static int cmpfunc(const void *a, const void *b) {
	return (*(uint32_t*)a - *(uint32_t*)b);
}

static void crpc_update_tb_rate(struct csd_conn *cc, uint64_t us)
{
	double rate = cc->tb_refresh_rate;
	uint32_t samp;
	double err;
	uint64_t now = microtime();
	int idx;
	int len;

	assert_mutex_held(&cc->lock);

	cc->res_ts[cc->res_idx++ % SEDA_NREQ] = (uint32_t)us;

	if (now - cc->seda_last_update > SEDA_TIMEOUT) {
		len = cc->res_idx % SEDA_NREQ;
		qsort(cc->res_ts, len, sizeof(uint32_t), cmpfunc);
		idx = (int)((len - 1) * 0.99);
		samp = cc->res_ts[idx];
		cc->cur = SEDA_ALPHA * cc->cur + (1 - SEDA_ALPHA) * samp;

		err = (cc->cur - SEDA_TARGET) / (double)SEDA_TARGET;

		if (err > SEDA_ERR_D)
			rate = rate / SEDA_ADJ_D;
		else if (err < SEDA_ERR_I)
			rate += -(err - SEDA_CI) * SEDA_ADJ_I;

		tb_set_rate(cc, rate);
		cc->seda_last_update = microtime();
		cc->res_idx = 0;
		return;
	}

	if (cc->res_idx % SEDA_NREQ > 0)
		return;

	// sort res_ts;
	qsort(cc->res_ts, SEDA_NREQ, sizeof(uint32_t), cmpfunc);
	samp = cc->res_ts[(int)(SEDA_NREQ * 0.99)];
	cc->cur = SEDA_ALPHA * cc->cur + (1 - SEDA_ALPHA) * samp;

	err = (cc->cur - SEDA_TARGET) / (double)SEDA_TARGET;

	if (err > SEDA_ERR_D) {
		rate = rate / SEDA_ADJ_D;
	} else if (err < SEDA_ERR_I) {
		rate += -(err - SEDA_CI) * SEDA_ADJ_I;
	}

	tb_set_rate(cc, rate);
	cc->seda_last_update = now;
}

ssize_t csd_recv_one(struct crpc_conn *cc_, void *buf, size_t len, void *arg)
{
	struct csd_conn *cc = (struct csd_conn *)cc_;
	struct ssd_hdr shdr;
	ssize_t ret;
	uint64_t now;
	uint64_t us;

again:
	/* read the server header */
	ret = tcp_read_full(cc->cmn.c, &shdr, sizeof(shdr));
	if (unlikely(ret <= 0))
		return ret;
	assert(ret == sizeof(shdr));

	/* parse the server header */
	if (unlikely(shdr.magic != SD_RESP_MAGIC)) {
		log_warn("crpc: got invalid magic %x", shdr.magic);
		return -EINVAL;
	}
	if (unlikely(shdr.len > MIN(SRPC_BUF_SIZE, len))) {
		log_warn("crpc: request len %ld too large (limit %ld)",
			 shdr.len, MIN(SRPC_BUF_SIZE, len));
		return -EINVAL;
	}

	now = microtime();
	switch (shdr.op) {
	case SD_OP_CALL:
		// ignore dropped requests
		if (shdr.len == 0)
			goto again;

		ret = tcp_read_full(cc->cmn.c, buf, shdr.len);
		if (unlikely(ret <= 0))
			return ret;
		assert(ret == shdr.len);
		cc->resp_rx_++;

		us = now - shdr.ts;

		mutex_lock(&cc->lock);
		crpc_update_tb_rate(cc, us);
		mutex_unlock(&cc->lock);

#if CSD_TRACK_FLOW
		if (s->id == CSD_TRACK_FLOW_ID) {
			printf("[%lu] ===> response: id=%lu\n",
			       now, shdr.id, shdr.win);
		}
#endif

		break;
	default:
		log_warn("crpc: got invalid op %d", shdr.op);
		return -EINVAL;
	}

	return shdr.len;
}


int csd_open(struct netaddr raddr, struct crpc_session **sout, int id,
	     crpc_ldrop_fn_t ldrop_handler, crpc_rdrop_fn_t rdrop_handler,
	     struct rpc_session_info *info)
{
	struct netaddr laddr;
	struct csd_session *s;
	struct csd_conn *cc;
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

	/* alloc conn */
	cc = smalloc(sizeof(*cc));
	if (!cc) {
		tcp_close(c);
		sfree(s);
		return -ENOMEM;
	}
	memset(cc, 0, sizeof(*cc));

	for (i = 0; i < CRPC_QLEN; ++i) {
		cc->qreq[i] = smalloc(sizeof(struct crpc_ctx));
		if (!cc->qreq[i])
			goto fail;
	}

	/* init conn */
	cc->cmn.c = c;
	cc->running = true;
	cc->tb_token = 0.0;
	cc->tb_refresh_rate = (rand() / (double)RAND_MAX) *
		(CSD_TB_INIT_RATE - CSD_TB_MIN_RATE) + CSD_TB_MIN_RATE;
	cc->tb_last_refresh = 0;
	cc->res_idx = 0;
	cc->session = s;

	mutex_init(&cc->lock);
	condvar_init(&cc->timer_cv);
	condvar_init(&cc->sender_cv);
	waitgroup_init(&cc->timer_waiter);
	waitgroup_add(&cc->timer_waiter, 1);
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

	/* spawn timer thread */
	ret = thread_spawn(crpc_timer, cc);
	BUG_ON(ret);

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

void csd_close(struct crpc_session *s_)
{
	struct csd_session *s = (struct csd_session *)s_;
	struct csd_conn *cc;
	int i, j;

	/* terminate client */
	for (i = 0; i < s->cmn.nconns; ++i) {
		cc = (struct csd_conn *)s->cmn.c[i];
		mutex_lock(&cc->lock);
		cc->running = false;
		condvar_signal(&cc->timer_cv);
		condvar_signal(&cc->sender_cv);
		mutex_unlock(&cc->lock);
	}

	/* wait for timer and sender thread */
	for (i = 0; i < s->cmn.nconns; ++i) {
		cc = (struct csd_conn *)s->cmn.c[i];
		waitgroup_wait(&cc->timer_waiter);
		waitgroup_wait(&cc->sender_waiter);
	}

	/* free resources */
	for (i = 0; i < s->cmn.nconns; ++i) {
		cc = (struct csd_conn *)s->cmn.c[i];
		tcp_close(cc->cmn.c);
		for (j = 0; j < CRPC_QLEN; ++j)
			sfree(cc->qreq[j]);
		sfree(cc);
	}
	sfree(s);
}

/* client-side stats */
uint32_t csd_credit(struct crpc_session *s_)
{
	return 0;
}

void csd_stat_clear(struct crpc_session *s_)
{
	return;
}

uint64_t csd_stat_credit_expired(struct crpc_session *s_)
{
	return 0;
}

uint64_t csd_stat_ecredit_rx(struct crpc_session *s_)
{
	return 0;
}

uint64_t csd_stat_cupdate_tx(struct crpc_session *s_)
{
	return 0;
}

uint64_t csd_conn_stat_resp_rx(struct crpc_conn *cc_)
{
	struct csd_conn *cc = (struct csd_conn *)cc_;
	return cc->resp_rx_;
}

uint64_t csd_sess_stat_resp_rx(struct crpc_session *s_)
{
	struct csd_session *s = (struct csd_session *)s_;
	uint64_t ret = 0;

	for(int i = 0; i < s->cmn.nconns; ++i) {
		ret += csd_conn_stat_resp_rx(s->cmn.c[i]);
	}

	return ret;
}

uint64_t csd_conn_stat_req_tx(struct crpc_conn *cc_)
{
	struct csd_conn *cc = (struct csd_conn *)cc_;
	return cc->req_tx_;
}

uint64_t csd_sess_stat_req_tx(struct crpc_session *s_)
{
	struct csd_session *s = (struct csd_session *)s_;
	uint64_t ret = 0;

	for(int i = 0; i < s->cmn.nconns; ++i) {
		ret += csd_conn_stat_req_tx(s->cmn.c[i]);
	}

	return ret;
}

uint64_t csd_conn_stat_req_dropped(struct crpc_conn *cc_)
{
	struct csd_conn *cc = (struct csd_conn *)cc_;
	return cc->req_dropped_;
}

uint64_t csd_sess_stat_req_dropped(struct crpc_session *s_)
{
	struct csd_session *s = (struct csd_session *)s_;
	uint64_t ret = s->req_dropped_;

	for(int i = 0; i < s->cmn.nconns; ++i) {
		ret += csd_conn_stat_req_dropped(s->cmn.c[i]);
	}

	return ret;
}

struct crpc_ops csd_ops = {
	.crpc_add_connection		= csd_add_connection,
	.crpc_send_one			= csd_send_one,
	.crpc_recv_one			= csd_recv_one,
	.crpc_open			= csd_open,
	.crpc_close			= csd_close,
	.crpc_credit			= csd_credit,
	.crpc_stat_clear		= csd_stat_clear,
	.crpc_stat_ecredit_rx		= csd_stat_ecredit_rx,
	.crpc_stat_credit_expired	= csd_stat_credit_expired,
	.crpc_stat_cupdate_tx		= csd_stat_cupdate_tx,
	.crpc_stat_resp_rx		= csd_sess_stat_resp_rx,
	.crpc_stat_req_tx		= csd_sess_stat_req_tx,
	.crpc_stat_req_dropped		= csd_sess_stat_req_dropped,
};
