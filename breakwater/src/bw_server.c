/*
 * RPC server-side support
 */

#include <stdio.h>

#include <base/atomic.h>
#include <base/stddef.h>
#include <base/time.h>
#include <base/list.h>
#include <base/log.h>
#include <runtime/tcp.h>
#include <runtime/sync.h>
#include <runtime/smalloc.h>
#include <runtime/thread.h>
#include <runtime/timer.h>
#include <runtime/runtime.h>

#include <breakwater/breakwater.h>

#include "util.h"
#include "bw_proto.h"
#include "bw_config.h"

/* time-series output */
#define SBW_TS_OUT		false
#define TS_BUF_SIZE_EXP		10
#define TS_BUF_SIZE		(1 << TS_BUF_SIZE_EXP)
#define TS_BUF_MASK		(TS_BUF_SIZE - 1)

#define SBW_TRACK_FLOW		false
#define SBW_TRACK_FLOW_ID	1

#define EWMA_WEIGHT		0.1f

BUILD_ASSERT((1 << SBW_MAX_WINDOW_EXP) == SBW_MAX_WINDOW);

#if SBW_TS_OUT
int nextIndex = 0;
FILE *ts_out = NULL;

struct Event {
	uint64_t timestamp;
	int credit_pool;
	int credit_used;
	int num_pending;
	int num_drained;
	int num_active;
	int num_sess;
	uint64_t delay;
	int num_cores;
	uint64_t avg_st;
};

static struct Event events[TS_BUF_SIZE];
#endif

/* the handler function for each RPC */
static srpc_fn_t srpc_handler;

/* total number of session */
atomic_t srpc_num_sess;

/* the number of drained session */
atomic_t srpc_num_drained;

/* the number of active sessions */
atomic_t srpc_num_active;

/* global credit pool */
atomic_t srpc_credit_pool;

/* global credit used */
atomic_t srpc_credit_used;

/* downstream credit for multi-hierarchy */
atomic_t srpc_credit_ds;

/* the number of pending requests */
atomic_t srpc_num_pending;

/* EWMA execution time */
atomic_t srpc_avg_st;

double credit_carry;

/* drained session list */
struct srpc_drained_ {
	spinlock_t lock;
	struct list_head list;
	void *pad[5];
};

BUILD_ASSERT(sizeof(struct srpc_drained_) == CACHE_LINE_SIZE);

static struct srpc_drained_ srpc_drained[NCPU]
		__attribute__((aligned(CACHE_LINE_SIZE)));


struct sbw_session {
	struct srpc_session	cmn;
	int			id;
	struct list_node	drained_link;
	/* drained_list's core number. -1 if not in the drained list */
	int			drained_core;
	bool			is_linked;
	bool			wake_up;
	waitgroup_t		send_waiter;
	int			credit;
	/* the number of recently advertised credit */
	int			advertised;
	int			num_pending;
	/* Whether this session requires explicit credit */
	bool			need_ecredit;
	uint64_t		demand;
	/* timestamp for the last explicit credit issue */
	uint64_t		last_ecredit_timestamp;
	bool			demand_sync;

	/* shared state between receiver and sender */
	DEFINE_BITMAP(avail_slots, SBW_MAX_WINDOW);

	/* shared statnhocho@hp159.utah.cloudlab.use between workers and sender */
	spinlock_t		lock;
	int			closed;
	thread_t		*sender_th;
	DEFINE_BITMAP(completed_slots, SBW_MAX_WINDOW);

	/* worker slots (one for each credit issued) */
	struct sbw_ctx		*slots[SBW_MAX_WINDOW];
};

/* credit-related stats */
atomic64_t srpc_stat_cupdate_rx_;
atomic64_t srpc_stat_ecredit_tx_;
atomic64_t srpc_stat_credit_tx_;
atomic64_t srpc_stat_req_rx_;
atomic64_t srpc_stat_req_dropped_;
atomic64_t srpc_stat_resp_tx_;

#if SBW_TS_OUT
static void printRecord()
{
	int i;

	if (!ts_out)
		ts_out = fopen("timeseries.csv", "w");

	for (i = 0; i < TS_BUF_SIZE; ++i) {
		struct Event *event = &events[i];
		fprintf(ts_out, "%lu,%d,%d,%d,%d,%d,%d,%lu,%d,%lu\n",
			event->timestamp, event->credit_pool,
			event->credit_used, event->num_pending,
			event->num_drained, event->num_active,
			event->num_sess, event->delay,
			event->num_cores, event->avg_st);
	}
	fflush(ts_out);
}

static void record(int credit_pool, uint64_t delay)
{
	struct Event *event = &events[nextIndex];
	nextIndex = (nextIndex + 1) & TS_BUF_MASK;

	event->timestamp = microtime();
	event->credit_pool = credit_pool;
	event->credit_used = atomic_read(&srpc_credit_used);
	event->num_pending = atomic_read(&srpc_num_pending);
	event->num_drained = atomic_read(&srpc_num_drained);
	event->num_active = atomic_read(&srpc_num_active);
	event->num_sess = atomic_read(&srpc_num_sess);
	event->delay = delay;
	event->num_cores = runtime_active_cores();
	event->avg_st = atomic_read(&srpc_avg_st);

	if (nextIndex == 0)
		printRecord();
}
#endif

static int srpc_get_slot(struct sbw_session *s)
{
	int base;
	int slot = -1;
	for (base = 0; base < BITMAP_LONG_SIZE(SBW_MAX_WINDOW); ++base) {
		slot = __builtin_ffsl(s->avail_slots[base]) - 1;
		if (slot >= 0)
			break;
	}

	if (slot >= 0) {
		slot += BITS_PER_LONG * base;
		bitmap_atomic_clear(s->avail_slots, slot);
		s->slots[slot] = smalloc(sizeof(struct sbw_ctx));
		s->slots[slot]->cmn.s = (struct srpc_session *)s;
		s->slots[slot]->cmn.idx = slot;
		s->slots[slot]->cmn.ds_credit = 0;
		s->slots[slot]->cmn.drop = false;
	}

	return slot;
}

static void srpc_put_slot(struct sbw_session *s, int slot)
{
	sfree(s->slots[slot]);
	s->slots[slot] = NULL;
	bitmap_atomic_set(s->avail_slots, slot);
}

static int srpc_send_ecredit(struct sbw_session *s)
{
	struct sbw_hdr shdr;
	int ret;

	/* craft the response header */
	shdr.magic = BW_RESP_MAGIC;
	shdr.op = BW_OP_CREDIT;
	shdr.len = 0;
	shdr.credit = (uint64_t)s->credit;

	/* send the packet */
	ret = tcp_write_full(s->cmn.c, &shdr, sizeof(shdr));
	if (unlikely(ret < 0))
		return ret;

	atomic64_inc(&srpc_stat_ecredit_tx_);

#if SBW_TRACK_FLOW
	if (s->id == SBW_TRACK_FLOW_ID) {
		printf("[%lu] <== ECredit: credit = %lu\n",
		       microtime(), shdr.credit);
	}
#endif

	return 0;
}

static int srpc_send_completion_vector(struct sbw_session *s,
				       unsigned long *slots)
{
	struct sbw_hdr shdr[SBW_MAX_WINDOW];
	struct iovec v[SBW_MAX_WINDOW * 2];
	int nriov = 0;
	int nrhdr = 0;
	int i;
	ssize_t ret = 0;

	bitmap_for_each_set(slots, SBW_MAX_WINDOW, i) {
		struct sbw_ctx *c = s->slots[i];
		size_t len;
		char *buf;
		uint8_t flags = 0;

		if (!c->cmn.drop) {
			len = c->cmn.resp_len;
			buf = c->cmn.resp_buf;
		} else {
			len = c->cmn.req_len;
			buf = c->cmn.req_buf;
			flags |= BW_SFLAG_DROP;
		}

		shdr[nrhdr].magic = BW_RESP_MAGIC;
		shdr[nrhdr].op = BW_OP_CALL;
		shdr[nrhdr].len = len;
		shdr[nrhdr].id = c->cmn.id;
		shdr[nrhdr].credit = (uint64_t)s->credit;
		shdr[nrhdr].ts_sent = c->ts_sent;
		shdr[nrhdr].flags = flags;

		v[nriov].iov_base = &shdr[nrhdr];
		v[nriov].iov_len = sizeof(struct sbw_hdr);
		nrhdr++;
		nriov++;

		if (len > 0) {
			v[nriov].iov_base = buf;
			v[nriov++].iov_len = len;
		}
	}

	/* send the completion(s) */
	if (nriov == 0)
		return 0;
	ret = tcp_writev_full(s->cmn.c, v, nriov);
	bitmap_for_each_set(slots, SBW_MAX_WINDOW, i)
		srpc_put_slot(s, i);

#if SBW_TRACK_FLOW
	if (s->id == SBW_TRACK_FLOW_ID) {
		printf("[%lu] <=== Response (%d): credit=%d\n",
			microtime(), nrhdr, s->credit);
	}
#endif
	atomic_sub_and_fetch(&srpc_num_pending, nrhdr);
	atomic64_fetch_and_add(&srpc_stat_resp_tx_, nrhdr);

	if (unlikely(ret < 0))
		return ret;
	return 0;
}

static void srpc_update_credit(struct sbw_session *s, bool req_dropped)
{
	int credit_pool = atomic_read(&srpc_credit_pool);
	int credit_ds = atomic_read(&srpc_credit_ds);
	int credit_used = atomic_read(&srpc_credit_used);
	int num_sess = atomic_read(&srpc_num_sess);
	int old_credit = s->credit;
	int credit_diff;
	int credit_unused;
	int max_overprovision;

	if (credit_ds > 0)
		credit_pool = MIN(credit_pool, credit_ds);

	assert_spin_lock_held(&s->lock);

	if (s->drained_core != -1)
		return;

	credit_unused = credit_pool - credit_used;
	max_overprovision = MAX((int)(credit_unused / num_sess), 1);
	if (credit_used < credit_pool) {
		s->credit = MIN(s->num_pending + s->demand + max_overprovision,
			     s->credit + credit_unused);
	} else if (credit_used > credit_pool) {
		s->credit--;
	}

	if (s->wake_up || num_sess <= runtime_max_cores())
		s->credit = MAX(s->credit, max_overprovision);

	// prioritize the session
	if (old_credit > 0 && s->credit == 0 && !req_dropped && !s->demand_sync)
		s->credit = max_overprovision;

	/* clamp to supported values */
	/* now we allow zero credit */
	s->credit = MAX(s->credit, s->num_pending);
	s->credit = MIN(s->credit, SBW_MAX_WINDOW - 1);

	if (s->demand_sync)
		s->credit = MIN(s->credit, s->num_pending + s->demand);
	else
		s->credit = MIN(s->credit, s->num_pending + s->demand + max_overprovision);

	credit_diff = s->credit - old_credit;
	atomic_fetch_and_add(&srpc_credit_used, credit_diff);
#if SBW_TRACK_FLOW
	if (s->id == SBW_TRACK_FLOW_ID) {
		printf("[%lu] credit update: credit_pool = %d, credit_used = %d, req_dropped = %d, num_pending = %d, demand = %d, num_sess = %d, old_credit = %d, new_credit = %d\n",
		       microtime(), credit_pool, credit_used, req_dropped, s->num_pending, s->demand, num_sess, old_credit, s->credit);
	}
#endif
}

static struct sbw_session *srpc_choose_drained_session(int core_id)
{
	struct sbw_session *ret;

	assert(core_id >= 0);
	assert(core_id < runtime_max_cores());

	ret = NULL;

	if (list_empty(&srpc_drained[core_id].list))
		return NULL;

	spin_lock_np(&srpc_drained[core_id].lock);
	if (list_empty(&srpc_drained[core_id].list)) {
		spin_unlock_np(&srpc_drained[core_id].lock);
		return NULL;
	}

	ret = list_pop(&srpc_drained[core_id].list,
		       struct sbw_session,
		       drained_link);

	assert(ret->is_linked);
	ret->is_linked = false;
	spin_unlock_np(&srpc_drained[core_id].lock);
	spin_lock_np(&ret->lock);
	ret->drained_core = -1;
	spin_unlock_np(&ret->lock);
	atomic_dec(&srpc_num_drained);
#if SBW_TRACK_FLOW
	if (ret->id == SBW_TRACK_FLOW_ID) {
		printf("[%lu] Session waken up\n", microtime());
	}
#endif

	return ret;
}

static void srpc_remove_from_drained_list(struct sbw_session *s)
{
	assert_spin_lock_held(&s->lock);

	if (s->drained_core == -1)
		return;

	spin_lock_np(&srpc_drained[s->drained_core].lock);
	if (s->is_linked) {
		list_del(&s->drained_link);
		s->is_linked = false;
		atomic_dec(&srpc_num_drained);
#if SBW_TRACK_FLOW
		if (s->id == SBW_TRACK_FLOW_ID) {
			printf("[%lu] Seesion is removed from drained list\n",
			       microtime());
		}
#endif
	}
	spin_unlock_np(&srpc_drained[s->drained_core].lock);
	s->drained_core = -1;
}

static void srpc_worker(void *arg)
{
	struct sbw_ctx *c = (struct sbw_ctx *)arg;
	struct sbw_session *s = (struct sbw_session *)c->cmn.s;
	uint64_t service_time;
	uint64_t avg_st;
	thread_t *th;

	c->cmn.drop = false;
	service_time = microtime();
	srpc_handler((struct srpc_ctx *)c);
	service_time = microtime() - service_time;
	avg_st = atomic_read(&srpc_avg_st);
	avg_st = (uint64_t)(avg_st - (avg_st >> 3) + (service_time >> 3));

	atomic_write(&srpc_avg_st, avg_st);
	atomic_write(&srpc_credit_ds, c->cmn.ds_credit);

	spin_lock_np(&s->lock);
	bitmap_set(s->completed_slots, c->cmn.idx);
	th = s->sender_th;
	s->sender_th = NULL;
	spin_unlock_np(&s->lock);
	if (th)
		thread_ready(th);
}

static int srpc_recv_one(struct sbw_session *s)
{
	struct cbw_hdr chdr;
	int idx, ret;
	thread_t *th;
	uint64_t old_demand;
	int credit_diff;
	char buf_tmp[SRPC_BUF_SIZE];
	struct sbw_ctx *c;

again:
	th = NULL;
	/* read the client header */
	ret = tcp_read_full(s->cmn.c, &chdr, sizeof(chdr));
	if (unlikely(ret <= 0)) {
		if (ret == 0)
			return -EIO;
		return ret;
	}

	/* parse the client header */
	if (unlikely(chdr.magic != BW_REQ_MAGIC)) {
		log_warn("srpc: got invalid magic %x", chdr.magic);
		return -EINVAL;
	}
	if (unlikely(chdr.len > SRPC_BUF_SIZE)) {
		log_warn("srpc: request len %ld too large (limit %d)",
			 chdr.len, SRPC_BUF_SIZE);
		return -EINVAL;
	}

	switch (chdr.op) {
	case BW_OP_CALL:
		atomic64_inc(&srpc_stat_req_rx_);
		/* reserve a slot */
		idx = srpc_get_slot(s);
		if (unlikely(idx < 0)) {
			tcp_read_full(s->cmn.c, buf_tmp, chdr.len);
			atomic64_inc(&srpc_stat_req_dropped_);
			return 0;
		}
		c = s->slots[idx];

		/* retrieve the payload */
		ret = tcp_read_full(s->cmn.c, c->cmn.req_buf, chdr.len);
		if (unlikely(ret <= 0)) {
			srpc_put_slot(s, idx);
			if (ret == 0)
				return -EIO;
			return ret;
		}

		c->cmn.req_len = chdr.len;
		c->cmn.resp_len = 0;
		c->cmn.id = chdr.id;
		c->ts_sent = chdr.ts_sent;

		spin_lock_np(&s->lock);
		old_demand = s->demand;
		s->demand = chdr.demand;
		s->demand_sync = (chdr.flags & BW_CFLAG_DSYNC);
		srpc_remove_from_drained_list(s);
		s->num_pending++;
		/* adjust credit if demand changed */
		if (s->credit > s->num_pending + s->demand) {
			credit_diff = s->credit - (s->num_pending + s->demand);
			s->credit = s->num_pending + s->demand;
			atomic_sub_and_fetch(&srpc_credit_used, credit_diff);
		}

		atomic_inc(&srpc_num_pending);

		if (SBW_DROP_THRESH > 0 &&
		    runtime_queue_us() >= SBW_DROP_THRESH) {
			thread_t *th;

			c->cmn.drop = true;
			bitmap_set(s->completed_slots, idx);
			th = s->sender_th;
			s->sender_th = NULL;
			spin_unlock_np(&s->lock);
			if (th)
				thread_ready(th);
			atomic64_inc(&srpc_stat_req_dropped_);
			goto again;
		}

		spin_unlock_np(&s->lock);

		ret = thread_spawn(srpc_worker, c);
		BUG_ON(ret);

#if SBW_TRACK_FLOW
		uint64_t now = microtime();
		if (s->id == SBW_TRACK_FLOW_ID) {
			printf("[%lu] ===> Request: id=%lu, demand=%lu, delay=%lu\n",
			       now, chdr.id, chdr.demand, now - s->last_ecredit_timestamp);
		}
#endif
		break;
	case BW_OP_CREDIT:
		if (unlikely(chdr.len != 0)) {
			log_warn("srpc: cupdate has nonzero len");
			return -EINVAL;
		}
		assert(chdr.len == 0);

		spin_lock_np(&s->lock);
		old_demand = s->demand;
		s->demand = chdr.demand;
		s->demand_sync = (chdr.flags & BW_CFLAG_DSYNC);

		if (old_demand > 0 && s->demand == 0) {
			srpc_remove_from_drained_list(s);
		} else if (old_demand == 0 && s->demand > 0) {
			if (s->num_pending == 0) {
				th = s->sender_th;
				s->sender_th = NULL;
				s->need_ecredit = true;
			}
		}

		if (s->demand == 0)
			s->advertised = 0;

		/* adjust credit if demand changed */
		if (s->credit > s->num_pending + s->demand) {
			credit_diff = s->credit - (s->num_pending + s->demand);
			s->credit = s->num_pending + s->demand;
			atomic_sub_and_fetch(&srpc_credit_used, credit_diff);
		}
		spin_unlock_np(&s->lock);

		if (th)
			thread_ready(th);

		atomic64_inc(&srpc_stat_cupdate_rx_);
#if SBW_TRACK_FLOW
		if (s->id == SBW_TRACK_FLOW_ID) {
			printf("[%lu] ===> Winupdate: demand=%lu, \n",
			       microtime(), chdr.demand);
		}
#endif
		goto again;
	default:
		log_warn("srpc: got invalid op %d", chdr.op);
		return -EINVAL;
	}

	return ret;
}

static void srpc_sender(void *arg)
{
	DEFINE_BITMAP(tmp, SBW_MAX_WINDOW);
	struct sbw_session *s = (struct sbw_session *)arg;
	int ret, i;
	bool sleep;
	int num_resp;
	unsigned int core_id;
	bool send_explicit_credit;
	int drained_core;
	int old_credit;
	int credit;
	int credit_issued;
	bool req_dropped;

	while (true) {
		/* find slots that have completed */
		spin_lock_np(&s->lock);
		while (true) {
			sleep = !s->closed && !s->need_ecredit && !s->wake_up &&
				bitmap_popcount(s->completed_slots,
						SBW_MAX_WINDOW) == 0;
			if (!sleep) {
				s->sender_th = NULL;
				break;
			}
			s->sender_th = thread_self();
			thread_park_and_unlock_np(&s->lock);
			spin_lock_np(&s->lock);
		}
		if (unlikely(s->closed)) {
			spin_unlock_np(&s->lock);
			break;
		}
		req_dropped = false;
		memcpy(tmp, s->completed_slots, sizeof(tmp));
		bitmap_init(s->completed_slots, SBW_MAX_WINDOW, false);

		bitmap_for_each_set(tmp, SBW_MAX_WINDOW, i) {
			struct sbw_ctx *c = s->slots[i];
			if (c->cmn.drop) {
				req_dropped = true;
				break;
			}
		}

		if (s->wake_up)
			srpc_remove_from_drained_list(s);

		drained_core = s->drained_core;
		num_resp = bitmap_popcount(tmp, SBW_MAX_WINDOW);
		s->num_pending -= num_resp;
		old_credit = s->credit;
		srpc_update_credit(s, req_dropped);
		credit = s->credit;

		credit_issued = MAX(0, credit - old_credit + num_resp);
		atomic64_fetch_and_add(&srpc_stat_credit_tx_, credit_issued);

		send_explicit_credit = (s->need_ecredit || s->wake_up) &&
			num_resp == 0 && s->advertised < s->credit;

		if (num_resp > 0 || send_explicit_credit)
			s->advertised = s->credit;

		s->need_ecredit = false;
		s->wake_up = false;

		if (send_explicit_credit)
			s->last_ecredit_timestamp = microtime();
		spin_unlock_np(&s->lock);

		/* Send WINUPDATE message */
		if (send_explicit_credit) {
			ret = srpc_send_ecredit(s);
			if (unlikely(ret))
				goto close;
			continue;
		}

		/* send a response for each completed slot */
		ret = srpc_send_completion_vector(s, tmp);
		core_id = get_current_affinity();

		/* add to the drained list if (1) credit becomes zero,
		 * (2) s is not in the list already,
		 * (3) it has no outstanding requests */
		if (credit == 0 && drained_core == -1 &&
		    bitmap_popcount(s->avail_slots, SBW_MAX_WINDOW) ==
		    SBW_MAX_WINDOW) {
			spin_lock_np(&s->lock);
			if (!s->demand_sync || s->demand > 0) {
				spin_lock_np(&srpc_drained[core_id].lock);
				assert(!s->is_linked);
				BUG_ON(s->credit > 0);
				if (!s->demand_sync) {
					list_add_tail(&srpc_drained[core_id].list,
						      &s->drained_link);
				} else if (s->demand > 0) {
					list_add_tail(&srpc_drained[core_id].list,
						      &s->drained_link);
				}
				s->is_linked = true;
				spin_unlock_np(&srpc_drained[core_id].lock);
				s->drained_core = core_id;
				atomic_inc(&srpc_num_drained);
			}
			spin_unlock_np(&s->lock);
#if SBW_TRACK_FLOW
			if (s->id == SBW_TRACK_FLOW_ID) {
				printf("[%lu] Session is drained: credit=%d, drained_core = %d\n",
				       microtime(), credit, s->drained_core);
			}
#endif
		}
	}

close:
	/* wait for in-flight completions to finish */
	spin_lock_np(&s->lock);
	while (!s->closed ||
	       bitmap_popcount(s->avail_slots, SBW_MAX_WINDOW) +
	       bitmap_popcount(s->completed_slots, SBW_MAX_WINDOW) <
	       SBW_MAX_WINDOW) {
		s->sender_th = thread_self();
		thread_park_and_unlock_np(&s->lock);
		spin_lock_np(&s->lock);
		s->sender_th = NULL;
	}

	/* remove from the drained list */
	srpc_remove_from_drained_list(s);
	spin_unlock_np(&s->lock);

	/* free any left over slots */
	for (i = 0; i < SBW_MAX_WINDOW; i++) {
		if (s->slots[i])
			srpc_put_slot(s, i);
	}

	/* notify server thread that the sender is done */
	waitgroup_done(&s->send_waiter);
}

static void srpc_server(void *arg)
{
	tcpconn_t *c = (tcpconn_t *)arg;
	struct sbw_session *s;
	thread_t *th;
	int ret;

	s = smalloc(sizeof(*s));
	BUG_ON(!s);
	memset(s, 0, sizeof(*s));

	s->cmn.c = c;
	s->drained_core = -1;
	s->id = atomic_fetch_and_add(&srpc_num_sess, 1) + 1;
	bitmap_init(s->avail_slots, SBW_MAX_WINDOW, true);

	waitgroup_init(&s->send_waiter);
	waitgroup_add(&s->send_waiter, 1);

#if SBW_TRACK_FLOW
	if (s->id == SBW_TRACK_FLOW_ID) {
		printf("[%lu] connection established.\n",
		       microtime());
	}
#endif

	ret = thread_spawn(srpc_sender, s);
	BUG_ON(ret);

	while (true) {
		ret = srpc_recv_one(s);
		if (ret)
			break;
	}

	spin_lock_np(&s->lock);
	th = s->sender_th;
	s->sender_th = NULL;
	s->closed = true;
	if (s->is_linked)
		srpc_remove_from_drained_list(s);
	atomic_sub_and_fetch(&srpc_credit_used, s->credit);
	atomic_sub_and_fetch(&srpc_num_pending, s->num_pending);
	s->num_pending = 0;
	s->demand = 0;
	s->credit = 0;
	spin_unlock_np(&s->lock);

	if (th)
		thread_ready(th);

	atomic_dec(&srpc_num_sess);
	waitgroup_wait(&s->send_waiter);
	tcp_close(c);
	sfree(s);

	/* initialize credits */
	if (atomic_read(&srpc_num_sess) == 0) {
		assert(atomic_read(&srpc_credit_used) == 0);
		assert(atomic_read(&srpc_num_drained) == 0);
		atomic_write(&srpc_credit_used, 0);
		atomic_write(&srpc_credit_pool, runtime_max_cores());
		atomic_write(&srpc_credit_ds, 0);
		fflush(stdout);
	}
}

static void srpc_cc_worker(void *arg)
{
	uint64_t us;
	float alpha;
	int new_cp;
	int credit_used;
	int credit_unused;
	int num_sess;
	struct sbw_session *ds;
	unsigned int max_cores = runtime_max_cores();
	unsigned int core_id, i;
	thread_t *th;

	while (true) {
		timer_sleep(SBW_RTT_US);
		us = runtime_queue_us();
		new_cp = atomic_read(&srpc_credit_pool);
		num_sess = atomic_read(&srpc_num_sess);
		credit_used = atomic_read(&srpc_credit_used);

		if (us >= SBW_MIN_DELAY_US) {
			// reducing credit pool
			alpha = (us - SBW_MIN_DELAY_US) / (float)SBW_MIN_DELAY_US;
			alpha = alpha * SBW_MD;
			alpha = MAX(1.0 - alpha, 0.5);

			new_cp = (int)(new_cp * alpha);
			credit_carry = 0.0;
		} else if (new_cp <= credit_used) {
			// increase credit pool when the server used up all
			credit_carry += num_sess * SBW_AI;
			if (credit_carry >= 1.0) {
				int new_credit_int = (int)credit_carry;
				new_cp += new_credit_int;
				credit_carry -= new_credit_int;
			}
		}

		new_cp = MAX(new_cp, max_cores);
		new_cp = MIN(new_cp, atomic_read(&srpc_num_sess) << SBW_MAX_WINDOW_EXP);

		// Wake up threads from drained list
		credit_unused = new_cp - credit_used;
		core_id = get_current_affinity();

		while (credit_unused > 0) {
			ds = srpc_choose_drained_session(core_id);

			i = (core_id + 1) % max_cores;
			while (!ds && i != core_id) {
				ds = srpc_choose_drained_session(i);
				i = (i + 1) % max_cores;
			}

			if (!ds)
				break;

			spin_lock_np(&ds->lock);
			BUG_ON(ds->credit > 0);
			th = ds->sender_th;
			ds->sender_th = NULL;
			ds->wake_up = true;
			ds->credit = 1;
			spin_unlock_np(&ds->lock);

			atomic_inc(&srpc_credit_used);

			if (th)
				thread_ready(th);
			credit_unused--;
		}

		atomic_write(&srpc_credit_pool, new_cp);

#if SBW_TS_OUT
		if (new_cp > 0)
			record(new_cp, us);
#endif
	}
}

static void srpc_listener(void *arg)
{
	struct netaddr laddr;
	tcpconn_t *c;
	tcpqueue_t *q;
	int ret;
	int i;

	for (i = 0 ; i < NCPU ; ++i) {
		spin_lock_init(&srpc_drained[i].lock);
		list_head_init(&srpc_drained[i].list);
	}

	atomic_write(&srpc_num_sess, 0);
	atomic_write(&srpc_num_drained, 0);

	atomic_write(&srpc_credit_pool, runtime_max_cores());
	atomic_write(&srpc_credit_used, 0);
	atomic_write(&srpc_credit_ds, 0);
	atomic_write(&srpc_num_pending, 0);
	atomic_write(&srpc_avg_st, 0);

	/* init stats */
	atomic64_write(&srpc_stat_cupdate_rx_, 0);
	atomic64_write(&srpc_stat_ecredit_tx_, 0);
	atomic64_write(&srpc_stat_req_rx_, 0);
	atomic64_write(&srpc_stat_resp_tx_, 0);

	credit_carry = 0.0;

	laddr.ip = 0;
	laddr.port = SRPC_PORT;

	ret = tcp_listen(laddr, 4096, &q);
	BUG_ON(ret);

	ret = thread_spawn(srpc_cc_worker, NULL);
	BUG_ON(ret);

	while (true) {
		ret = tcp_accept(q, &c);
		if (WARN_ON(ret))
			continue;
		ret = thread_spawn(srpc_server, c);
		WARN_ON(ret);
	}
}

int sbw_enable(srpc_fn_t handler)
{
	static DEFINE_SPINLOCK(l);
	int ret;

	spin_lock_np(&l);
	if (srpc_handler) {
		spin_unlock_np(&l);
		return -EBUSY;
	}
	srpc_handler = handler;
	spin_unlock_np(&l);

	ret = thread_spawn(srpc_listener, NULL);
	BUG_ON(ret);
	return 0;
}

uint64_t sbw_stat_cupdate_rx()
{
	return atomic64_read(&srpc_stat_cupdate_rx_);
}

uint64_t sbw_stat_ecredit_tx()
{
	return atomic64_read(&srpc_stat_ecredit_tx_);
}

uint64_t sbw_stat_credit_tx()
{
	return atomic64_read(&srpc_stat_credit_tx_);
}

uint64_t sbw_stat_req_rx()
{
	return atomic64_read(&srpc_stat_req_rx_);
}

uint64_t sbw_stat_req_dropped()
{
	return atomic64_read(&srpc_stat_req_dropped_);
}

uint64_t sbw_stat_resp_tx()
{
	return atomic64_read(&srpc_stat_resp_tx_);
}

struct srpc_ops sbw_ops = {
	.srpc_enable		= sbw_enable,
	.srpc_stat_cupdate_rx	= sbw_stat_cupdate_rx,
	.srpc_stat_ecredit_tx	= sbw_stat_ecredit_tx,
	.srpc_stat_credit_tx	= sbw_stat_credit_tx,
	.srpc_stat_req_rx	= sbw_stat_req_rx,
	.srpc_stat_req_dropped	= sbw_stat_req_dropped,
	.srpc_stat_resp_tx	= sbw_stat_resp_tx,
};
