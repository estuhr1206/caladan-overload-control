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
#include "bw2_config.h"

// #include <bw_server.h>
extern atomic_t srpc_credit_pool;
extern atomic_t srpc_credit_used;

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
	uint64_t num_successes;
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
// moving definition to runtime/sched.c for breakwater parking
// atomic_t srpc_credit_pool;

/* timestamp of the latest credit pool update */
uint64_t srpc_last_cp_update;

/* global credit used */
// moving definition to runtime/sched.c for breakwater parking
// atomic_t srpc_credit_used;

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
	// high priority sessions (demand > 0)
	// LIFO queue
	struct list_head list_h;
	// low priority sessions (demand == 0)
	// FIFO queue
	struct list_head list_l;
	void *pad[3];
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
	/* when this session has been drained (used for priority change) */
	uint64_t		drained_ts;
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
// tracking throughput
atomic64_t srpc_successes_;

#if SBW_TS_OUT
static void printRecord()
{
	int i;

	if (!ts_out)
		ts_out = fopen("timeseries.csv", "w");

	for (i = 0; i < TS_BUF_SIZE; ++i) {
		struct Event *event = &events[i];
		fprintf(ts_out, "%lu,%d,%d,%d,%d,%d,%d,%lu,%d,%lu,%d\n",
			event->timestamp, event->credit_pool,
			event->credit_used, event->num_pending,
			event->num_drained, event->num_active,
			event->num_sess, event->delay,
			event->num_cores, event->avg_st, event->num_successes);
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
	event->num_successes = atomic64_read(&srpc_successes_);
	atomic64_write(&srpc_successes_, 0); // clear every time. Going to remove record that's not on the RTT

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
	int temp_successes = 0;

	bitmap_for_each_set(slots, SBW_MAX_WINDOW, i) {
		struct sbw_ctx *c = s->slots[i];
		size_t len;
		char *buf;
		uint8_t flags = 0;

		if (!c->cmn.drop) {
			len = c->cmn.resp_len;
			buf = c->cmn.resp_buf;
			temp_successes++;
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
	atomic64_fetch_and_add(&srpc_successes_, temp_successes);

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
	if (old_credit > 0 && s->credit == 0 && !req_dropped)
		s->credit = max_overprovision;

	/* clamp to supported values */
	/* now we allow zero credit */
	s->credit = MAX(s->credit, s->num_pending);
	s->credit = MIN(s->credit, SBW_MAX_WINDOW - 1);
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

/* srpc_choose_drained_h: choose a drained session with high priority */
static struct sbw_session *srpc_choose_drained_h(int core_id)
{
	struct sbw_session *s;
	uint64_t now = microtime();
	int demand_timeout = MAX(CBW_MAX_CLIENT_DELAY_US - SBW_RTT_US, 0);

	assert(core_id >= 0);
	assert(core_id < runtime_max_cores());

	if (list_empty(&srpc_drained[core_id].list_h))
		return NULL;

	spin_lock_np(&srpc_drained[core_id].lock);

	// First check for the sessions with outdated demand
	while (true) {
		s = list_tail(&srpc_drained[core_id].list_h,
			      struct sbw_session,
			      drained_link);
		if (!s) break;

		spin_lock_np(&s->lock);
		if (now > (s->drained_ts + demand_timeout)) {
			// enough time has passed
			list_del(&s->drained_link);
			// move to low priority queue
			list_add_tail(&srpc_drained[core_id].list_l,
				      &s->drained_link);
		} else {
			spin_unlock_np(&s->lock);
			break;
		}
		spin_unlock_np(&s->lock);
	}

	if (list_empty(&srpc_drained[core_id].list_h)) {
		spin_unlock_np(&srpc_drained[core_id].lock);
		return NULL;
	}

	s = list_pop(&srpc_drained[core_id].list_h,
		     struct sbw_session,
		     drained_link);

	BUG_ON(!s->is_linked);
	s->is_linked = false;
	spin_unlock_np(&srpc_drained[core_id].lock);
	spin_lock_np(&s->lock);
	s->drained_core = -1;
	spin_unlock_np(&s->lock);
	atomic_dec(&srpc_num_drained);

	return s;
}

/* srpc_choose_drained_l: choose a drained session with low priority */
static struct sbw_session *srpc_choose_drained_l(int core_id)
{
	struct sbw_session *s;

	assert(core_id >= 0);
	assert(core_id < runtime_max_cores());

	if (list_empty(&srpc_drained[core_id].list_l))
		return NULL;

	spin_lock_np(&srpc_drained[core_id].lock);
	if (list_empty(&srpc_drained[core_id].list_l)) {
		spin_unlock_np(&srpc_drained[core_id].lock);
		return NULL;
	}

	s = list_pop(&srpc_drained[core_id].list_l,
		     struct sbw_session,
		     drained_link);

	assert(s->is_linked);
	s->is_linked = false;
	spin_unlock_np(&srpc_drained[core_id].lock);
	spin_lock_np(&s->lock);
	s->drained_core = -1;
	spin_unlock_np(&s->lock);
	atomic_dec(&srpc_num_drained);
#if SBW_TRACK_FLOW
	if (s->id == SBW_TRACK_FLOW_ID) {
		printf("[%lu] Session waken up\n", microtime());
	}
#endif

	return s;
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

/* decr_credit_pool: return decreased credit pool size with congestion control
 *
 * @ qus: queueing delay in us
 * */
static int decr_credit_pool(uint64_t qus)
{
	float alpha;
	int credit_pool = atomic_read(&srpc_credit_pool);
	int num_sess = atomic_read(&srpc_num_sess);

	alpha = (qus - SBW_DELAY_TARGET) / (float)SBW_DELAY_TARGET;
	alpha = alpha * SBW_MD;
	alpha = MAX(1.0 - alpha, 0.5);

	credit_pool = MIN((int)(credit_pool * alpha), credit_pool - 1);
	credit_carry = 0.0;

	credit_pool = MAX(credit_pool, runtime_max_cores());
	credit_pool = MIN(credit_pool, atomic_read(&srpc_num_sess) << SBW_MAX_WINDOW_EXP);

	return credit_pool;
}

/* incr_credit_pool: return increased credit pool size with congestion control
 *
 * @ qus: queueing delay in us
 * */
static int incr_credit_pool (uint64_t qus)
{
	int credit_pool = atomic_read(&srpc_credit_pool);
	int num_sess = atomic_read(&srpc_num_sess);

	credit_carry += num_sess * SBW_AI;
	if (credit_carry >= 1.0) {
		int new_credit_int = (int)credit_carry;
		credit_pool += new_credit_int;
		credit_carry -= new_credit_int;
	}

	credit_pool = MAX(credit_pool, runtime_max_cores());
	credit_pool = MIN(credit_pool, num_sess << SBW_MAX_WINDOW_EXP);

	return credit_pool;
}

/* wakeup_drained_session: wakes up drained session which will send explicit
 * credit if there is available credit in credit pool
 *
 * @num_session: the number of sessions to wake up
 * */
static void wakeup_drained_session(int num_session)
{
	unsigned int i;
	unsigned int core_id = get_current_affinity();
	unsigned int max_cores = runtime_max_cores();
	struct sbw_session *s;
	thread_t *th;

	while (num_session > 0) {
		s = srpc_choose_drained_h(core_id);

		i = (core_id + 1) % max_cores;
		while (!s && i != core_id) {
			s = srpc_choose_drained_h(i);
			i = (i + 1) % max_cores;
		}

		if (!s) {
			s = srpc_choose_drained_l(core_id);

			i = (core_id + 1) % max_cores;
			while (!s && i != core_id) {
				s = srpc_choose_drained_l(i);
				i = (i + 1) % max_cores;
			}
		}

		if (!s)
			break;

		spin_lock_np(&s->lock);
		BUG_ON(s->credit > 0);
		th = s->sender_th;
		s->sender_th = NULL;
		s->wake_up = true;
		s->credit = 1;
		spin_unlock_np(&s->lock);

		atomic_inc(&srpc_credit_used);

		if (th)
			thread_ready(th);
		num_session--;
	}
}
// Only actually runs through its code every RTT, even though it's called more often. See first if conditional
static void srpc_update_credit_pool()
{
	uint64_t now = microtime();
	uint64_t qus;
	int new_cp;
	int credit_used;
	int credit_unused;

	if (now - srpc_last_cp_update < SBW_RTT_US)
		return;

	srpc_last_cp_update = now;

	qus = runtime_queue_us();
	new_cp = atomic_read(&srpc_credit_pool);
	credit_used = atomic_read(&srpc_credit_used);

	if (qus >= SBW_DELAY_TARGET)
		new_cp = decr_credit_pool(qus);
	//else if (credit_used >= new_cp)
	else
		new_cp = incr_credit_pool(qus);

	credit_unused = new_cp - credit_used;
	wakeup_drained_session(credit_unused);
	atomic_write(&srpc_credit_pool, new_cp);

#if SBW_TS_OUT
	record(new_cp, qus);
#endif
}

/* srpc_handle_req_drop: a routine called when a request is dropped while
 * enqueueing
 *
 * @ qus: ingress queueing delay
 * */

static void srpc_handle_req_drop(uint64_t qus)
{
	uint64_t now = microtime();
	int new_cp;

	if (now - srpc_last_cp_update < SBW_RTT_US)
		return;

	srpc_last_cp_update = now;
	new_cp = decr_credit_pool(qus);
	atomic_write(&srpc_credit_pool, new_cp);

// #if SBW_TS_OUT
// 	record(new_cp, qus);
// #endif
// TODO decide if we need this.
}

static void srpc_worker(void *arg)
{
	struct sbw_ctx *c = (struct sbw_ctx *)arg;
	struct sbw_session *s = (struct sbw_session *)c->cmn.s;
	uint64_t service_time;
	uint64_t avg_st;
	thread_t *th;

	set_rpc_ctx((void *)&c->cmn);
	set_acc_qdel(runtime_queue_us() * cycles_per_us);
	c->cmn.drop = (get_acc_qdel_us() > SBW_LATENCY_BUDGET);

	if (!c->cmn.drop) {
		service_time = microtime();
		srpc_handler((struct srpc_ctx *)c);
	}

	if (!c->cmn.drop) {
		service_time = microtime() - service_time;
		avg_st = atomic_read(&srpc_avg_st);
		avg_st = (uint64_t)(avg_st - (avg_st >> 3) + (service_time >> 3));

		atomic_write(&srpc_avg_st, avg_st);
		atomic_write(&srpc_credit_ds, c->cmn.ds_credit);
	}
/*
	c->cmn.drop = false;

	if (!c->cmn.drop) {
		service_time = microtime();
		srpc_handler((struct srpc_ctx *)c);
		service_time = microtime() - service_time;
		avg_st = atomic_read(&srpc_avg_st);
		avg_st = (uint64_t)(avg_st - (avg_st >> 3) + (service_time >> 3));

		atomic_write(&srpc_avg_st, avg_st);
		atomic_write(&srpc_credit_ds, c->cmn.ds_credit);
	}
*/
	spin_lock_np(&s->lock);
	bitmap_set(s->completed_slots, c->cmn.idx);
	th = s->sender_th;
	s->sender_th = NULL;
	spin_unlock_np(&s->lock);

	// update credit pool
	if (!c->cmn.drop)
		srpc_update_credit_pool();
	else
		atomic64_inc(&srpc_stat_req_dropped_);

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
	uint64_t us;

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
		srpc_remove_from_drained_list(s);
		s->num_pending++;
		/* adjust credit if demand changed */
		if (s->credit > s->num_pending + s->demand) {
			credit_diff = s->credit - (s->num_pending + s->demand);
			s->credit = s->num_pending + s->demand;
			atomic_sub_and_fetch(&srpc_credit_used, credit_diff);
		}

		atomic_inc(&srpc_num_pending);

		us = runtime_queue_us();
		if (us >= SBW_DROP_THRESH) {
			thread_t *th;

			// precedure called when the incoming request is dropped
			srpc_handle_req_drop(us);
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

		BUG_ON(old_demand > 0);
		BUG_ON(s->drained_core > -1);
		// if s->num_pending > 0 do nothing.
		// sender thread will handle this session.
		if (s->num_pending == 0 && s->demand > 0) {
			// With positive demand
			// sender will handle this session
			if (s->num_pending == 0) {
				th = s->sender_th;
				s->sender_th = NULL;
				s->need_ecredit = true;
			}
		} else if (s->num_pending == 0) {
			// s->demand == 0
			// push the session to the low priority drained queue
			unsigned int core_id = get_current_affinity();

			spin_lock_np(&srpc_drained[core_id].lock);
			BUG_ON(s->is_linked);
			BUG_ON(s->credit > 0);
			// FIFO queue
			list_add_tail(&srpc_drained[core_id].list_l,
				      &s->drained_link);
			s->is_linked = true;
			spin_unlock_np(&srpc_drained[core_id].lock);
			s->drained_core = core_id;
			atomic_inc(&srpc_num_drained);
			s->advertised = 0;
		}

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

		/* add to the drained list if (1) credit becomes zero,
		 * (2) s is not in the list already,
		 * (3) it has no outstanding requests */
		if (credit == 0 && drained_core == -1 &&
		    bitmap_popcount(s->avail_slots, SBW_MAX_WINDOW) ==
		    SBW_MAX_WINDOW) {
			core_id = get_current_affinity();
			spin_lock_np(&s->lock);
			spin_lock_np(&srpc_drained[core_id].lock);
			BUG_ON(s->is_linked);
			BUG_ON(s->credit > 0);
			if (s->demand > 0) {
				// positive demand: drained with high priority
				// LIFO queue
				list_add(&srpc_drained[core_id].list_h,
					 &s->drained_link);
				s->drained_ts = microtime();
			} else {
				// zero demand: drained with low priority
				// FIFO queue
				list_add_tail(&srpc_drained[core_id].list_l,
					      &s->drained_link);
			}
			s->is_linked = true;
			spin_unlock_np(&srpc_drained[core_id].lock);
			s->drained_core = core_id;
			atomic_inc(&srpc_num_drained);
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
	struct rpc_session_info info;
	thread_t *th;
	int ret;

	s = smalloc(sizeof(*s));
	BUG_ON(!s);
	memset(s, 0, sizeof(*s));

	/* receive session info */
	ret = tcp_read_full(c, &info, sizeof(info));
	BUG_ON(ret <= 0);

	s->cmn.c = c;
	s->cmn.session_type = 0;
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
		//atomic_write(&srpc_credit_pool, runtime_max_cores());
		atomic_write(&srpc_credit_pool, runtime_max_cores());
		srpc_last_cp_update = microtime();
		atomic_write(&srpc_credit_ds, 0);
		fflush(stdout);
	}
}

static void srpc_listener(void *arg)
{
	waitgroup_t *wg_listener = (waitgroup_t *)arg;
	struct netaddr laddr;
	tcpconn_t *c;
	tcpqueue_t *q;
	int ret;
	int i;

	for (i = 0; i < NCPU; ++i) {
		spin_lock_init(&srpc_drained[i].lock);
		list_head_init(&srpc_drained[i].list_h);
		list_head_init(&srpc_drained[i].list_l);
	}

	atomic_write(&srpc_num_sess, 0);
	atomic_write(&srpc_num_drained, 0);
	atomic_write(&srpc_credit_pool, runtime_max_cores());
	atomic_write(&srpc_credit_used, 0);
	atomic_write(&srpc_num_pending, 0);
	atomic_write(&srpc_credit_ds, 0);
	atomic_write(&srpc_avg_st, 0);
	credit_carry = 0.0;

	srpc_last_cp_update = microtime();

	/* init stats */
	atomic64_write(&srpc_stat_cupdate_rx_, 0);
	atomic64_write(&srpc_stat_ecredit_tx_, 0);
	atomic64_write(&srpc_stat_req_rx_, 0);
	atomic64_write(&srpc_stat_resp_tx_, 0);

	laddr.ip = 0;
	laddr.port = SRPC_PORT;

	ret = tcp_listen(laddr, 4096, &q);
	BUG_ON(ret);

	waitgroup_done(wg_listener);

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
	waitgroup_t wg_listener;

	spin_lock_np(&l);
	if (srpc_handler) {
		spin_unlock_np(&l);
		return -EBUSY;
	}
	srpc_handler = handler;
	spin_unlock_np(&l);

	waitgroup_init(&wg_listener);
	waitgroup_add(&wg_listener, 1);
	ret = thread_spawn(srpc_listener, &wg_listener);
	BUG_ON(ret);

	waitgroup_wait(&wg_listener);

	return 0;
}

void sbw_drop() {
        struct srpc_ctx *ctx = (struct srpc_ctx *)get_rpc_ctx();
	ctx->drop = true;
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

// caladan-overload-control

// int get_breakwater_srpc_credit_used() {
// 	return atomic_read(&srpc_credit_used);
// }

// void notify_breakwater_parking(int* old_C_issued, int* breakwater_park_target) {
// 	int curr_cores = runtime_active_cores();
// 	int credit_pool = atomic_read(&srpc_credit_pool);
// 	// this minimum for credits (max cores) is used throughout breakwater implementation
// 	int new_credit_pool = (int) (SBW_CORE_PARK_TARGET * (credit_pool - (credit_pool / curr_cores)));
// 	new_credit_pool = MAX(runtime_max_cores(), new_credit_pool);
// 	*old_C_issued = atomic_read(&srpc_credit_used);
// 	atomic_write(&srpc_credit_pool, new_credit_pool);
// 	*breakwater_park_target = credit_pool - new_credit_pool;
// }

// void notify_breakwater_found_work(int restore) {
// 	atomic_fetch_and_add(&srpc_credit_pool, restore);
// }

struct srpc_ops sbw_ops = {
	.srpc_enable		= sbw_enable,
	.srpc_drop		= sbw_drop,
	.srpc_stat_cupdate_rx	= sbw_stat_cupdate_rx,
	.srpc_stat_ecredit_tx	= sbw_stat_ecredit_tx,
	.srpc_stat_credit_tx	= sbw_stat_credit_tx,
	.srpc_stat_req_rx	= sbw_stat_req_rx,
	.srpc_stat_req_dropped	= sbw_stat_req_dropped,
	.srpc_stat_resp_tx	= sbw_stat_resp_tx,
};
