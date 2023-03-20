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

#include <breakwater/breakwater2.h>
#include <breakwater/sync.h>

#include "util.h"
#include "bw_proto.h"
#include "bw2_config.h"

/* time-series output */
#define SBW_TS_OUT		false
#define TS_BUF_SIZE_EXP		10
#define TS_BUF_SIZE		(1 << TS_BUF_SIZE_EXP)
#define TS_BUF_MASK		(TS_BUF_SIZE - 1)

#define SBW_TRACK_FLOW		false
#define SBW_TRACK_FLOW_ID	1

#define EWMA_WEIGHT		0.1f

#define NSESSIONTYPE		2

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
atomic_t srpc_num_sess[NSESSIONTYPE];

/* the number of drained session */
atomic_t srpc_num_drained[NSESSIONTYPE];

/* the number of active sessions */
atomic_t srpc_num_active[NSESSIONTYPE];

/* global credit pool */
atomic_t srpc_credit_pool[NSESSIONTYPE];

/* timestamp of the latest credit pool update */
uint64_t srpc_last_cp_update;

/* global credit used */
atomic_t srpc_credit_used[NSESSIONTYPE];

/* downstream credit for multi-hierarchy */
atomic_t srpc_credit_ds[NSESSIONTYPE];

/* the number of pending requests */
atomic_t srpc_num_pending[NSESSIONTYPE];

/* EWMA execution time */
atomic_t srpc_avg_st[NSESSIONTYPE];

double credit_carry[NSESSIONTYPE];

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

static struct srpc_drained_ srpc_drained[NCPU * NSESSIONTYPE]
		__attribute__((aligned(CACHE_LINE_SIZE)));

static inline int drained_idx(int session_type, int core_id) {
	return (session_type * NCPU + core_id);
}

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


/* delay tracker */
struct delay_source {
	get_delay_fn_t		delay_fn;
	struct list_node	link;
};

struct delay_source_list {
	spinlock_t		lock;
	struct list_head	list;
	void			*pad[5];
};

BUILD_ASSERT(sizeof(struct delay_source_list) == CACHE_LINE_SIZE);

static struct delay_source_list delay_list[NSESSIONTYPE]
		__attribute__((aligned(CACHE_LINE_SIZE)));

static inline uint64_t get_stype_delay(int stype) {
	uint64_t delay = 0;
	struct delay_source *s;

	if (stype >= NSESSIONTYPE)
		return 0;

	list_for_each(&delay_list[stype].list, s, link) {
		delay += s->delay_fn();
	}

	return delay;
}

void sbw2_register_delay_source(int stype, get_delay_fn_t dfn) {
	struct delay_source *d;
	if (stype >= NSESSIONTYPE)
		return;

	// create delay_source
	d = smalloc(sizeof(*d));
	BUG_ON(!d);
	d->delay_fn = dfn;

	spin_lock_np(&delay_list[stype].lock);
	list_add_tail(&delay_list[stype].list, &d->link);
	spin_unlock_np(&delay_list[stype].lock);
}

/* credit-related stats */
atomic64_t srpc_stat_cupdate_rx_;
atomic64_t srpc_stat_ecredit_tx_;
atomic64_t srpc_stat_credit_tx_;
atomic64_t srpc_stat_req_rx_;
atomic64_t srpc_stat_req_dropped_;
atomic64_t srpc_stat_resp_tx_;

/* throughput-based credit management */
spinlock_t srpc_cm_lock;
atomic64_t srpc_cm_in_cnt;
atomic64_t srpc_cm_out_cnt;
atomic64_t srpc_cm_drop_cnt;
atomic64_t srpc_cm_last_in;
atomic64_t srpc_cm_last_out;
uint64_t srpc_cm_last_update;
bool srpc_cm_reset_stat;
bool srpc_cm_req_dropped;

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
	event->credit_used = atomic_read(&srpc_credit_used[0]);
	event->num_pending = atomic_read(&srpc_num_pending[0]);
	event->num_drained = atomic_read(&srpc_num_drained[0]);
	event->num_active = atomic_read(&srpc_num_active[0]);
	event->num_sess = atomic_read(&srpc_num_sess[0]);
	event->delay = delay;
	event->num_cores = runtime_active_cores();
	event->avg_st = atomic_read(&srpc_avg_st[0]);

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
		s->slots[slot]->cmn.track = false;
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
	bitmap_for_each_set(slots, SBW_MAX_WINDOW, i) {
		srpc_put_slot(s, i);
	}

#if SBW_TRACK_FLOW
	if (s->id == SBW_TRACK_FLOW_ID) {
		printf("[%lu] <=== Response (%d): credit=%d\n",
			microtime(), nrhdr, s->credit);
	}
#endif
	atomic_sub_and_fetch(&srpc_num_pending[s->cmn.session_type], nrhdr);
	atomic64_fetch_and_add(&srpc_stat_resp_tx_, nrhdr);

	if (unlikely(ret < 0))
		return ret;
	return 0;
}

static void srpc_update_credit(struct sbw_session *s, bool req_dropped)
{
	int stype = s->cmn.session_type;
	int credit_pool = atomic_read(&srpc_credit_pool[stype]);
	int credit_ds = atomic_read(&srpc_credit_ds[stype]);
	int credit_used = atomic_read(&srpc_credit_used[stype]);
	int num_sess = atomic_read(&srpc_num_sess[stype]);
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
	atomic_fetch_and_add(&srpc_credit_used[stype], credit_diff);
#if SBW_TRACK_FLOW
	if (s->id == SBW_TRACK_FLOW_ID) {
		printf("[%lu] credit update: credit_pool = %d, credit_used = %d, req_dropped = %d, num_pending = %d, demand = %d, num_sess = %d, old_credit = %d, new_credit = %d\n",
		       microtime(), credit_pool, credit_used, req_dropped, s->num_pending, s->demand, num_sess, old_credit, s->credit);
	}
#endif
}

/* srpc_choose_drained_h: choose a drained session with high priority */
static struct sbw_session *srpc_choose_drained_h(int stype, int core_id)
{
	struct sbw_session *s;
	uint64_t now = microtime();
	int demand_timeout = MAX(CBW_MAX_CLIENT_DELAY_US - SBW_RTT_US, 0);
	int didx = drained_idx(stype, core_id);

	assert(core_id >= 0);
	assert(core_id < runtime_max_cores());

	if (list_empty(&srpc_drained[didx].list_h))
		return NULL;

	spin_lock_np(&srpc_drained[didx].lock);

	// First check for the sessions with outdated demand
	while (true) {
		s = list_tail(&srpc_drained[didx].list_h,
			      struct sbw_session,
			      drained_link);
		if (!s) break;

		spin_lock_np(&s->lock);
		if (now > (s->drained_ts + demand_timeout)) {
			// enough time has passed
			list_del(&s->drained_link);
			// move to low priority queue
			list_add_tail(&srpc_drained[didx].list_l,
				      &s->drained_link);
		} else {
			spin_unlock_np(&s->lock);
			break;
		}
		spin_unlock_np(&s->lock);
	}

	if (list_empty(&srpc_drained[didx].list_h)) {
		spin_unlock_np(&srpc_drained[didx].lock);
		return NULL;
	}

	s = list_pop(&srpc_drained[didx].list_h,
		     struct sbw_session,
		     drained_link);

	BUG_ON(!s->is_linked);
	s->is_linked = false;
	spin_unlock_np(&srpc_drained[didx].lock);
	spin_lock_np(&s->lock);
	s->drained_core = -1;
	spin_unlock_np(&s->lock);
	atomic_dec(&srpc_num_drained[s->cmn.session_type]);

	return s;
}

/* srpc_choose_drained_l: choose a drained session with low priority */
static struct sbw_session *srpc_choose_drained_l(int stype, int core_id)
{
	struct sbw_session *s;
	int didx = drained_idx(stype, core_id);

	assert(core_id >= 0);
	assert(core_id < runtime_max_cores());

	if (list_empty(&srpc_drained[didx].list_l))
		return NULL;

	spin_lock_np(&srpc_drained[didx].lock);
	if (list_empty(&srpc_drained[didx].list_l)) {
		spin_unlock_np(&srpc_drained[didx].lock);
		return NULL;
	}

	s = list_pop(&srpc_drained[didx].list_l,
		     struct sbw_session,
		     drained_link);

	assert(s->is_linked);
	s->is_linked = false;
	spin_unlock_np(&srpc_drained[didx].lock);
	spin_lock_np(&s->lock);
	s->drained_core = -1;
	spin_unlock_np(&s->lock);
	atomic_dec(&srpc_num_drained[s->cmn.session_type]);
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

	int didx = drained_idx(s->cmn.session_type, s->drained_core);

	spin_lock_np(&srpc_drained[didx].lock);
	if (s->is_linked) {
		list_del(&s->drained_link);
		s->is_linked = false;
		atomic_dec(&srpc_num_drained[s->cmn.session_type]);
#if SBW_TRACK_FLOW
		if (s->id == SBW_TRACK_FLOW_ID) {
			printf("[%lu] Seesion is removed from drained list\n",
			       microtime());
		}
#endif
	}
	spin_unlock_np(&srpc_drained[didx].lock);
	s->drained_core = -1;
}

/* decr_credit_pool: return decreased credit pool size with congestion control
 *
 * @ stype: session type
 * @ qus: queueing delay in us
 * */
static int decr_credit_pool(int stype, uint64_t qus)
{
	int credit_pool = atomic_read(&srpc_credit_pool[stype]);
	int num_sess = atomic_read(&srpc_num_sess[stype]);

	credit_pool -= MAX(1, (int)(num_sess * SBW_AI));
	credit_carry[stype] = 0.0;

	credit_pool = MAX(credit_pool, runtime_max_cores());
	credit_pool = MIN(credit_pool, atomic_read(&srpc_num_sess[stype]) << SBW_MAX_WINDOW_EXP);

	return credit_pool;
}

/* incr_credit_pool: return increased credit pool size with congestion control
 *
 * @ stype: session type
 * @ qus: queueing delay in us
 * */
static int incr_credit_pool(int stype, uint64_t qus)
{
	int credit_pool = atomic_read(&srpc_credit_pool[stype]);
	int num_sess = atomic_read(&srpc_num_sess[stype]);

	credit_carry[stype] += MAX(num_sess * SBW_AI, 1.0);
	if (credit_carry[stype] >= 1.0) {
		int new_credit_int = (int)credit_carry[stype];
		credit_pool += new_credit_int;
		credit_carry[stype] -= new_credit_int;
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
static void wakeup_drained_session(int stype, int num_session)
{
	unsigned int i;
	unsigned int core_id = get_current_affinity();
	unsigned int max_cores = runtime_max_cores();
	struct sbw_session *s;
	thread_t *th;

	while (num_session > 0) {
		s = srpc_choose_drained_h(stype, core_id);

		i = (core_id + 1) % max_cores;
		while (!s && i != core_id) {
			s = srpc_choose_drained_h(stype, i);
			i = (i + 1) % max_cores;
		}

		if (!s) {
			s = srpc_choose_drained_l(stype, core_id);

			i = (core_id + 1) % max_cores;
			while (!s && i != core_id) {
				s = srpc_choose_drained_l(stype, i);
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

		atomic_inc(&srpc_credit_used[s->cmn.session_type]);

		if (th)
			thread_ready(th);
		num_session--;
	}
}

static void srpc_update_credit_pool()
{
	uint64_t now = microtime();
	uint64_t in_cnt;
	uint64_t out_cnt;
	uint64_t last_in_cnt;
	uint64_t last_out_cnt;
	int old_cp;
	int new_cp;
	int credit_used;
	int credit_unused;
	uint64_t tdiff;

	spin_lock_np(&srpc_cm_lock);
	tdiff = now - srpc_cm_last_update;
	if (tdiff < SRPC_CM_P99_RTT) {
//        if (tdiff < 2 * (atomic_read(&srpc_avg_st[0]) + 10)) {
		spin_unlock_np(&srpc_cm_lock);
		return;
	}

	if (!srpc_cm_reset_stat) {
		atomic64_write(&srpc_cm_in_cnt, 0);
		atomic64_write(&srpc_cm_out_cnt, 0);
		atomic64_write(&srpc_cm_drop_cnt, 0);
		srpc_cm_reset_stat = true;
	}

	if (tdiff < SRPC_CM_UPDATE_INTERVAL) {
//        if (tdiff < 4 * (atomic_read(&srpc_avg_st[0]) + 10)) {
		spin_unlock_np(&srpc_cm_lock);
		return;
	}

	srpc_cm_last_update = now;
	srpc_cm_reset_stat = false;
	spin_unlock_np(&srpc_cm_lock);

	// TODO: support multiple session type
	credit_used = atomic_read(&srpc_credit_used[0]);
	new_cp = atomic_read(&srpc_credit_pool[0]);
	old_cp = new_cp;
	in_cnt = atomic64_read(&srpc_cm_in_cnt);
	out_cnt = atomic64_read(&srpc_cm_out_cnt);
	last_in_cnt = atomic64_read(&srpc_cm_last_in);
	last_out_cnt = atomic64_read(&srpc_cm_last_out);

	double slope = 0.0;

	if (in_cnt == 0) {
		new_cp = incr_credit_pool(0, 0);
	}else if (in_cnt >= last_in_cnt) {
		// INCR phase
		if (in_cnt > last_in_cnt) slope = (int)(out_cnt - last_out_cnt) / (double)((int)(in_cnt - last_in_cnt));
		else if (out_cnt > last_out_cnt) slope = 10.0;
		else if (out_cnt < last_out_cnt) slope = -10.0;
		else slope = 0.0;
		if (out_cnt > last_out_cnt &&
		    SRPC_CM_SLOPE_INV * (out_cnt - last_out_cnt) >= (in_cnt - last_in_cnt) &&
		    credit_used >= new_cp) {
			// slope > 0.25
			new_cp = incr_credit_pool(0, 0);
			//printf("[%lu] INCR increase credit pool: out = %ld, in = %ld, cp:%d -> %d\n",
			//       now, out_cnt - last_out_cnt, in_cnt - last_in_cnt, old_cp, new_cp);
			/*
			printf("%lu,%lf,%d,%d,%d,%lf\n",
			       now, slope, new_cp, credit_used,
			       atomic_read(&srpc_num_pending[0]), runtime_load());
			*/
		} else {
			new_cp = decr_credit_pool(0, 0);
			//printf("[%lu] INCR decrease credit pool: out = %ld, in = %ld, cp:%d -> %d\n",
			//       now, out_cnt - last_out_cnt, in_cnt - last_in_cnt, old_cp, new_cp);
			/*
			printf("%lu,%lf,%d,%d,%d,%lf\n",
			       now, slope, new_cp, credit_used,
			       atomic_read(&srpc_num_pending[0]), runtime_load());
			*/
		}
	} else {
		// DECR phase
		if (last_out_cnt > out_cnt &&
		    SRPC_CM_SLOPE_INV * (last_out_cnt - out_cnt) > (last_in_cnt - in_cnt) &&
		    credit_used >= new_cp) {
			new_cp = incr_credit_pool(0, 0);
			//printf("[%lu] DECR increase credit pool: out = %ld, in = %ld, cp:%d -> %d\n",
			//       now, out_cnt - last_out_cnt, in_cnt - last_in_cnt, old_cp, new_cp);
			/*
			printf("%lu,%lf,%d,%d,%d,%lf\n",
			       now, slope, new_cp, credit_used,
			       atomic_read(&srpc_num_pending[0]), runtime_load());
			*/
		} else {
			new_cp = decr_credit_pool(0, 0);
			//printf("[%lu] DECR decrease credit pool: out = %ld, in = %ld, cp:%d -> %d\n",
			//       now, out_cnt - last_out_cnt, in_cnt - last_in_cnt, old_cp, new_cp);
			/*
			printf("%lu,%lf,%d,%d,%d,%lf\n",
			       now, slope, new_cp, credit_used,
			       atomic_read(&srpc_num_pending[0]), runtime_load());
			*/
		}
	}

	credit_unused = new_cp - credit_used;
	wakeup_drained_session(0, credit_unused);
	atomic_write(&srpc_credit_pool[0], new_cp);

	atomic64_write(&srpc_cm_last_in, in_cnt);
	atomic64_write(&srpc_cm_last_out, out_cnt);
	atomic64_write(&srpc_cm_in_cnt, 0);
	atomic64_write(&srpc_cm_out_cnt, 0);
	atomic64_write(&srpc_cm_drop_cnt, 0);
}

/* srpc_handle_req_drop: a routine called when a request is dropped while
 * enqueueing
 *
 * @ stype: session type
 * @ qus: ingress queueing delay
 * */
static void srpc_handle_req_drop(int stype, uint64_t qus)
{
	// INHO
	srpc_update_credit_pool();
	atomic64_inc(&srpc_cm_drop_cnt);
}
/*
static void srpc_handle_req_drop(int stype, uint64_t qus)
{
	uint64_t now = microtime();
	int new_cp;

	spin_lock_np(&srpc_cm_lock);

	if (now - srpc_cm_last_update < SRPC_CM_UPDATE_INTERVAL) {
		spin_unlock_np(&srpc_cm_lock);
		return;
	}

	srpc_cm_last_update = now;
	srpc_cm_reset_stat = false;
	spin_unlock_np(&srpc_cm_lock);

	new_cp = decr_credit_pool(stype, qus);

	atomic_write(&srpc_credit_pool[stype], new_cp);
	atomic64_write(&srpc_cm_last_in, atomic64_read(&srpc_cm_in_cnt));
	atomic64_write(&srpc_cm_last_out, atomic64_read(&srpc_cm_out_cnt));
	atomic64_write(&srpc_cm_in_cnt, 0);
	atomic64_write(&srpc_cm_out_cnt, 0);

#if SBW_TS_OUT
	record(new_cp, qus);
#endif
}*/

static void srpc_worker(void *arg)
{
	struct sbw_ctx *c = (struct sbw_ctx *)arg;
	struct sbw_session *s = (struct sbw_session *)c->cmn.s;
	uint64_t service_time;
	uint64_t avg_st;
	thread_t *th;
	int stype = s->cmn.session_type;
	uint64_t now = rdtsc();

	set_rpc_ctx((void *)&c->cmn);
	set_acc_qdel(runtime_queue_us() * cycles_per_us);
	// INHO
	//c->cmn.drop = (get_acc_qdel_us() > SBW_LATENCY_BUDGET);
	c->cmn.drop = false;

	if (!c->cmn.drop) {
		srpc_handler((struct srpc_ctx *)c);
	}

	if (!c->cmn.drop) {
		service_time = (rdtsc() - now) / cycles_per_us;
		avg_st = atomic_read(&srpc_avg_st[0]);
		avg_st = (uint64_t)(avg_st - (avg_st >> 3) + (service_time >> 3));

		atomic_write(&srpc_avg_st[0], avg_st);
		atomic_write(&srpc_credit_ds, c->cmn.ds_credit);
		atomic64_inc(&srpc_cm_out_cnt);
	}

	spin_lock_np(&s->lock);
	bitmap_set(s->completed_slots, c->cmn.idx);
	th = s->sender_th;
	s->sender_th = NULL;
	spin_unlock_np(&s->lock);

	// INHO
	srpc_update_credit_pool();

	if (c->cmn.drop) {
		atomic64_inc(&srpc_stat_req_dropped_);
	}
/*
	if (c->cmn.drop) {
		int new_cp = decr_credit_pool(0, 0);
		atomic_write(&srpc_credit_pool[0], new_cp);
		atomic64_inc(&srpc_stat_req_dropped_);
	} else {
		srpc_update_credit_pool();
	}
*/
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
		c->cmn.track = (s->id == 1);
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
			atomic_sub_and_fetch(&srpc_credit_used[s->cmn.session_type], credit_diff);
		}

		atomic_inc(&srpc_num_pending[s->cmn.session_type]);
		atomic64_inc(&srpc_cm_in_cnt);

		us = runtime_queue_us();
		//us = get_stype_delay(s->cmn.session_type);
		if (us > SBW_LATENCY_BUDGET) {
		    //atomic64_read(&srpc_credit_used[0]) > atomic64_read(&srpc_credit_pool[0])) {
			thread_t *th;

			// precedure called when the incoming request is dropped
			srpc_handle_req_drop(s->cmn.session_type, us);
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
			int didx = drained_idx(s->cmn.session_type, core_id);

			spin_lock_np(&srpc_drained[didx].lock);
			BUG_ON(s->is_linked);
			BUG_ON(s->credit > 0);
			// FIFO queue
			list_add_tail(&srpc_drained[didx].list_l,
				      &s->drained_link);
			s->is_linked = true;
			spin_unlock_np(&srpc_drained[didx].lock);
			s->drained_core = core_id;
			atomic_inc(&srpc_num_drained[s->cmn.session_type]);
			s->advertised = 0;
		}

		/* adjust credit if demand changed */
		if (s->credit > s->num_pending + s->demand) {
			credit_diff = s->credit - (s->num_pending + s->demand);
			s->credit = s->num_pending + s->demand;
			atomic_sub_and_fetch(&srpc_credit_used[s->cmn.session_type], credit_diff);
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
	int didx;
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
			didx = drained_idx(s->cmn.session_type, core_id);
			spin_lock_np(&s->lock);
			spin_lock_np(&srpc_drained[didx].lock);
			BUG_ON(s->is_linked);
			BUG_ON(s->credit > 0);
			if (s->demand > 0) {
				// positive demand: drained with high priority
				// LIFO queue
				list_add(&srpc_drained[didx].list_h,
					 &s->drained_link);
				s->drained_ts = microtime();
			} else {
				// zero demand: drained with low priority
				// FIFO queue
				list_add_tail(&srpc_drained[didx].list_l,
					      &s->drained_link);
			}
			s->is_linked = true;
			spin_unlock_np(&srpc_drained[didx].lock);
			s->drained_core = core_id;
			atomic_inc(&srpc_num_drained[s->cmn.session_type]);
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
	int stype;
	int ret;

	s = smalloc(sizeof(*s));
	BUG_ON(!s);
	memset(s, 0, sizeof(*s));

	/* receive session info */
	ret = tcp_read_full(c, &info, sizeof(info));
	BUG_ON(ret <= 0);

	stype = info.session_type;
	if (stype >= NSESSIONTYPE)
		stype = 0;
	s->cmn.c = c;
	s->cmn.session_type = stype;
	s->drained_core = -1;
	s->id = atomic_fetch_and_add(&srpc_num_sess[stype], 1) + 1;
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
	atomic_sub_and_fetch(&srpc_credit_used[stype], s->credit);
	atomic_sub_and_fetch(&srpc_num_pending[stype], s->num_pending);
	s->num_pending = 0;
	s->demand = 0;
	s->credit = 0;
	spin_unlock_np(&s->lock);

	if (th)
		thread_ready(th);

	stype = s->cmn.session_type;
	atomic_dec(&srpc_num_sess[stype]);
	waitgroup_wait(&s->send_waiter);
	tcp_close(c);
	sfree(s);

	/* initialize credits */
	if (atomic_read(&srpc_num_sess[stype]) == 0) {
		assert(atomic_read(&srpc_credit_used[stype]) == 0);
		assert(atomic_read(&srpc_num_drained[stype]) == 0);
		atomic_write(&srpc_credit_used[stype], 0);
		// INHO
		atomic_write(&srpc_credit_pool[stype], 10 * runtime_max_cores());
		//atomic_write(&srpc_credit_pool[stype], 64000);
		srpc_last_cp_update = microtime();
		atomic_write(&srpc_credit_ds[stype], 0);
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

	for (i = 0 ; i < NCPU * NSESSIONTYPE ; ++i) {
		spin_lock_init(&srpc_drained[i].lock);
		list_head_init(&srpc_drained[i].list_h);
		list_head_init(&srpc_drained[i].list_l);
	}

	for (i = 0; i < NSESSIONTYPE; ++i) {
		atomic_write(&srpc_num_sess[i], 0);
		atomic_write(&srpc_num_drained[i], 0);
		// INHO
		atomic_write(&srpc_credit_pool[i], 10 * runtime_max_cores());
		//atomic_write(&srpc_credit_pool[i], 64000);
		atomic_write(&srpc_credit_used[i], 0);
		atomic_write(&srpc_num_pending[i], 0);
		atomic_write(&srpc_credit_ds[i], 0);
		atomic_write(&srpc_avg_st[i], 0);
		credit_carry[i] = 0.0;

		spin_lock_init(&delay_list[i].lock);
		list_head_init(&delay_list[i].list);
		sbw2_register_delay_source(i, runtime_queue_us);
	}

	srpc_last_cp_update = microtime();

	/* init stats */
	atomic64_write(&srpc_stat_cupdate_rx_, 0);
	atomic64_write(&srpc_stat_ecredit_tx_, 0);
	atomic64_write(&srpc_stat_req_rx_, 0);
	atomic64_write(&srpc_stat_resp_tx_, 0);

	spin_lock_init(&srpc_cm_lock);
	atomic64_write(&srpc_cm_in_cnt, 0);
	atomic64_write(&srpc_cm_out_cnt, 0);
	atomic64_write(&srpc_cm_drop_cnt, 0);
	atomic64_write(&srpc_cm_last_in, 0);
	atomic64_write(&srpc_cm_last_out, 0);
	srpc_cm_last_update = microtime();
	srpc_cm_reset_stat = false;

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

int sbw2_enable(srpc_fn_t handler)
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

	printf("Running Breakwater2\n");
	fflush(stdout);

	waitgroup_init(&wg_listener);
	waitgroup_add(&wg_listener, 1);
	ret = thread_spawn(srpc_listener, &wg_listener);
	BUG_ON(ret);

	waitgroup_wait(&wg_listener);

	return 0;
}

void sbw2_drop()
{
        struct srpc_ctx *ctx = (struct srpc_ctx *)get_rpc_ctx();
	ctx->drop = true;
}

uint64_t sbw2_stat_cupdate_rx()
{
	return atomic64_read(&srpc_stat_cupdate_rx_);
}

uint64_t sbw2_stat_ecredit_tx()
{
	return atomic64_read(&srpc_stat_ecredit_tx_);
}

uint64_t sbw2_stat_credit_tx()
{
	return atomic64_read(&srpc_stat_credit_tx_);
}

uint64_t sbw2_stat_req_rx()
{
	return atomic64_read(&srpc_stat_req_rx_);
}

uint64_t sbw2_stat_req_dropped()
{
	return atomic64_read(&srpc_stat_req_dropped_);
}

uint64_t sbw2_stat_resp_tx()
{
	return atomic64_read(&srpc_stat_resp_tx_);
}

struct srpc_ops sbw2_ops = {
	.srpc_enable		= sbw2_enable,
	.srpc_drop		= sbw2_drop,
	.srpc_stat_cupdate_rx	= sbw2_stat_cupdate_rx,
	.srpc_stat_ecredit_tx	= sbw2_stat_ecredit_tx,
	.srpc_stat_credit_tx	= sbw2_stat_credit_tx,
	.srpc_stat_req_rx	= sbw2_stat_req_rx,
	.srpc_stat_req_dropped	= sbw2_stat_req_dropped,
	.srpc_stat_resp_tx	= sbw2_stat_resp_tx,
};
