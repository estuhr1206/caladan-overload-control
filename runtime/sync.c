/*
 * sync.c - support for synchronization
 */

#include <base/lock.h>
#include <base/log.h>
#include <runtime/thread.h>
#include <runtime/sync.h>
#include <runtime/timer.h>

#include "defs.h"


/*
 * Mutex support
 */

#define WAITER_FLAG (1 << 31)

/**
 * mutex_queue_tsc - returns mutex queueing delay
 * @m: the mutex to get queueing delay
 */
uint64_t mutex_queue_tsc(mutex_t *m)
{
	uint64_t cur_tsc = rdtsc();
	if (cur_tsc < m->oldest_tsc) {
		return 0;
	}

	return (cur_tsc - m->oldest_tsc);
}

/**
 * mutex_queue_us - returns mutex queueing delay in us
 * @m: the mutex to get queueing delay
 */
uint64_t mutex_queue_us(mutex_t *m)
{
	return mutex_queue_tsc(m) / cycles_per_us;
}

void __mutex_lock(mutex_t *m)
{
	thread_t *myth;

	spin_lock_np(&m->waiter_lock);

	/* did we race with mutex_unlock? */
	if (atomic_fetch_and_or(&m->held, WAITER_FLAG) == 0) {
		atomic_write(&m->held, 1);
		spin_unlock_np(&m->waiter_lock);
		return;
	}

	myth = thread_self();
	bool is_first = list_empty(&m->waiters);
	myth->ready_tsc = rdtsc();
	myth->enque_ts = myth->ready_tsc;
	list_add_tail(&m->waiters, &myth->link);
	if (is_first)
		m->oldest_tsc = myth->ready_tsc;
	thread_park_and_unlock_np(&m->waiter_lock);
}


void __mutex_unlock(mutex_t *m)
{
	thread_t *waketh;

	spin_lock_np(&m->waiter_lock);

	waketh = list_pop(&m->waiters, thread_t, link);
	if (!waketh) {
		atomic_write(&m->held, 0);
		spin_unlock_np(&m->waiter_lock);
		return;
	}

	if (list_empty(&m->waiters)) {
		m->oldest_tsc = UINT64_MAX;
	} else {
		thread_t *oldest_th = list_top(&m->waiters, thread_t, link);
		m->oldest_tsc = oldest_th->ready_tsc;
	}
	spin_unlock_np(&m->waiter_lock);
	waketh->acc_qdel += (rdtsc() - waketh->enque_ts);
	thread_ready(waketh);
}

/**
 * mutex_init - initializes a mutex
 * @m: the mutex to initialize
 */
void mutex_init(mutex_t *m)
{
	atomic_write(&m->held, 0);
	spin_lock_init(&m->waiter_lock);
	list_head_init(&m->waiters);
	m->oldest_tsc = UINT64_MAX;
}


/*
 * Read-write mutex support
 */

/**
 * rwmutex_init - initializes a rwmutex
 * @m: the rwmutex to initialize
 */
void rwmutex_init(rwmutex_t *m)
{
	spin_lock_init(&m->waiter_lock);
	list_head_init(&m->read_waiters);
	list_head_init(&m->write_waiters);
	m->count = 0;
	m->read_waiter_count = 0;
}

/**
 * rwmutex_rdlock - acquires a read lock on a rwmutex
 * @m: the rwmutex to acquire
 */
void rwmutex_rdlock(rwmutex_t *m)
{
	thread_t *myth;

	spin_lock_np(&m->waiter_lock);
	myth = thread_self();
	if (m->count >= 0) {
		m->count++;
		spin_unlock_np(&m->waiter_lock);
		return;
	}
	m->read_waiter_count++;
	list_add_tail(&m->read_waiters, &myth->link);
	thread_park_and_unlock_np(&m->waiter_lock);
}

/**
 * rwmutex_try_rdlock - attempts to acquire a read lock on a rwmutex
 * @m: the rwmutex to acquire
 *
 * Returns true if the acquire was successful.
 */
bool rwmutex_try_rdlock(rwmutex_t *m)
{
	spin_lock_np(&m->waiter_lock);
	if (m->count >= 0) {
		m->count++;
		spin_unlock_np(&m->waiter_lock);
		return true;
	}
	spin_unlock_np(&m->waiter_lock);
	return false;
}

/**
 * rwmutex_wrlock - acquires a write lock on a rwmutex
 * @m: the rwmutex to acquire
 */
void rwmutex_wrlock(rwmutex_t *m)
{
	thread_t *myth;

	spin_lock_np(&m->waiter_lock);
	myth = thread_self();
	if (m->count == 0) {
		m->count = -1;
		spin_unlock_np(&m->waiter_lock);
		return;
	}
	list_add_tail(&m->write_waiters, &myth->link);
	thread_park_and_unlock_np(&m->waiter_lock);
}

/**
 * rwmutex_try_wrlock - attempts to acquire a write lock on a rwmutex
 * @m: the rwmutex to acquire
 *
 * Returns true if the acquire was successful.
 */
bool rwmutex_try_wrlock(rwmutex_t *m)
{
	spin_lock_np(&m->waiter_lock);
	if (m->count == 0) {
		m->count = -1;
		spin_unlock_np(&m->waiter_lock);
		return true;
	}
	spin_unlock_np(&m->waiter_lock);
	return false;
}

/**
 * rwmutex_unlock - releases a rwmutex
 * @m: the rwmutex to release
 */
void rwmutex_unlock(rwmutex_t *m)
{
	thread_t *th;
	struct list_head tmp;
	list_head_init(&tmp);

	spin_lock_np(&m->waiter_lock);
	assert(m->count != 0);
	if (m->count < 0)
		m->count = 0;
	else
		m->count--;

	if (m->count == 0 && m->read_waiter_count > 0) {
		m->count = m->read_waiter_count;
		m->read_waiter_count = 0;
		list_append_list(&tmp, &m->read_waiters);
		spin_unlock_np(&m->waiter_lock);
		while (true) {
			th = list_pop(&tmp, thread_t, link);
			if (!th)
				break;
			thread_ready(th);
		}
		return;
	}

	if (m->count == 0) {
		th = list_pop(&m->write_waiters, thread_t, link);
		if (!th) {
			spin_unlock_np(&m->waiter_lock);
			return;
		}
		m->count = -1;
		spin_unlock_np(&m->waiter_lock);
		thread_ready(th);
		return;
	}

	spin_unlock_np(&m->waiter_lock);

}

/*
 * Condition variable support
 */

/**
 * condvar_wait - waits for a condition variable to be signalled
 * @cv: the condition variable to wait for
 * @m: the currently held mutex that projects the condition
 */
void condvar_wait(condvar_t *cv, mutex_t *m)
{
	thread_t *myth;

	assert_mutex_held(m);
	spin_lock_np(&cv->waiter_lock);
	myth = thread_self();
	mutex_unlock(m);
	bool is_first = list_empty(&cv->waiters);
	myth->ready_tsc = rdtsc();
	myth->enque_ts = myth->ready_tsc;
	list_add_tail(&cv->waiters, &myth->link);
	if (is_first)
		cv->oldest_tsc = myth->ready_tsc;
	thread_park_and_unlock_np(&cv->waiter_lock);

	mutex_lock(m);
}

struct condvar_timed_wait_args {
	thread_t *th;
	condvar_t *cv;
};

static void condvar_timed_wait_expired(unsigned long args) {
	struct condvar_timed_wait_args *t_args =
		(struct condvar_timed_wait_args *)args;
	thread_t *th = t_args->th;
	condvar_t *cv = t_args->cv;
	thread_t *waiter = NULL;

	// see if still th is in the waiter lists
	spin_lock_np(&cv->waiter_lock);
	list_for_each(&cv->waiters, waiter, link) {
		if (waiter == th) {
			list_del(&waiter->link);
			break;
		}
	}
	if (list_empty(&cv->waiters)) {
		cv->oldest_tsc = UINT64_MAX;
	} else {
		thread_t *oldest_th = list_top(&cv->waiters, thread_t, link);
		cv->oldest_tsc = oldest_th->ready_tsc;
	}
	spin_unlock_np(&cv->waiter_lock);

	if (waiter == th) {
		th->acc_qdel += (rdtsc() - th->enque_ts);
		thread_ready(th);
	}
}

/**
 * condvar_timed_wait - waits for a condition variable to be signalled
 * or for a timer to be expired.
 * @cv: the condition variable to wait for
 * @m: the currently held mutex that projects the condition
 * @timeout_us: amount time to wait for
 */
void condvar_timed_wait(condvar_t *cv, mutex_t *m, uint64_t timeout_us)
{
	struct kthread *k;
	struct timer_entry e;
	struct condvar_timed_wait_args args = {thread_self(), cv};
	uint64_t deadline_us = microtime() + timeout_us;

	assert_mutex_held(m);

	// schedule timer
	timer_init(&e, condvar_timed_wait_expired, (unsigned long)&args);
	timer_start(&e, deadline_us);

	// wait for signal
	condvar_wait(cv, m);

	// cancel timer
	timer_cancel(&e);
}

/**
 * condvar_signal - signals a thread waiting on a condition variable
 * @cv: the condition variable to signal
 */
void condvar_signal(condvar_t *cv)
{
	thread_t *waketh;

	spin_lock_np(&cv->waiter_lock);
	waketh = list_pop(&cv->waiters, thread_t, link);
	if (list_empty(&cv->waiters)) {
		cv->oldest_tsc = UINT64_MAX;
	} else {
		thread_t *oldest_th = list_top(&cv->waiters, thread_t, link);
		cv->oldest_tsc = oldest_th->ready_tsc;
	}
	spin_unlock_np(&cv->waiter_lock);
	if (waketh) {
		waketh->acc_qdel += (rdtsc() - waketh->enque_ts);
		thread_ready(waketh);
	}
}

/**
 * condvar_broadcast - signals all waiting threads on a condition variable
 * @cv: the condition variable to signal
 */
void condvar_broadcast(condvar_t *cv)
{
	thread_t *waketh;
	struct list_head tmp;

	list_head_init(&tmp);

	spin_lock_np(&cv->waiter_lock);
	list_append_list(&tmp, &cv->waiters);
	cv->oldest_tsc = UINT64_MAX;
	spin_unlock_np(&cv->waiter_lock);

	while (true) {
		waketh = list_pop(&tmp, thread_t, link);
		if (!waketh)
			break;
		waketh->acc_qdel += (rdtsc() - waketh->enque_ts);
		thread_ready(waketh);
	}
}

/**
 * condvar_init - initializes a condition variable
 * @cv: the condition variable to initialize
 */
void condvar_init(condvar_t *cv)
{
	spin_lock_init(&cv->waiter_lock);
	list_head_init(&cv->waiters);
	cv->oldest_tsc = UINT64_MAX;
}

/**
 * condvar_queue_tsc - returns condvar queueing delay
 * @cv: the condvar to get queueing delay
 */
uint64_t condvar_queue_tsc(condvar_t *cv)
{
	uint64_t cur_tsc = rdtsc();
	if (cur_tsc < cv->oldest_tsc)
		return 0;

	return (cur_tsc - cv->oldest_tsc);
}

/**
 * condvar_queue_tsc - returns condvar queueing delay in us
 * @cv: the condvar to get queueing delay
 */
uint64_t condvar_queue_us(condvar_t *cv)
{
	return condvar_queue_tsc(cv) / cycles_per_us;
}


/*
 * Wait group support
 */

/**
 * waitgroup_add - adds or removes waiters from a wait group
 * @wg: the wait group to update
 * @cnt: the count to add to the waitgroup (can be negative)
 *
 * If the wait groups internal count reaches zero, the waiting thread (if it
 * exists) will be signalled. The wait group must be incremented at least once
 * before calling waitgroup_wait().
 */
void waitgroup_add(waitgroup_t *wg, int cnt)
{
	thread_t *waketh;
	struct list_head tmp;

	list_head_init(&tmp);

	spin_lock_np(&wg->lock);
	wg->cnt += cnt;
	BUG_ON(wg->cnt < 0);
	if (wg->cnt == 0)
		list_append_list(&tmp, &wg->waiters);
	spin_unlock_np(&wg->lock);

	while (true) {
		waketh = list_pop(&tmp, thread_t, link);
		if (!waketh)
			break;
		thread_ready(waketh);
	}
}

/**
 * waitgroup_wait - waits for the wait group count to become zero
 * @wg: the wait group to wait on
 */
void waitgroup_wait(waitgroup_t *wg)
{
	thread_t *myth;

	spin_lock_np(&wg->lock);
	myth = thread_self();
	if (wg->cnt == 0) {
		spin_unlock_np(&wg->lock);
		return;
	}
	list_add_tail(&wg->waiters, &myth->link);
	thread_park_and_unlock_np(&wg->lock);
}

/**
 * waitgroup_init - initializes a wait group
 * @wg: the wait group to initialize
 */
void waitgroup_init(waitgroup_t *wg)
{
	spin_lock_init(&wg->lock);
	list_head_init(&wg->waiters);
	wg->cnt = 0;
}


/*
 * Barrier support
 */

/**
 * barrier_init - initializes a barrier
 * @b: the wait group to initialize
 * @count: number of threads that must wait before releasing
 */
void barrier_init(barrier_t *b, int count)
{
	spin_lock_init(&b->lock);
	list_head_init(&b->waiters);
	b->count = count;
	b->waiting = 0;
}

/**
 * barrier_wait - waits on a barrier
 * @b: the barrier to wait on
 *
 * Returns true if the calling thread releases the barrier
 */
bool barrier_wait(barrier_t *b)
{
	thread_t *th;
	struct list_head tmp;

	list_head_init(&tmp);

	spin_lock_np(&b->lock);

	if (++b->waiting >= b->count) {
		list_append_list(&tmp, &b->waiters);
		b->waiting = 0;
		spin_unlock_np(&b->lock);
		while (true) {
			th = list_pop(&tmp, thread_t, link);
			if (!th)
				break;
			thread_ready(th);
		}
		return true;
	}

	th = thread_self();
	list_add_tail(&b->waiters, &th->link);
	thread_park_and_unlock_np(&b->lock);
	return false;
}
