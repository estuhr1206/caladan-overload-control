/*
 * breakwater.h - breakwater implementation for RPC layer
 */

#pragma once

#include <base/types.h>
#include <base/atomic.h>
#include <runtime/sync.h>

#include "rpc.h"

/* for RPC server */

struct sbw_ctx {
	struct srpc_ctx		cmn;
	uint64_t		ts_sent;
};

struct cbw_session;
/* for RPC client-connection */
struct cbw_conn {
	struct crpc_conn	cmn;

	struct cbw_session	*session;

	/* credit-related variables */
	bool			waiting_winupdate;
	uint32_t		win_avail;
	uint32_t		win_used;

	/* per-connection stats */
	uint64_t		winu_rx_;
	uint64_t		winu_tx_;
	uint64_t		resp_rx_;
	uint64_t		req_tx_;
	uint64_t		win_expired_;
};

/* for RPC client */
struct cbw_session {
	struct crpc_session	cmn;

	uint64_t		id;
	uint64_t		req_id;
	int			next_conn_idx;
	bool			running;
	bool			demand_sync;
	bool			init;
	mutex_t			lock;

	/* timer for request expire in the queue */
	waitgroup_t		timer_waiter;
	condvar_t		timer_cv;

	/* a queue of pending RPC requests */
	uint32_t		head;
	uint32_t		tail;
	struct crpc_ctx		*qreq[CRPC_QLEN];

	/* per-client stat */
	uint64_t		req_dropped_;
};
