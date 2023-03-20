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

typedef uint64_t (*get_delay_fn_t)(void);
void sbw2_register_delay_source(int stype, get_delay_fn_t dfn);

struct cbw_session;
/* for RPC client-connection */
struct cbw_conn {
	struct crpc_conn	cmn;

	struct cbw_session	*session;

	/* credit-related variables */
	bool			waiting_resp;
	uint32_t		credit;
	uint32_t		credit_used;

	/* per-connection stats */
	uint64_t		ecredit_rx_;
	uint64_t		cupdate_tx_;
	uint64_t		resp_rx_;
	uint64_t		req_tx_;
	uint64_t		credit_expired_;
};

/* for RPC client */
struct cbw_session {
	struct crpc_session	cmn;

	uint64_t		id;
	uint64_t		req_id;
	int			next_conn_idx;
	bool			running;
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
