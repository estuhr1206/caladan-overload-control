/*
 * dagor.h - DAGOR implementation for RPC layer
 */

#pragma once

#include <base/types.h>
#include <base/atomic.h>
#include <runtime/sync.h>

#include "rpc.h"

/* for RPC server */

struct sdg_ctx {
	struct srpc_ctx		cmn;
	uint64_t		ts_sent;
};

struct cdg_ctx {
	struct crpc_ctx		cmn;
	int			prio;
};

struct cdg_session;
/* for RPC client-connection */
struct cdg_conn {
	struct crpc_conn	cmn;
	struct cdg_session	*session;
	int			local_prio;
	mutex_t			lock;
	condvar_t		sender_cv;
	waitgroup_t		sender_waiter;
	bool			running;

	/* per-connection stats */
	uint64_t		winu_rx_;
	uint64_t		winu_tx_;
	uint64_t		resp_rx_;
	uint64_t		req_tx_;
	uint64_t		win_expired_;

	/* a queue of pending RPC requests */
	uint32_t		head;
	uint32_t		tail;
	struct cdg_ctx		*qreq[CRPC_QLEN];
};

/* for RPC client */
struct cdg_session {
	struct crpc_session	cmn;
	uint64_t		id;
	int			next_conn_idx;
	uint64_t		req_id;
	mutex_t			lock;

	/* per-client stats */
	uint64_t		req_dropped_;
};
