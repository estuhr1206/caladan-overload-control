/*
 * nocontrol.h - No server overload implementation
 * for RPC layer
 */

#pragma once

#include <base/types.h>
#include <runtime/sync.h>

#include "rpc.h"

/* for RPC server */

struct snc_ctx {
	struct srpc_ctx		cmn;
	uint64_t		ts;
};

struct cnc_session;
/* for RPC client-connection */
struct cnc_conn {
	struct crpc_conn	cmn;
	struct cnc_session	*session;

	/* per-connection stats */
	uint64_t		resp_rx_;
	uint64_t		req_tx_;
};

/* for RPC client */
struct cnc_session {
	struct crpc_session	cmn;
	uint64_t		req_id;

	uint64_t		id;
	int			next_conn_idx;
	mutex_t			lock;
};
