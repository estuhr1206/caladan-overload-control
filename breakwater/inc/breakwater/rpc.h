/*
 * rpc.h - an interface for remote procedure calls
 */

#pragma once

#include <runtime/net.h>
#include <runtime/tcp.h>

/*
 * Information shared between client - server
 */
struct rpc_session_info {
	int			session_type;
};

/*
 * Server API
 */

#define SRPC_PORT	8123
#define SRPC_BUF_SIZE	2048

struct srpc_session {
	tcpconn_t		*c;
	int			session_type;
};

struct srpc_ctx {
	struct srpc_session	*s;
	int			idx;
	uint64_t		id;
	size_t			req_len;
	size_t			resp_len;
	char			req_buf[SRPC_BUF_SIZE];
	char			resp_buf[SRPC_BUF_SIZE];
	bool			drop;
	bool			track;
	uint64_t		ds_credit;
	bool			should_put;
};

typedef void (*srpc_fn_t)(struct srpc_ctx *ctx);

struct srpc_ops {
	/**
	 * srpc_enable - starts the RPC server
	 * @handler: the handler function to call for each RPC.
	 *
	 * Returns 0 if successful.
	 */
	int (*srpc_enable)(srpc_fn_t handler);
	void (*srpc_drop)();

	uint64_t (*srpc_stat_cupdate_rx)();
	uint64_t (*srpc_stat_ecredit_tx)();
	uint64_t (*srpc_stat_credit_tx)();
	uint64_t (*srpc_stat_req_rx)();
	uint64_t (*srpc_stat_req_dropped)();
	uint64_t (*srpc_stat_resp_tx)();
};

/*
 * Client API
 */

#define CRPC_QLEN		16
#define CRPC_MAX_REPLICA	256

struct crpc_ctx {
	size_t			len;
	uint64_t		id;
	uint64_t		ts;
	char			buf[SRPC_BUF_SIZE];
	void*			arg;
};

// local drop handler
typedef void (*crpc_ldrop_fn_t)(struct crpc_ctx *ctx);
typedef void (*crpc_rdrop_fn_t)(void *buf, size_t len, void *arg);

struct crpc_conn {
	tcpconn_t		*c;
};

struct crpc_session {
	struct crpc_conn	*c[CRPC_MAX_REPLICA];
	int			nconns;
	crpc_ldrop_fn_t		ldrop_handler;
	crpc_rdrop_fn_t		rdrop_handler;
	int			session_type;
};

struct crpc_ops {
	/** crpc_add_connection - add a connection to a replica
	 *  @s: the RPC session to add connection to
	 *  @raddr: the remote address of replica (port must be SRPC_PORT)
	 *
	 *  Returns 0 if successful.
	 */
	int (*crpc_add_connection)(struct crpc_session *s, struct netaddr raddr);

	/**
	 * crpc_send_one - sends one RPC request
	 * @s: the RPC session to send to
	 * @ident: the unique identifier associated with the request
	 * @buf: the payload buffer to send
	 * @len: the length of @buf (up to SRPC_BUF_SIZE)
	 * @arg: (optional) arguments provided to the drop handler for a context
	 *
	 * WARNING: This function could block.
	 *
	 * On success, returns the length sent in bytes (i.e. @len). On failure,
	 * returns -ENOBUFS if the window is full. Otherwise, returns standard socket
	 * errors (< 0).
	 */
	ssize_t (*crpc_send_one)(struct crpc_session *s,
				 const void *buf, size_t len, int hash, void *arg);
	/**
	 * crpc_recv_one - receive one RPC request
	 * @s: the RPC session to receive from
	 * @buf: a buffer to store the received payload
	 * @len: the length of @buf (up to SRPC_BUF_SIZE)
	 * @arg: (optional) arguments provided to the drop handler for a context
	 *
	 * WARNING: This function could block.
	 *
	 * On success, returns the length received in bytes. On failure returns standard
	 * socket errors (<= 0).
	 */
	ssize_t (*crpc_recv_one)(struct crpc_conn *s, void *buf, size_t len, void *arg);

	/**
	 * crpc_open - creates an RPC session
	 * @raddr: the remote address to connect to (port must be SRPC_PORT)
	 * @sout: the connection session that was created
	 *
	 * WARNING: This function could block.
	 *
	 * Returns 0 if successful.
	 */
	int (*crpc_open)(struct netaddr raddr, struct crpc_session **sout,
			 int id, crpc_ldrop_fn_t ldrop_handler,
			 crpc_rdrop_fn_t rdrop_handler,
			 struct rpc_session_info *info);

	/**
	 * crpc_close - closes an RPC session
	 * @s: the session to close
	 *
	 * WARNING: This function could block.
	 */
	void (*crpc_close)(struct crpc_session *s);

	uint32_t (*crpc_credit)(struct crpc_session *s);
	void (*crpc_stat_clear)(struct crpc_session *s);
	uint64_t (*crpc_stat_ecredit_rx)(struct crpc_session *s);
	uint64_t (*crpc_stat_credit_expired)(struct crpc_session *s);
	uint64_t (*crpc_stat_cupdate_tx)(struct crpc_session *s);
	uint64_t (*crpc_stat_resp_rx)(struct crpc_session *s);
	uint64_t (*crpc_stat_req_tx)(struct crpc_session *s);
	uint64_t (*crpc_stat_req_dropped)(struct crpc_session *s);
};

/*
 * RPC implementations
 */

extern const struct srpc_ops *srpc_ops;
extern const struct crpc_ops *crpc_ops;
extern struct srpc_ops sbw_ops;
extern struct srpc_ops sbw2_ops;
extern struct crpc_ops cbw_ops;
extern struct srpc_ops ssd_ops;
extern struct crpc_ops csd_ops;
extern struct srpc_ops sdg_ops;
extern struct crpc_ops cdg_ops;
extern struct srpc_ops snc_ops;
extern struct crpc_ops cnc_ops;
