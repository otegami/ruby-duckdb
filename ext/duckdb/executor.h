#ifndef RUBY_DUCKDB_EXECUTOR_H
#define RUBY_DUCKDB_EXECUTOR_H

/*
 * Shared executor infrastructure for dispatching callbacks from non-Ruby
 * threads (DuckDB worker threads) to Ruby threads that can safely hold
 * the GVL.
 *
 * Two mechanisms are provided:
 *
 * 1. Global executor thread — a single Ruby thread that processes callbacks
 *    from a shared queue. Used as bootstrap and fallback.
 *
 * 2. Per-worker proxy threads — each DuckDB worker thread gets a dedicated
 *    Ruby thread. Created via init_local_state and stored in DuckDB's
 *    per-thread local state. Eliminates the global executor bottleneck.
 */

/* Generic callback function signature */
typedef void (*rbduckdb_callback_fn)(void *data);

/* Initialize the executor subsystem (call from Init_duckdb_native) */
void rbduckdb_init_executor(void);

/* Ensure the global executor thread is running (call with GVL held) */
void rbduckdb_executor_ensure_started(void);

/*
 * Dispatch a callback to the global executor thread.
 * Called from a non-Ruby thread. Blocks until the callback completes.
 */
void rbduckdb_executor_dispatch(rbduckdb_callback_fn fn, void *data);

/*
 * Per-worker proxy thread.
 * Opaque structure — callers use the functions below.
 */
struct worker_proxy;

/*
 * Create a per-worker proxy thread.
 * Must be called from a context where GVL can be acquired (typically
 * dispatched through the global executor from init_local_state).
 */
struct worker_proxy *rbduckdb_worker_proxy_create(void);

/*
 * Dispatch a callback through a per-worker proxy.
 * Called from the DuckDB worker thread that owns this proxy.
 * Blocks until the callback completes.
 */
void rbduckdb_worker_proxy_dispatch(struct worker_proxy *proxy,
                                     rbduckdb_callback_fn fn, void *data);

/*
 * Destroy a per-worker proxy.
 * Compatible with duckdb_delete_callback_t signature (void (*)(void *)).
 * Safe to call from non-Ruby threads — only sets flags and signals OS condvar.
 */
void rbduckdb_worker_proxy_destroy(void *proxy);

#endif
