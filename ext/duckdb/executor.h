#ifndef RUBY_DUCKDB_EXECUTOR_H
#define RUBY_DUCKDB_EXECUTOR_H

/*
 * Shared executor infrastructure for dispatching callbacks from non-Ruby
 * threads (DuckDB worker threads) to Ruby threads that can safely hold
 * the GVL.
 *
 * A global executor thread waits for callback requests from a shared queue.
 * Used as a generic mechanism for any C file that needs to dispatch callbacks
 * from non-Ruby threads.
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
void rbduckdb_executor_dispatch(rbduckdb_callback_fn callback_func, void *callback_data);

#endif
