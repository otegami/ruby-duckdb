#include "ruby-duckdb.h"

/*
 * Cross-platform threading primitives.
 * MSVC (mswin) does not provide <pthread.h>.
 * MinGW-w64 (mingw, ucrt) provides <pthread.h> via winpthreads.
 */
#ifdef _MSC_VER
#include <windows.h>
#else
#include <pthread.h>
#endif

#include "executor.h"

/* ============================================================================
 * Global Executor Thread
 * ============================================================================
 *
 * A single Ruby thread that processes callback requests from non-Ruby threads.
 * DuckDB worker threads enqueue requests and block until completion.
 *
 * Modeled after FFI gem's async callback dispatcher:
 *   https://github.com/ffi/ffi/blob/master/ext/ffi_c/Function.c
 */

/* Per-callback request, stack-allocated on the DuckDB worker thread */
struct executor_request {
    rbduckdb_callback_fn callback_func;
    void *callback_data;
    int done;
#ifdef _MSC_VER
    CRITICAL_SECTION done_lock;
    CONDITION_VARIABLE done_cond;
#else
    pthread_mutex_t done_mutex;
    pthread_cond_t done_cond;
#endif
    struct executor_request *next;
};

/* Global executor state */
#ifdef _MSC_VER
static CRITICAL_SECTION g_executor_lock;
static CONDITION_VARIABLE g_executor_cond;
static int g_sync_initialized = 0;
#else
static pthread_mutex_t g_executor_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_executor_cond = PTHREAD_COND_INITIALIZER;
#endif
static struct executor_request *g_request_list = NULL;
static VALUE g_executor_thread = Qnil;
static int g_executor_started = 0;

/* Data passed to the executor wait function */
struct executor_wait_data {
    struct executor_request *request;
    int stop;
};

/* Runs without GVL: blocks on condvar waiting for a callback request */
static void *executor_wait_func(void *data) {
    struct executor_wait_data *w = (struct executor_wait_data *)data;

    w->request = NULL;

#ifdef _MSC_VER
    EnterCriticalSection(&g_executor_lock);
    while (!w->stop && g_request_list == NULL) {
        SleepConditionVariableCS(&g_executor_cond, &g_executor_lock, INFINITE);
    }
    if (g_request_list != NULL) {
        w->request = g_request_list;
        g_request_list = g_request_list->next;
    }
    LeaveCriticalSection(&g_executor_lock);
#else
    pthread_mutex_lock(&g_executor_mutex);
    while (!w->stop && g_request_list == NULL) {
        pthread_cond_wait(&g_executor_cond, &g_executor_mutex);
    }
    if (g_request_list != NULL) {
        w->request = g_request_list;
        g_request_list = g_request_list->next;
    }
    pthread_mutex_unlock(&g_executor_mutex);
#endif

    return NULL;
}

/* Unblock function: called by Ruby to interrupt the executor (e.g., VM shutdown) */
static void executor_stop_func(void *data) {
    struct executor_wait_data *w = (struct executor_wait_data *)data;

#ifdef _MSC_VER
    EnterCriticalSection(&g_executor_lock);
    w->stop = 1;
    WakeConditionVariable(&g_executor_cond);
    LeaveCriticalSection(&g_executor_lock);
#else
    pthread_mutex_lock(&g_executor_mutex);
    w->stop = 1;
    pthread_cond_signal(&g_executor_cond);
    pthread_mutex_unlock(&g_executor_mutex);
#endif
}

/* The executor thread main loop (Ruby thread) */
static VALUE executor_thread_func(void *data) {
    struct executor_wait_data w;
    w.stop = 0;

    while (!w.stop) {
        /* Release GVL and wait for a callback request */
        rb_thread_call_without_gvl(executor_wait_func, &w, executor_stop_func, &w);

        if (w.request != NULL) {
            struct executor_request *req = w.request;

            /* Execute the callback with the GVL */
            req->callback_func(req->callback_data);

            /* Signal the DuckDB worker thread that the callback is done */
#ifdef _MSC_VER
            EnterCriticalSection(&req->done_lock);
            req->done = 1;
            WakeConditionVariable(&req->done_cond);
            LeaveCriticalSection(&req->done_lock);
#else
            pthread_mutex_lock(&req->done_mutex);
            req->done = 1;
            pthread_cond_signal(&req->done_cond);
            pthread_mutex_unlock(&req->done_mutex);
#endif
        }
    }

    return Qnil;
}

/*
 * Start the global executor thread (must be called with GVL held).
 *
 * Thread safety: This function is called from Ruby methods that always run
 * with the GVL held. The GVL serializes all calls, so the g_executor_started
 * check-then-set is safe without an extra mutex.
 */
void rbduckdb_executor_ensure_started(void) {
    if (g_executor_started) return;

#ifdef _MSC_VER
    if (!g_sync_initialized) {
        InitializeCriticalSection(&g_executor_lock);
        InitializeConditionVariable(&g_executor_cond);
        g_sync_initialized = 1;
    }
#endif

    g_executor_thread = rb_thread_create(executor_thread_func, NULL);
    rb_global_variable(&g_executor_thread);
    g_executor_started = 1;
}

/*
 * Dispatch a callback to the global executor thread.
 * Called from a non-Ruby thread. Blocks until the callback completes.
 */
void rbduckdb_executor_dispatch(rbduckdb_callback_fn callback_func, void *callback_data) {
    struct executor_request req;

    req.callback_func = callback_func;
    req.callback_data = callback_data;
    req.done = 0;
    req.next = NULL;

#ifdef _MSC_VER
    InitializeCriticalSection(&req.done_lock);
    InitializeConditionVariable(&req.done_cond);

    /* Enqueue the request */
    EnterCriticalSection(&g_executor_lock);
    req.next = g_request_list;
    g_request_list = &req;
    WakeConditionVariable(&g_executor_cond);
    LeaveCriticalSection(&g_executor_lock);

    /* Wait for the executor to process our callback */
    EnterCriticalSection(&req.done_lock);
    while (!req.done) {
        SleepConditionVariableCS(&req.done_cond, &req.done_lock, INFINITE);
    }
    LeaveCriticalSection(&req.done_lock);

    DeleteCriticalSection(&req.done_lock);
#else
    pthread_mutex_init(&req.done_mutex, NULL);
    pthread_cond_init(&req.done_cond, NULL);

    /* Enqueue the request */
    pthread_mutex_lock(&g_executor_mutex);
    req.next = g_request_list;
    g_request_list = &req;
    pthread_cond_signal(&g_executor_cond);
    pthread_mutex_unlock(&g_executor_mutex);

    /* Wait for the executor to process our callback */
    pthread_mutex_lock(&req.done_mutex);
    while (!req.done) {
        pthread_cond_wait(&req.done_cond, &req.done_mutex);
    }
    pthread_mutex_unlock(&req.done_mutex);

    pthread_cond_destroy(&req.done_cond);
    pthread_mutex_destroy(&req.done_mutex);
#endif
}

/* ============================================================================
 * Per-Worker Proxy Threads
 * ============================================================================
 *
 * Each DuckDB worker thread can be assigned a dedicated Ruby proxy thread.
 * The proxy waits for callback requests via OS condvar, acquires the GVL,
 * executes the callback, and signals completion.
 *
 * This eliminates the global executor bottleneck — each worker has its own
 * condvar and its own Ruby thread, so GVL acquisition is distributed.
 */

/* GC protection array for proxy Ruby threads */
static VALUE g_proxy_threads = Qnil;

struct worker_proxy {
    VALUE ruby_thread;
    volatile int stop_requested;
    rbduckdb_callback_fn callback_func;
    void *callback_data;
    volatile int has_request;
    volatile int request_done;
    volatile int thread_exited;
#ifdef _MSC_VER
    CRITICAL_SECTION lock;
    CONDITION_VARIABLE request_cond;
    CONDITION_VARIABLE request_done_cond;
    CONDITION_VARIABLE thread_exit_cond;
#else
    pthread_mutex_t lock;
    pthread_cond_t request_cond;
    pthread_cond_t request_done_cond;
    pthread_cond_t thread_exit_cond;
#endif
};

/* Runs without GVL: proxy waits for a callback request */
static void *proxy_wait_func(void *data) {
    struct worker_proxy *proxy = (struct worker_proxy *)data;

#ifdef _MSC_VER
    EnterCriticalSection(&proxy->lock);
    while (!proxy->stop_requested && !proxy->has_request) {
        SleepConditionVariableCS(&proxy->request_cond, &proxy->lock, INFINITE);
    }
    LeaveCriticalSection(&proxy->lock);
#else
    pthread_mutex_lock(&proxy->lock);
    while (!proxy->stop_requested && !proxy->has_request) {
        pthread_cond_wait(&proxy->request_cond, &proxy->lock);
    }
    pthread_mutex_unlock(&proxy->lock);
#endif

    return NULL;
}

/* Unblock function for proxy thread (VM shutdown or Thread#kill) */
static void proxy_stop_func(void *data) {
    struct worker_proxy *proxy = (struct worker_proxy *)data;

#ifdef _MSC_VER
    EnterCriticalSection(&proxy->lock);
    proxy->stop_requested = 1;
    WakeConditionVariable(&proxy->request_cond);
    LeaveCriticalSection(&proxy->lock);
#else
    pthread_mutex_lock(&proxy->lock);
    proxy->stop_requested = 1;
    pthread_cond_signal(&proxy->request_cond);
    pthread_mutex_unlock(&proxy->lock);
#endif
}

/* The proxy thread main loop (Ruby thread) */
static VALUE proxy_thread_func(void *data) {
    struct worker_proxy *proxy = (struct worker_proxy *)data;

    while (!proxy->stop_requested) {
        /* Release GVL and wait for a request */
        rb_thread_call_without_gvl(proxy_wait_func, proxy, proxy_stop_func, proxy);

        if (proxy->stop_requested) break;

        if (proxy->has_request) {
            /* Execute the callback with the GVL held */
            proxy->callback_func(proxy->callback_data);

            /* Signal completion to the DuckDB worker thread */
#ifdef _MSC_VER
            EnterCriticalSection(&proxy->lock);
            proxy->has_request = 0;
            proxy->request_done = 1;
            WakeConditionVariable(&proxy->request_done_cond);
            LeaveCriticalSection(&proxy->lock);
#else
            pthread_mutex_lock(&proxy->lock);
            proxy->has_request = 0;
            proxy->request_done = 1;
            pthread_cond_signal(&proxy->request_done_cond);
            pthread_mutex_unlock(&proxy->lock);
#endif
        }
    }

    /* Remove ourselves from the GC protection array */
    if (g_proxy_threads != Qnil) {
        rb_ary_delete(g_proxy_threads, proxy->ruby_thread);
    }

    /*
     * Signal that this thread has finished and no longer references
     * the proxy struct. After this signal, rbduckdb_worker_proxy_destroy
     * may free the struct.
     */
#ifdef _MSC_VER
    EnterCriticalSection(&proxy->lock);
    proxy->thread_exited = 1;
    WakeConditionVariable(&proxy->thread_exit_cond);
    LeaveCriticalSection(&proxy->lock);
#else
    pthread_mutex_lock(&proxy->lock);
    proxy->thread_exited = 1;
    pthread_cond_signal(&proxy->thread_exit_cond);
    pthread_mutex_unlock(&proxy->lock);
#endif

    return Qnil;
}

/*
 * Create a per-worker proxy thread.
 * Must be called with GVL held (e.g., from the global executor callback).
 */
struct worker_proxy *rbduckdb_worker_proxy_create(void) {
    /*
     * Use calloc (not xcalloc) because rbduckdb_worker_proxy_destroy
     * frees the struct from a non-Ruby thread where xfree is unsafe.
     */
    struct worker_proxy *proxy = calloc(1, sizeof(struct worker_proxy));
    if (proxy == NULL) {
        rb_raise(rb_eNoMemError, "failed to allocate worker_proxy");
    }

    proxy->stop_requested = 0;
    proxy->has_request = 0;
    proxy->request_done = 0;
    proxy->thread_exited = 0;

#ifdef _MSC_VER
    InitializeCriticalSection(&proxy->lock);
    InitializeConditionVariable(&proxy->request_cond);
    InitializeConditionVariable(&proxy->request_done_cond);
    InitializeConditionVariable(&proxy->thread_exit_cond);
#else
    pthread_mutex_init(&proxy->lock, NULL);
    pthread_cond_init(&proxy->request_cond, NULL);
    pthread_cond_init(&proxy->request_done_cond, NULL);
    pthread_cond_init(&proxy->thread_exit_cond, NULL);
#endif

    proxy->ruby_thread = rb_thread_create(proxy_thread_func, proxy);

    /* Protect from GC */
    if (g_proxy_threads != Qnil) {
        rb_ary_push(g_proxy_threads, proxy->ruby_thread);
    }

    return proxy;
}

/*
 * Dispatch a callback through a per-worker proxy.
 * Called from the DuckDB worker thread (non-Ruby thread).
 * Blocks until the proxy completes the callback.
 */
void rbduckdb_worker_proxy_dispatch(struct worker_proxy *proxy,
                                     rbduckdb_callback_fn callback_func,
                                     void *callback_data) {
#ifdef _MSC_VER
    EnterCriticalSection(&proxy->lock);
    proxy->callback_func = callback_func;
    proxy->callback_data = callback_data;
    proxy->request_done = 0;
    proxy->has_request = 1;
    WakeConditionVariable(&proxy->request_cond);
    LeaveCriticalSection(&proxy->lock);

    /* Wait for completion */
    EnterCriticalSection(&proxy->lock);
    while (!proxy->request_done) {
        SleepConditionVariableCS(&proxy->request_done_cond, &proxy->lock, INFINITE);
    }
    LeaveCriticalSection(&proxy->lock);
#else
    pthread_mutex_lock(&proxy->lock);
    proxy->callback_func = callback_func;
    proxy->callback_data = callback_data;
    proxy->request_done = 0;
    proxy->has_request = 1;
    pthread_cond_signal(&proxy->request_cond);
    pthread_mutex_unlock(&proxy->lock);

    /* Wait for completion */
    pthread_mutex_lock(&proxy->lock);
    while (!proxy->request_done) {
        pthread_cond_wait(&proxy->request_done_cond, &proxy->lock);
    }
    pthread_mutex_unlock(&proxy->lock);
#endif
}

/*
 * Destroy a per-worker proxy.
 * Compatible with duckdb_delete_callback_t: void (*)(void *).
 * Safe to call from non-Ruby threads — uses only OS primitives.
 *
 * Blocks until the proxy thread has exited and no longer references the
 * struct, then destroys OS synchronization primitives and frees memory.
 */
void rbduckdb_worker_proxy_destroy(void *data) {
    struct worker_proxy *proxy = (struct worker_proxy *)data;
    if (proxy == NULL) return;

    /* Signal the proxy thread to stop */
#ifdef _MSC_VER
    EnterCriticalSection(&proxy->lock);
    proxy->stop_requested = 1;
    WakeConditionVariable(&proxy->request_cond);
    LeaveCriticalSection(&proxy->lock);

    /* Wait for the proxy thread to finish */
    EnterCriticalSection(&proxy->lock);
    while (!proxy->thread_exited) {
        SleepConditionVariableCS(&proxy->thread_exit_cond, &proxy->lock, INFINITE);
    }
    LeaveCriticalSection(&proxy->lock);

    DeleteCriticalSection(&proxy->lock);
#else
    pthread_mutex_lock(&proxy->lock);
    proxy->stop_requested = 1;
    pthread_cond_signal(&proxy->request_cond);
    pthread_mutex_unlock(&proxy->lock);

    /* Wait for the proxy thread to finish */
    pthread_mutex_lock(&proxy->lock);
    while (!proxy->thread_exited) {
        pthread_cond_wait(&proxy->thread_exit_cond, &proxy->lock);
    }
    pthread_mutex_unlock(&proxy->lock);

    pthread_cond_destroy(&proxy->thread_exit_cond);
    pthread_cond_destroy(&proxy->request_done_cond);
    pthread_cond_destroy(&proxy->request_cond);
    pthread_mutex_destroy(&proxy->lock);
#endif

    free(proxy);
}

/*
 * Initialize the executor subsystem.
 * Called once from Init_duckdb_native.
 */
void rbduckdb_init_executor(void) {
    g_proxy_threads = rb_ary_new();
    rb_global_variable(&g_proxy_threads);
}
