#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdalign.h>
#include <stdatomic.h>
#include <sys/eventfd.h>

#include "rvn.h"
#include "rvn_internal.h"
#include "status_codes.h"
#include "internal_posix.h"

/*
 * A process wide io_uring used only to prefetch file ranges into the page cache (FADVISE / WILLNEED).
 * It is separate from the journal ring: prefetches are hints, they are fire and forget, and they must never
 * queue behind (or in front of) durable writes.
 *
 * The pager registers a data file with a ring when it opens it (OPEN_FILE_PREFETCH) and unregisters it when the last handle
 * closes. A registration puts the file in the ring's fixed file table; the ring holds its own reference to the
 * file, so a request can never reach another file that reused the same descriptor number. The pager keeps a
 * ticket = (generation << 32) | slot. Every registration and unregistration increments the slot's generation, so
 * the submitter drops queued requests whose ticket is from an earlier registration. A request that the kernel
 * already holds keeps its file reference and completes harmlessly.
 *
 * Producers (any thread) push to a bounded lock-free queue; a full queue drops the request (it is only a hint).
 * One submitter thread drains the queue, submits in batches and reaps the completions.
 *
 * A ring is an object: rvn_prefetch_ring_create / rvn_prefetch_ring_destroy. The server keeps one for the process;
 * a ring can only be destroyed once no file is registered with it.
 *
 * Where io_uring is not allowed (an old kernel or a seccomp policy, as in default Docker), the same queue is drained by
 * the same thread with posix_fadvise on a duplicate of each registered descriptor, so callers never wait either way.
 * A request that races the unregistration of its file may reach the descriptor number after it was reused: that is a
 * wasted read-ahead, never a write.
 */

#define CACHE_LINE 64

struct prefetch_request
{
    /* Vyukov bounded queue: a cell at position pos is free while sequence == pos, and holds a request
       while sequence == pos + 1; emptying it sets sequence = pos + queue size (free for the next lap) */
    _Atomic uint64_t sequence;
    int64_t ticket;
    uint64_t offset;
    uint32_t length;
};

struct prefetch_state
{
    /* written by every producer */
    alignas(CACHE_LINE) _Atomic uint64_t enqueue_pos;
    _Atomic uint64_t dropped_full;
    /* written by the submitter, read by every producer */
    alignas(CACHE_LINE) _Atomic int32_t sleeping;
    _Atomic int32_t stopping;
    /* owned by the submitter thread (the only consumer); the counters are read by rvn_prefetch_ring_stats */
    alignas(CACHE_LINE) uint64_t dequeue_pos;
    uint32_t inflight;
    _Atomic uint64_t submitted;
    _Atomic uint64_t dropped_stale;
    _Atomic uint64_t failed;

    /* set once at init */
    alignas(CACHE_LINE) struct io_uring ring;
    bool use_io_uring;
    int *fds; /* without io_uring: a duplicate of each registered file's descriptor, -1 when free */
    int eventfd;
    pthread_t thread;
    uint32_t ring_entries;
    uint64_t queue_mask;
    int32_t max_files;

    /* registration, guarded by slots_lock */
    pthread_mutex_t slots_lock;
    int32_t free_head;
    int32_t free_count;
    int32_t *free_slots; /* FIFO ring of free slot numbers, so a released slot is reused as late as possible */
    _Atomic uint32_t *generations;

    struct prefetch_request *queue;
};

static inline int32_t _ticket_slot(int64_t ticket) { return (int32_t)(uint32_t)ticket; }
static inline uint32_t _ticket_generation(int64_t ticket) { return (uint32_t)((uint64_t)ticket >> 32); }
static inline int64_t _make_ticket(int32_t slot, uint32_t generation) { return (int64_t)(((uint64_t)generation << 32) | (uint32_t)slot); }

PRIVATE bool
_prefetch_dequeue(struct prefetch_state *s, struct prefetch_request *out)
{
    struct prefetch_request *cell = &s->queue[s->dequeue_pos & s->queue_mask];
    if (atomic_load_explicit(&cell->sequence, memory_order_acquire) != s->dequeue_pos + 1)
        return false; /* empty: the producer has not published this cell yet */
    out->ticket = cell->ticket;
    out->offset = cell->offset;
    out->length = cell->length;
    atomic_store_explicit(&cell->sequence, s->dequeue_pos + s->queue_mask + 1, memory_order_release);
    s->dequeue_pos++;
    return true;
}

PRIVATE void
_prefetch_complete(struct prefetch_state *s, struct io_uring_cqe *cqe)
{
    if (cqe->res < 0)
        atomic_fetch_add_explicit(&s->failed, 1, memory_order_relaxed);
    io_uring_cqe_seen(&s->ring, cqe);
    s->inflight--;
}

PRIVATE void
_prefetch_reap(struct prefetch_state *s, bool wait_for_one)
{
    struct io_uring_cqe *cqe;
    if (wait_for_one && s->inflight > 0 && io_uring_wait_cqe(&s->ring, &cqe) == 0)
        _prefetch_complete(s, cqe);
    while (s->inflight > 0 && io_uring_peek_cqe(&s->ring, &cqe) == 0)
        _prefetch_complete(s, cqe);
}

PRIVATE void
_prefetch_submit(struct prefetch_state *s)
{
    int rc = io_uring_submit(&s->ring);
    if (rc > 0)
        s->inflight += (uint32_t)rc; /* only what the kernel took; the rest stays in the SQ for the next submit */
}

/* An SQE for the next request. In flight + prepared never exceed the SQ size, and the CQ is twice the SQ size,
   so completions cannot overflow; when the ring is full we submit what we have and wait for one completion. */
PRIVATE struct io_uring_sqe *
_prefetch_get_sqe(struct prefetch_state *s)
{
    if (s->inflight + io_uring_sq_ready(&s->ring) >= s->ring_entries)
    {
        _prefetch_submit(s);
        _prefetch_reap(s, true);
    }
    return io_uring_get_sqe(&s->ring);
}

PRIVATE void *
rvn_prefetch_ring_submitter(void *arg)
{
    struct prefetch_state *s = arg;
    struct prefetch_request req;
    while (atomic_load_explicit(&s->stopping, memory_order_acquire) == 0)
    {
        while (_prefetch_dequeue(s, &req))
        {
            int32_t slot = _ticket_slot(req.ticket);
            if (atomic_load_explicit(&s->generations[slot], memory_order_acquire) != _ticket_generation(req.ticket))
            {
                atomic_fetch_add_explicit(&s->dropped_stale, 1, memory_order_relaxed);
                continue;
            }
            if (s->use_io_uring == false)
            {
                int fd = s->fds[slot];
                if (fd == -1 || posix_fadvise(fd, (off_t)req.offset, (off_t)req.length, POSIX_FADV_WILLNEED) != 0)
                    atomic_fetch_add_explicit(&s->failed, 1, memory_order_relaxed);
                else
                    atomic_fetch_add_explicit(&s->submitted, 1, memory_order_relaxed);
                continue;
            }
            struct io_uring_sqe *sqe = _prefetch_get_sqe(s);
            if (sqe == NULL)
            {
                atomic_fetch_add_explicit(&s->failed, 1, memory_order_relaxed); /* the submit failed and the SQ is still full */
                continue;
            }
            io_uring_prep_fadvise(sqe, slot, req.offset, req.length, POSIX_FADV_WILLNEED);
            sqe->flags |= IOSQE_FIXED_FILE;
            io_uring_sqe_set_data64(sqe, (uint64_t)req.ticket);
            atomic_fetch_add_explicit(&s->submitted, 1, memory_order_relaxed);
        }
        if (s->use_io_uring)
        {
            if (io_uring_sq_ready(&s->ring) > 0)
                _prefetch_submit(s);
            _prefetch_reap(s, false);
        }

        /* sleep until a producer wakes us; re-check after announcing, so a push between the drain and the flag is not lost */
        atomic_store_explicit(&s->sleeping, 1, memory_order_seq_cst);
        struct prefetch_request *cell = &s->queue[s->dequeue_pos & s->queue_mask];
        if (atomic_load_explicit(&cell->sequence, memory_order_seq_cst) == s->dequeue_pos + 1)
        {
            atomic_store_explicit(&s->sleeping, 0, memory_order_relaxed);
            continue;
        }
        if (atomic_load_explicit(&s->stopping, memory_order_acquire))
            break;
        uint64_t value;
        ssize_t r = read(s->eventfd, &value, sizeof(value));
        (void)r;
        atomic_store_explicit(&s->sleeping, 0, memory_order_relaxed);
    }
    while (s->inflight > 0) /* the kernel still uses the SQEs' memory until it completes them */
        _prefetch_reap(s, true);
    return NULL;
}

PRIVATE void
_prefetch_free(struct prefetch_state *s)
{
    free(s->fds);
    free(s->queue);
    free(s->free_slots);
    free((void *)s->generations);
    free(s);
}

EXPORT int32_t
rvn_prefetch_ring_create(int32_t ring_entries, int32_t max_files, int32_t queue_size, void **ring, int32_t *detailed_error_code)
{
    *ring = NULL;
    *detailed_error_code = 0;
    if (ring_entries <= 0 || max_files <= 0 || queue_size <= 0 || (queue_size & (queue_size - 1)) != 0)
        return FAIL_INVALID_CONFIGURATION; /* the queue indexes by mask: a power of two */

    struct prefetch_state *s = NULL;
    size_t state_size = (sizeof(struct prefetch_state) + CACHE_LINE - 1) & ~(size_t)(CACHE_LINE - 1);
    if (posix_memalign((void **)&s, CACHE_LINE, state_size) != 0)
    {
        *detailed_error_code = ENOMEM;
        return FAIL_NOMEM;
    }
    memset(s, 0, state_size);
    s->eventfd = -1;
    s->ring_entries = (uint32_t)ring_entries;
    s->max_files = max_files;
    uint64_t size = (uint64_t)queue_size;
    s->queue_mask = size - 1;

    s->queue = calloc(size, sizeof(struct prefetch_request));
    s->free_slots = calloc((size_t)max_files, sizeof(int32_t));
    s->generations = calloc((size_t)max_files, sizeof(_Atomic uint32_t));
    if (s->queue == NULL || s->free_slots == NULL || s->generations == NULL)
    {
        _prefetch_free(s);
        *detailed_error_code = ENOMEM;
        return FAIL_NOMEM;
    }
    for (uint64_t i = 0; i < size; i++)
        atomic_store_explicit(&s->queue[i].sequence, i, memory_order_relaxed); /* every cell free for the first lap */
    for (int32_t i = 0; i < max_files; i++)
        s->free_slots[i] = i;
    s->free_count = max_files;
    pthread_mutex_init(&s->slots_lock, NULL);

    s->use_io_uring = io_uring_queue_init(s->ring_entries, &s->ring, 0) == 0;
    if (s->use_io_uring && io_uring_register_files_sparse(&s->ring, (unsigned)max_files) < 0)
    {
        io_uring_queue_exit(&s->ring);
        s->use_io_uring = false;
    }
    if (s->use_io_uring == false)
    {
        s->fds = malloc((size_t)max_files * sizeof(int));
        if (s->fds == NULL)
        {
            _prefetch_free(s);
            *detailed_error_code = ENOMEM;
            return FAIL_NOMEM;
        }
        for (int32_t i = 0; i < max_files; i++)
            s->fds[i] = -1;
    }
    s->eventfd = eventfd(0, EFD_CLOEXEC);
    int rc = s->eventfd == -1 ? errno : pthread_create(&s->thread, NULL, rvn_prefetch_ring_submitter, s); /* returns the error, errno is not set */
    if (rc != 0)
    {
        *detailed_error_code = rc;
        if (s->eventfd != -1)
            close(s->eventfd);
        if (s->use_io_uring)
            io_uring_queue_exit(&s->ring);
        _prefetch_free(s);
        return FAIL_CREATE_THREAD;
    }
    pthread_setname_np(s->thread, "Rvn.Prefetch");
    *ring = s;
    return SUCCESS;
}

/* Stops the submitter and frees the ring. Fails while files are still registered with it. */
EXPORT int32_t
rvn_prefetch_ring_destroy(void *ring, int32_t *detailed_error_code)
{
    *detailed_error_code = 0;
    struct prefetch_state *s = ring;
    if (s == NULL)
        return FAIL_INVALID_HANDLE;

    pthread_mutex_lock(&s->slots_lock);
    int32_t registered = s->max_files - s->free_count;
    pthread_mutex_unlock(&s->slots_lock);
    if (registered != 0)
    {
        *detailed_error_code = EBUSY;
        return FAIL_INVALID_HANDLE;
    }

    atomic_store_explicit(&s->stopping, 1, memory_order_seq_cst);
    uint64_t one = 1;
    ssize_t r = write(s->eventfd, &one, sizeof(one));
    (void)r;
    pthread_join(s->thread, NULL);

    if (s->use_io_uring)
        io_uring_queue_exit(&s->ring);
    close(s->eventfd);
    pthread_mutex_destroy(&s->slots_lock);
    _prefetch_free(s);
    return SUCCESS;
}

/* Registers fd with the ring. A full file table is an error. */
PRIVATE int32_t
rvn_prefetch_register(void *ring, int fd, int64_t *ticket, int32_t *detailed_error_code)
{
    *ticket = -1;
    struct prefetch_state *s = ring;
    if (s == NULL)
        return FAIL_INVALID_HANDLE;

    pthread_mutex_lock(&s->slots_lock);
    if (s->free_count == 0)
    {
        pthread_mutex_unlock(&s->slots_lock);
        *detailed_error_code = EMFILE;
        return FAIL_PREFETCH_FILES_EXHAUSTED;
    }
    int32_t slot = s->free_slots[s->free_head];
    if (s->use_io_uring)
    {
        int rc = io_uring_register_files_update(&s->ring, (unsigned)slot, &fd, 1);
        if (rc < 0)
        {
            pthread_mutex_unlock(&s->slots_lock);
            *detailed_error_code = -rc;
            return FAIL_CREATE_IO_RING;
        }
    }
    else
    {
        int dup_fd = dup(fd); /* the pager may close and reopen its descriptor; the duplicate lives until unregister */
        if (dup_fd == -1)
        {
            *detailed_error_code = errno;
            pthread_mutex_unlock(&s->slots_lock);
            return FAIL_DUPLICATE_HANDLE;
        }
        s->fds[slot] = dup_fd;
    }
    s->free_head = (s->free_head + 1) % s->max_files;
    s->free_count--;
    uint32_t generation = atomic_fetch_add_explicit(&s->generations[slot], 1, memory_order_acq_rel) + 1;
    pthread_mutex_unlock(&s->slots_lock);

    *ticket = _make_ticket(slot, generation);
    return SUCCESS;
}

PRIVATE void
rvn_prefetch_unregister(void *ring, int64_t ticket)
{
    struct prefetch_state *s = ring;
    int32_t slot = _ticket_slot(ticket);
    if (s == NULL || ticket == -1 || slot < 0 || slot >= s->max_files)
        return;

    pthread_mutex_lock(&s->slots_lock);
    /* queued requests carry the old generation and are dropped; in flight ones hold their own file reference */
    atomic_fetch_add_explicit(&s->generations[slot], 1, memory_order_acq_rel);
    if (s->use_io_uring)
    {
        int none = -1;
        (void)io_uring_register_files_update(&s->ring, (unsigned)slot, &none, 1);
    }
    else
    {
        close(s->fds[slot]);
        s->fds[slot] = -1;
    }
    s->free_slots[(s->free_head + s->free_count) % s->max_files] = slot;
    s->free_count++;
    pthread_mutex_unlock(&s->slots_lock);
}

/* returns 1 when queued, 0 when dropped (queue full or file not registered) */
PRIVATE int32_t
rvn_prefetch_enqueue(void *ring, int64_t ticket, int64_t offset, int64_t length)
{
    struct prefetch_state *s = ring;
    int32_t slot = _ticket_slot(ticket);
    if (s == NULL || ticket == -1 || slot < 0 || slot >= s->max_files || length <= 0)
        return 0;
    if (length > UINT32_MAX)
        length = UINT32_MAX;

    /* pos must outlive the loop: a failed CAS reloads it, and the publish below needs it */
    uint64_t pos = atomic_load_explicit(&s->enqueue_pos, memory_order_relaxed);
    struct prefetch_request *cell;
    for (;;)
    {
        cell = &s->queue[pos & s->queue_mask];
        int64_t diff = (int64_t)atomic_load_explicit(&cell->sequence, memory_order_acquire) - (int64_t)pos;
        if (diff == 0)
        {
            if (atomic_compare_exchange_weak_explicit(&s->enqueue_pos, &pos, pos + 1, memory_order_relaxed, memory_order_relaxed))
                break;
        }
        else if (diff < 0)
        {
            /* the cell still holds a request from the previous lap: the queue is full, drop this hint */
            atomic_fetch_add_explicit(&s->dropped_full, 1, memory_order_relaxed);
            return 0;
        }
        else
        {
            pos = atomic_load_explicit(&s->enqueue_pos, memory_order_relaxed);
        }
    }
    cell->ticket = ticket;
    cell->offset = (uint64_t)offset;
    cell->length = (uint32_t)length;
    /* seq_cst, not release: with the submitter's seq_cst store of sleeping and its re-check this is a Dekker pair,
       so either we see sleeping == 1 and wake it, or it sees this cell and does not sleep */
    atomic_store_explicit(&cell->sequence, pos + 1, memory_order_seq_cst);

    if (atomic_load_explicit(&s->sleeping, memory_order_seq_cst))
    {
        uint64_t one = 1;
        ssize_t r = write(s->eventfd, &one, sizeof(one));
        (void)r;
    }
    return 1;
}

EXPORT int32_t
rvn_prefetch_ring_stats(void *ring, int64_t *submitted, int64_t *dropped_full, int64_t *dropped_stale, int64_t *failed)
{
    struct prefetch_state *s = ring;
    if (s == NULL)
    {
        *submitted = *dropped_full = *dropped_stale = *failed = 0;
        return FAIL_INVALID_HANDLE;
    }
    *submitted = (int64_t)atomic_load(&s->submitted);
    *dropped_full = (int64_t)atomic_load(&s->dropped_full);
    *dropped_stale = (int64_t)atomic_load(&s->dropped_stale);
    *failed = (int64_t)atomic_load(&s->failed);
    return SUCCESS;
}
