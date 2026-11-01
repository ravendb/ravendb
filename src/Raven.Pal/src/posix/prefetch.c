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
 * An io_uring for reads into the page cache, separate from the journal ring so it never queues behind or in front of
 * durable writes. Any thread queues requests; one submitter thread runs them. Two kinds of requests:
 *  - WILLNEED range: a hint, dropped when the queue is full.
 *  - read group: an 8-byte read across the 4 KB boundary inside each 8 KB page, so each page comes in one I/O. The offsets
 *    are copied into the queue, up to PREFETCH_CELL_PAGES per cell; on a full queue the producer waits for free cells. The
 *    caller owns the group index: the index reaches on_reads_done once all of the group's reads complete, and it must not
 *    be queued again before that.
 *
 * A registered file is in the ring's fixed file table, and the ring holds its own reference to it, so a request never
 * reaches another file that reused the descriptor number. A ticket is (generation << 32) | slot; registering and
 * unregistering bump the slot's generation, so the submitter drops queued requests of an earlier registration.
 *
 * Without io_uring, the submitter runs WILLNEED with posix_fadvise on a duplicate descriptor, and read groups complete at
 * once without reads.
 */

#define CACHE_LINE 64
#define PREFETCH_READ_AT (4096 - 4) /* 4 bytes on each side of the 4 KB boundary inside a page */
#define PREFETCH_READ_LENGTH 8
#define PREFETCH_PAGE_SIZE 8192
#define PREFETCH_GROUP_TAG (1ULL << 63) /* user_data of a group read: the tag | the group index; WILLNEED uses 0 */
#define PREFETCH_DONE_BATCH 64
#define PREFETCH_CELL_PAGES 12 /* a cell is two cache lines */

struct prefetch_request
{
    int64_t ticket;
    uint32_t group;
    uint16_t count;           /* read group: offsets in this cell; 0 for WILLNEED */
    uint16_t first;           /* read group: the group's first cell, which carries the total */
    uint32_t total;           /* read group: offsets in all of its cells */
    uint32_t length;          /* WILLNEED: the range starts at offsets[0] */
    int64_t offsets[PREFETCH_CELL_PAGES];
};

struct prefetch_cell
{
    /* Vyukov bounded queue: a cell at position pos is free while sequence == pos, and holds a request
       while sequence == pos + 1; emptying it sets sequence = pos + queue size (free for the next lap) */
    _Atomic uint64_t sequence;
    struct prefetch_request request;
};

_Static_assert(sizeof(struct prefetch_cell) == 2 * 64, "a cell is two cache lines");

struct prefetch_state
{
    /* written by every producer */
    alignas(CACHE_LINE) _Atomic uint64_t enqueue_pos;
    _Atomic uint64_t dropped_full;
    _Atomic uint32_t space_waiters;
    /* written by the submitter, read by every producer */
    alignas(CACHE_LINE) _Atomic int32_t sleeping;
    _Atomic int32_t stopping;
    _Atomic uint32_t space_seq; /* a futex word: bumped when cells were freed and a producer waits */
    /* owned by the submitter thread (the only consumer); the counters are read by rvn_prefetch_ring_stats */
    alignas(CACHE_LINE) uint64_t dequeue_pos;
    uint32_t inflight;
    _Atomic uint64_t submitted;
    _Atomic uint64_t dropped_stale;
    _Atomic uint64_t failed;
    _Atomic uint64_t read_groups;
    _Atomic uint64_t reads;

    uint32_t *group_pending; /* by group index */
    uint32_t done[PREFETCH_DONE_BATCH]; /* completed group indexes not yet passed to on_reads_done */
    int32_t done_count;
    alignas(CACHE_LINE) char sink[CACHE_LINE]; /* registered buffer 0: the target of every read, never read */

    /* set once at init */
    alignas(CACHE_LINE) struct io_uring ring;
    bool use_io_uring;
    int *fds; /* without io_uring: a duplicate of each registered file's descriptor, -1 when free */
    int eventfd;
    pthread_t thread;
    uint32_t ring_entries;
    uint64_t queue_mask;
    int32_t max_files;
    uint32_t max_groups;
    rvn_reads_done on_reads_done;
    void *on_reads_done_context;

    /* registration, guarded by slots_lock */
    pthread_mutex_t slots_lock;
    int32_t free_head;
    int32_t free_count;
    int32_t *free_slots; /* FIFO ring of free slot numbers, so a released slot is reused as late as possible */
    _Atomic uint32_t *generations;

    struct prefetch_cell *queue;
};

static inline int32_t _ticket_slot(int64_t ticket) { return (int32_t)(uint32_t)ticket; }
static inline uint32_t _ticket_generation(int64_t ticket) { return (uint32_t)((uint64_t)ticket >> 32); }
static inline int64_t _make_ticket(int32_t slot, uint32_t generation) { return (int64_t)(((uint64_t)generation << 32) | (uint32_t)slot); }

PRIVATE bool
_prefetch_dequeue(struct prefetch_state *s, struct prefetch_request *out)
{
    struct prefetch_cell *cell = &s->queue[s->dequeue_pos & s->queue_mask];
    if (atomic_load_explicit(&cell->sequence, memory_order_acquire) != s->dequeue_pos + 1)
        return false; /* empty, or the producer has not published the cell yet */
    *out = cell->request;
    atomic_store_explicit(&cell->sequence, s->dequeue_pos + s->queue_mask + 1, memory_order_release);
    s->dequeue_pos++;
    return true;
}

/* Submitter side of a full queue: wakes the producers waiting for free cells, if any */
PRIVATE void
_signal_space(struct prefetch_state *s)
{
    atomic_thread_fence(memory_order_seq_cst); /* the cells we freed are visible before we look for waiters */
    if (atomic_load_explicit(&s->space_waiters, memory_order_relaxed) == 0)
        return;
    atomic_fetch_add_explicit(&s->space_seq, 1, memory_order_seq_cst);
    _futex_wake_all((uint32_t *)&s->space_seq);
}

/* passes the completed groups to on_reads_done, and wakes the producers waiting for free cells */
PRIVATE void
_flush_done(struct prefetch_state *s)
{
    if (s->done_count != 0)
    {
        s->on_reads_done(s->on_reads_done_context, s->done, s->done_count);
        s->done_count = 0;
    }
    _signal_space(s);
}

PRIVATE void
_group_read_done(struct prefetch_state *s, uint32_t group)
{
    if (--s->group_pending[group] != 0)
        return;
    if (s->done_count == PREFETCH_DONE_BATCH)
        _flush_done(s);
    s->done[s->done_count++] = group;
}

PRIVATE void
_prefetch_complete(struct prefetch_state *s, struct io_uring_cqe *cqe)
{
    if (cqe->res < 0)
        atomic_fetch_add_explicit(&s->failed, 1, memory_order_relaxed); /* a failed read still completes its group */
    uint64_t data = io_uring_cqe_get_data64(cqe);
    io_uring_cqe_seen(&s->ring, cqe);
    s->inflight--;
    if (data & PREFETCH_GROUP_TAG)
        _group_read_done(s, (uint32_t)(data & ~PREFETCH_GROUP_TAG));
}

/* Producer side of a full queue: sleeps until the submitter frees cells */
PRIVATE void
_wait_for_space(struct prefetch_state *s, struct prefetch_cell *cell, uint64_t pos)
{
    uint32_t seen = atomic_load_explicit(&s->space_seq, memory_order_seq_cst);
    atomic_fetch_add_explicit(&s->space_waiters, 1, memory_order_seq_cst);
    /* with _signal_space a Dekker pair: either we see the freed cell, or the submitter sees us waiting */
    if ((int64_t)(atomic_load_explicit(&cell->sequence, memory_order_seq_cst) - pos) < 0)
        _futex_wait((uint32_t *)&s->space_seq, seen);
    atomic_fetch_sub_explicit(&s->space_waiters, 1, memory_order_relaxed);
}

PRIVATE void
_prefetch_reap(struct prefetch_state *s, bool wait_for_one)
{
    struct io_uring_cqe *cqe;
    if (wait_for_one)
        _flush_done(s); /* completed groups and waiting producers must not wait while we block */
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

/* In flight + prepared never exceed the SQ size, so the CQ (twice that) cannot overflow: when full, submit and wait for
   one completion. */
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

/* read == false (stale ticket, no io_uring, stopping): the cell's pages count as done without reads */
PRIVATE void
_prefetch_read_cell(struct prefetch_state *s, int32_t slot, const struct prefetch_request *req, bool read)
{
    if (req->first)
    {
        s->group_pending[req->group] = req->total;
        atomic_fetch_add_explicit(&s->read_groups, 1, memory_order_relaxed);
    }
    for (uint32_t i = 0; i < req->count; i++)
    {
        struct io_uring_sqe *sqe = read ? _prefetch_get_sqe(s) : NULL; /* may reap: this group's earlier reads can complete */
        if (sqe == NULL)
        {
            if (read)
                atomic_fetch_add_explicit(&s->failed, 1, memory_order_relaxed); /* the submit failed and the SQ is still full */
            _group_read_done(s, req->group);
            continue;
        }
        io_uring_prep_read_fixed(sqe, slot, s->sink, PREFETCH_READ_LENGTH, (uint64_t)req->offsets[i] + PREFETCH_READ_AT, 0);
        sqe->flags |= IOSQE_FIXED_FILE;
        io_uring_sqe_set_data64(sqe, PREFETCH_GROUP_TAG | req->group);
        atomic_fetch_add_explicit(&s->reads, 1, memory_order_relaxed);
    }
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
            bool stale = atomic_load_explicit(&s->generations[slot], memory_order_acquire) != _ticket_generation(req.ticket);
            if (stale)
                atomic_fetch_add_explicit(&s->dropped_stale, 1, memory_order_relaxed);
            if (req.count != 0)
            {
                _prefetch_read_cell(s, slot, &req, stale == false && s->use_io_uring);
                continue;
            }
            if (stale)
                continue;
            if (s->use_io_uring == false)
            {
                int fd = s->fds[slot];
                if (fd == -1 || posix_fadvise(fd, (off_t)req.offsets[0], (off_t)req.length, POSIX_FADV_WILLNEED) != 0)
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
            io_uring_prep_fadvise(sqe, slot, (uint64_t)req.offsets[0], req.length, POSIX_FADV_WILLNEED);
            sqe->flags |= IOSQE_FIXED_FILE;
            io_uring_sqe_set_data64(sqe, 0);
            atomic_fetch_add_explicit(&s->submitted, 1, memory_order_relaxed);
        }
        if (s->use_io_uring)
        {
            if (io_uring_sq_ready(&s->ring) > 0)
                _prefetch_submit(s);
            _prefetch_reap(s, false);
        }
        _flush_done(s);

        /* sleep until a producer or a completion signals the eventfd; re-check after announcing, so a push between the drain
           and the flag is not lost */
        atomic_store_explicit(&s->sleeping, 1, memory_order_seq_cst);
        struct prefetch_cell *cell = &s->queue[s->dequeue_pos & s->queue_mask];
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
    while (_prefetch_dequeue(s, &req)) /* stopping: a group still in the queue completes without its reads */
    {
        if (req.count != 0)
            _prefetch_read_cell(s, 0, &req, false);
    }
    while (s->inflight > 0) /* in-flight reads write into the sink, which is freed with the state */
        _prefetch_reap(s, true);
    _flush_done(s);
    return NULL;
}

PRIVATE void
_prefetch_free(struct prefetch_state *s)
{
    free(s->fds);
    free(s->group_pending);
    free(s->queue);
    free(s->free_slots);
    free((void *)s->generations);
    free(s);
}

EXPORT int32_t
rvn_prefetch_ring_create(int32_t ring_entries, int32_t max_files, int32_t queue_size, int32_t max_groups,
    rvn_reads_done on_reads_done, void *on_reads_done_context, void **ring, int32_t *detailed_error_code)
{
    *ring = NULL;
    *detailed_error_code = 0;
    if (ring_entries <= 0 || max_files <= 0 || queue_size <= 0 || (queue_size & (queue_size - 1)) != 0 || max_groups <= 0 ||
        on_reads_done == NULL)
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
    s->on_reads_done = on_reads_done;
    s->on_reads_done_context = on_reads_done_context;
    s->max_groups = (uint32_t)max_groups;
    uint64_t size = (uint64_t)queue_size;
    s->queue_mask = size - 1;

    if (posix_memalign((void **)&s->queue, CACHE_LINE, size * sizeof(struct prefetch_cell)) != 0)
        s->queue = NULL;
    s->free_slots = calloc((size_t)max_files, sizeof(int32_t));
    s->generations = calloc((size_t)max_files, sizeof(_Atomic uint32_t));
    s->group_pending = calloc((size_t)max_groups, sizeof(uint32_t));
    if (s->queue == NULL || s->free_slots == NULL || s->generations == NULL || s->group_pending == NULL)
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

    s->eventfd = eventfd(0, EFD_CLOEXEC);
    if (s->eventfd == -1)
    {
        *detailed_error_code = errno;
        _prefetch_free(s);
        return FAIL_CREATE_THREAD;
    }
    s->use_io_uring = io_uring_queue_init(s->ring_entries, &s->ring, 0) == 0;
    struct iovec sink = { .iov_base = s->sink, .iov_len = sizeof(s->sink) };
    if (s->use_io_uring &&
        (io_uring_register_files_sparse(&s->ring, (unsigned)max_files) < 0 || io_uring_register_eventfd(&s->ring, s->eventfd) < 0 ||
         io_uring_register_buffers(&s->ring, &sink, 1) < 0))
    {
        io_uring_queue_exit(&s->ring);
        s->use_io_uring = false;
    }
    if (s->use_io_uring == false)
    {
        s->fds = malloc((size_t)max_files * sizeof(int));
        if (s->fds == NULL)
        {
            close(s->eventfd);
            _prefetch_free(s);
            *detailed_error_code = ENOMEM;
            return FAIL_NOMEM;
        }
        for (int32_t i = 0; i < max_files; i++)
            s->fds[i] = -1;
    }
    int rc = pthread_create(&s->thread, NULL, rvn_prefetch_ring_submitter, s); /* returns the error, errno is not set */
    if (rc != 0)
    {
        *detailed_error_code = rc;
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

/* Returns 1 when queued. On a full queue, returns 0 (a dropped hint), or waits for the submitter to free a cell. */
PRIVATE int32_t
_prefetch_push(struct prefetch_state *s, const struct prefetch_request *req, bool wait)
{
    /* pos must outlive the loop: a failed CAS reloads it, and the publish below needs it */
    uint64_t pos = atomic_load_explicit(&s->enqueue_pos, memory_order_relaxed);
    struct prefetch_cell *cell;
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
            /* the cell still holds a request from the previous lap: the queue is full */
            if (wait == false)
            {
                atomic_fetch_add_explicit(&s->dropped_full, 1, memory_order_relaxed);
                return 0;
            }
            _wait_for_space(s, cell, pos);
            pos = atomic_load_explicit(&s->enqueue_pos, memory_order_relaxed);
        }
        else
        {
            pos = atomic_load_explicit(&s->enqueue_pos, memory_order_relaxed);
        }
    }
    cell->request = *req;
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

PRIVATE bool
_prefetch_valid_ticket(struct prefetch_state *s, int64_t ticket)
{
    int32_t slot = _ticket_slot(ticket);
    return s != NULL && ticket != -1 && slot >= 0 && slot < s->max_files;
}

/* returns 1 when queued, 0 when dropped (queue full or file not registered) */
PRIVATE int32_t
rvn_prefetch_enqueue(void *ring, int64_t ticket, int64_t offset, int64_t length)
{
    struct prefetch_state *s = ring;
    if (_prefetch_valid_ticket(s, ticket) == false || length <= 0)
        return 0;
    struct prefetch_request req = {0};
    req.ticket = ticket;
    req.offsets[0] = offset;
    req.length = length > UINT32_MAX ? UINT32_MAX : (uint32_t)length;
    return _prefetch_push(s, &req, false);
}

/* Async read of the 8 KB pages at offsets; calls on_reads_done with group when done. Returns 1 when queued */
EXPORT int32_t
rvn_prefetch_ring_read_pages(void *ring, int64_t ticket, const int64_t *offsets, int32_t count, uint32_t group)
{
    struct prefetch_state *s = ring;
    if (_prefetch_valid_ticket(s, ticket) == false || offsets == NULL || count <= 0 || group >= s->max_groups)
        return 0;
    struct prefetch_request req = {0};
    req.ticket = ticket;
    req.group = group;
    req.total = (uint32_t)count;
    for (int32_t done = 0; done < count; done += PREFETCH_CELL_PAGES)
    {
        req.first = done == 0;
        req.count = (uint16_t)rvn_min(PREFETCH_CELL_PAGES, count - done);
        memcpy(req.offsets, offsets + done, req.count * sizeof(int64_t));
        _prefetch_push(s, &req, true);
    }
    return 1;
}

/* Starts reading the pages not in the page cache, without waiting; returns them at the front of offsets */
PRIVATE int32_t
rvn_prefetch_start_reads(int fd, int64_t *offsets, int32_t count)
{
    char sink[PREFETCH_READ_LENGTH];
    int32_t missing = 0;
    for (int32_t i = 0; i < count; i++)
    {
        int64_t offset = offsets[i];
        ssize_t rc = _preadv2_nowait(fd, sink, sizeof(sink), offset + PREFETCH_READ_AT);
        if (rc == PREFETCH_READ_LENGTH)
            continue;
        if (rc == -1 && (errno == EOPNOTSUPP || errno == ENOSYS)) /* the file system or the kernel has no RWF_NOWAIT */
            (void)posix_fadvise(fd, (off_t)offset, PREFETCH_PAGE_SIZE, POSIX_FADV_WILLNEED);
        offsets[missing++] = offset;
    }
    return missing;
}

EXPORT int32_t
rvn_prefetch_ring_stats(void *ring, struct rvn_prefetch_ring_stats *stats)
{
    struct prefetch_state *s = ring;
    memset(stats, 0, sizeof(*stats));
    if (s == NULL)
        return FAIL_INVALID_HANDLE;
    stats->submitted = (int64_t)atomic_load(&s->submitted);
    stats->dropped_full = (int64_t)atomic_load(&s->dropped_full);
    stats->dropped_stale = (int64_t)atomic_load(&s->dropped_stale);
    stats->failed = (int64_t)atomic_load(&s->failed);
    stats->read_groups = (int64_t)atomic_load(&s->read_groups);
    stats->reads = (int64_t)atomic_load(&s->reads);
    stats->uses_io_uring = s->use_io_uring ? 1 : 0;
    return SUCCESS;
}
