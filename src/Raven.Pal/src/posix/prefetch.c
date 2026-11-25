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
#include <poll.h>
#include <time.h>

#include "rvn.h"
#include "rvn_internal.h"
#include "status_codes.h"
#include "internal_posix.h"
#include "slots.h"

/*
 * An io_uring for reads into the page cache, separate from the journal ring so it never queues behind or in front of
 * durable writes. Any thread queues requests; one submitter thread runs them. Two kinds of requests:
 *  - WILLNEED range: a hint, dropped when the queue is full.
 *  - read group: an 8-byte read across the 4 KB boundary inside each 8 KB page, so each page comes in one I/O.
 *
 * A producer never waits: a full queue means the device is already busy, and the reader faults the pages itself.
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
#define PREFETCH_GIVE_UP_MS 15000 /* transient submit errors for this long fail the ring */

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
    /* written by the submitter, read by every producer */
    alignas(CACHE_LINE) _Atomic int32_t sleeping;
    _Atomic int32_t stopping;
    /* owned by the submitter thread (the only consumer); the counters are read by rvn_prefetch_ring_stats */
    alignas(CACHE_LINE) uint64_t dequeue_pos;
    uint32_t inflight;
    _Atomic uint64_t submitted;
    _Atomic uint64_t dropped_stale;
    _Atomic uint64_t failed;
    _Atomic uint64_t read_groups;
    _Atomic uint64_t reads;

    uint32_t backoff_ms;     /* delay before retrying a failed submit; 0 after a success */
    int64_t failing_since_ms; /* start of the current run of transient submit errors, -1 for none */
    _Atomic int32_t give_up_ms; /* PREFETCH_GIVE_UP_MS; tests lower it */
    _Atomic int32_t failing_submits;      /* tests: submits that fail before reaching the kernel */
    _Atomic int32_t failing_submit_error; /* tests: their -errno */
    bool ring_failed;        /* a submit failed for good: later requests complete without reads */
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
    struct rvn_slots slots;
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

PRIVATE void
_flush_done(struct prefetch_state *s)
{
    if (s->done_count != 0)
    {
        s->on_reads_done(s->on_reads_done_context, s->done, s->done_count);
        s->done_count = 0;
    }
}

PRIVATE void
_group_read_done(struct prefetch_state *s, uint32_t group)
{
    if (s->group_pending[group] == 0)
        return; /* completed when the ring failed: its remaining cells are ignored */
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

PRIVATE void
_prefetch_reap(struct prefetch_state *s, bool wait_for_one)
{
    struct io_uring_cqe *cqe;
    if (wait_for_one)
        _flush_done(s); /* completed groups must not wait while we block */
    if (wait_for_one && s->inflight > 0 && io_uring_wait_cqe(&s->ring, &cqe) == 0)
        _prefetch_complete(s, cqe);
    while (s->inflight > 0 && io_uring_peek_cqe(&s->ring, &cqe) == 0)
        _prefetch_complete(s, cqe);
}

/* Completes every pending group without its reads, which will never run */
PRIVATE void
_prefetch_complete_pending_groups(struct prefetch_state *s)
{
    for (uint32_t group = 0; group < s->max_groups; group++)
    {
        if (s->group_pending[group] == 0)
            continue;
        s->group_pending[group] = 1;
        _group_read_done(s, group);
    }
}

static int64_t
_monotonic_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

PRIVATE int
_prefetch_submit(struct prefetch_state *s)
{
    if (io_uring_sq_ready(&s->ring) == 0)
        return 0;
    int rc;
    if (atomic_load_explicit(&s->failing_submits, memory_order_relaxed) > 0)
    {
        atomic_fetch_sub_explicit(&s->failing_submits, 1, memory_order_relaxed);
        rc = atomic_load_explicit(&s->failing_submit_error, memory_order_relaxed);
    }
    else
        rc = io_uring_submit(&s->ring);
    if (rc > 0)
    {
        s->inflight += (uint32_t)rc;
        s->backoff_ms = 0;
        s->failing_since_ms = -1;
        return 0;
    }
    if ((rc == 0 || rc == -EAGAIN || rc == -EBUSY || rc == -ENOMEM || rc == -EINTR) &&
        atomic_load_explicit(&s->stopping, memory_order_acquire) == 0)
    {
        int64_t now = _monotonic_ms();
        if (s->failing_since_ms == -1)
            s->failing_since_ms = now;
        if (now - s->failing_since_ms < atomic_load_explicit(&s->give_up_ms, memory_order_relaxed))
            return 0;
    }
    atomic_fetch_add_explicit(&s->failed, io_uring_sq_ready(&s->ring), memory_order_relaxed);
    s->ring_failed = true;
    struct io_uring_cqe *cqe;
    while (s->inflight > 0 && io_uring_wait_cqe(&s->ring, &cqe) == 0) /* wait for all the pending work */
        _prefetch_complete(s, cqe);
    _prefetch_complete_pending_groups(s); /* now report failure for everything */
    return -1;
}

/* With nothing in flight no completion wakes the submitter, so a failed submit is retried after a growing delay */
PRIVATE int
_prefetch_next_backoff(struct prefetch_state *s)
{
    s->backoff_ms = s->backoff_ms == 0 ? 1 : rvn_min(s->backoff_ms * 2, 100);
    return (int)s->backoff_ms;
}

/* Sleeps until a producer or a completion signals the eventfd, or for timeout_ms (-1: no timeout) */
PRIVATE void
_prefetch_sleep(struct prefetch_state *s, int timeout_ms)
{
    _flush_done(s); /* completed groups must not wait while we sleep */
    struct pollfd pfd = {.fd = s->eventfd, .events = POLLIN};
    if (poll(&pfd, 1, timeout_ms) > 0)
    {
        uint64_t value;
        ssize_t r = read(s->eventfd, &value, sizeof(value));
        (void)r;
    }
}

PRIVATE bool
_prefetch_flush(struct prefetch_state *s, bool wait)
{
    if (s->ring_failed || _prefetch_submit(s) != 0)
        return false;
    if (s->inflight > 0)
        _prefetch_reap(s, wait);
    else if (wait && io_uring_sq_ready(&s->ring) > 0)
        _prefetch_sleep(s, _prefetch_next_backoff(s));
    return true;
}

/* In flight + prepared never exceed the SQ size, so the CQ (twice that) cannot overflow. Returns NULL once the ring failed */
PRIVATE struct io_uring_sqe *
_prefetch_get_sqe(struct prefetch_state *s)
{
    while (s->inflight + io_uring_sq_ready(&s->ring) >= s->ring_entries)
    {
        if (_prefetch_flush(s, true) == false)
            return NULL;
    }
    return s->ring_failed ? NULL : io_uring_get_sqe(&s->ring);
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
                atomic_fetch_add_explicit(&s->failed, 1, memory_order_relaxed); /* the ring failed */
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
                /* no lock: a descriptor closed and reused after the generation check only gets a harmless read-ahead */
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
                atomic_fetch_add_explicit(&s->failed, 1, memory_order_relaxed); /* the ring failed */
                continue;
            }
            io_uring_prep_fadvise(sqe, slot, (uint64_t)req.offsets[0], req.length, POSIX_FADV_WILLNEED);
            sqe->flags |= IOSQE_FIXED_FILE;
            io_uring_sqe_set_data64(sqe, 0);
            atomic_fetch_add_explicit(&s->submitted, 1, memory_order_relaxed);
        }
        if (s->use_io_uring)
            _prefetch_flush(s, false);
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
        /* requests left in the SQ by a failed submit: no completion will signal the eventfd */
        bool retry = s->use_io_uring && s->ring_failed == false && s->inflight == 0 && io_uring_sq_ready(&s->ring) > 0;
        _prefetch_sleep(s, retry ? _prefetch_next_backoff(s) : -1);
        atomic_store_explicit(&s->sleeping, 0, memory_order_relaxed);
    }
    while (_prefetch_dequeue(s, &req)) /* stopping: a group still in the queue completes without its reads */
    {
        if (req.count != 0)
            _prefetch_read_cell(s, 0, &req, false);
    }
    /* every file is unregistered by now, so nothing left in the SQ is submitted; in-flight reads write into the sink,
       which is freed with the state */
    while (s->inflight > 0)
        _prefetch_reap(s, true);
    _prefetch_complete_pending_groups(s); /* reads that were never submitted */
    _flush_done(s);
    return NULL;
}

PRIVATE void
_prefetch_free(struct prefetch_state *s)
{
    free(s->fds);
    free(s->group_pending);
    free(s->queue);
    rvn_slots_free(&s->slots);
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
    s->failing_since_ms = -1;
    atomic_store_explicit(&s->give_up_ms, PREFETCH_GIVE_UP_MS, memory_order_relaxed);
    s->ring_entries = (uint32_t)ring_entries;
    s->max_files = max_files;
    s->on_reads_done = on_reads_done;
    s->on_reads_done_context = on_reads_done_context;
    s->max_groups = (uint32_t)max_groups;
    uint64_t size = (uint64_t)queue_size;
    s->queue_mask = size - 1;

    if (posix_memalign((void **)&s->queue, CACHE_LINE, size * sizeof(struct prefetch_cell)) != 0)
        s->queue = NULL;
    s->generations = calloc((size_t)max_files, sizeof(_Atomic uint32_t));
    s->group_pending = calloc((size_t)max_groups, sizeof(uint32_t));
    if (s->queue == NULL || s->generations == NULL || s->group_pending == NULL ||
        rvn_slots_init(&s->slots, max_files, detailed_error_code) != SUCCESS)
    {
        _prefetch_free(s);
        *detailed_error_code = ENOMEM;
        return FAIL_NOMEM;
    }
    for (uint64_t i = 0; i < size; i++)
        atomic_store_explicit(&s->queue[i].sequence, i, memory_order_relaxed); /* every cell free for the first lap */
    pthread_mutex_init(&s->slots_lock, NULL);

    s->eventfd = eventfd(0, EFD_CLOEXEC);
    if (s->eventfd == -1)
    {
        *detailed_error_code = errno;
        _prefetch_free(s);
        return FAIL_CREATE_EVENTFD;
    }
    s->use_io_uring = io_uring_queue_init(s->ring_entries, &s->ring, 0) == 0;
    struct iovec sink = { .iov_base = s->sink, .iov_len = sizeof(s->sink) };
    unsigned registered = (unsigned)max_files;
    if (s->use_io_uring &&
        (_io_uring_register_files_empty(&s->ring, &registered) < 0 || io_uring_register_eventfd(&s->ring, s->eventfd) < 0 ||
         io_uring_register_buffers(&s->ring, &sink, 1) < 0))
    {
        io_uring_queue_exit(&s->ring);
        s->use_io_uring = false;
    }
    if (s->use_io_uring)
        s->slots.capacity = (int32_t)registered;
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
    int32_t registered = s->slots.count;
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
    int32_t slot = rvn_slots_take(&s->slots);
    if (slot == -1)
    {
        pthread_mutex_unlock(&s->slots_lock);
        *detailed_error_code = EMFILE;
        return FAIL_PREFETCH_FILES_EXHAUSTED;
    }
    if (s->use_io_uring)
    {
        int rc = io_uring_register_files_update(&s->ring, (unsigned)slot, &fd, 1);
        if (rc < 0)
        {
            rvn_slots_release(&s->slots, slot);
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
            rvn_slots_release(&s->slots, slot);
            pthread_mutex_unlock(&s->slots_lock);
            return FAIL_DUPLICATE_HANDLE;
        }
        s->fds[slot] = dup_fd;
    }
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
    rvn_slots_release(&s->slots, slot);
    pthread_mutex_unlock(&s->slots_lock);
}

/* Wakes the submitter if it sleeps; a producer calls it once, after its pushes. With the submitter's seq_cst store of
   sleeping and its re-check, a Dekker pair: either we see sleeping == 1, or it sees our cells */
PRIVATE void
_prefetch_wake(struct prefetch_state *s)
{
    if (atomic_load_explicit(&s->sleeping, memory_order_seq_cst))
    {
        uint64_t one = 1;
        ssize_t r = write(s->eventfd, &one, sizeof(one));
        (void)r;
    }
}

/* Claims count consecutive cells, all or none; -1 when the queue has no room. Checking the last cell is enough: the one
   consumer frees cells in order */
PRIVATE int64_t
_prefetch_claim(struct prefetch_state *s, uint32_t count)
{
    uint64_t pos = atomic_load_explicit(&s->enqueue_pos, memory_order_relaxed);
    for (;;)
    {
        uint64_t last = pos + count - 1;
        int64_t diff = (int64_t)atomic_load_explicit(&s->queue[last & s->queue_mask].sequence, memory_order_acquire) - (int64_t)last;
        if (count > s->queue_mask + 1 || diff < 0) /* larger than the queue, or the last cell is still in use */
        {
            atomic_fetch_add_explicit(&s->dropped_full, 1, memory_order_relaxed);
            return -1;
        }
        if (diff == 0)
        {
            /* a failed CAS reloads pos */
            if (atomic_compare_exchange_weak_explicit(&s->enqueue_pos, &pos, pos + count, memory_order_relaxed, memory_order_relaxed))
                return (int64_t)pos;
        }
        else
        {
            pos = atomic_load_explicit(&s->enqueue_pos, memory_order_relaxed); /* another producer claimed it */
        }
    }
}

/* The caller wakes the submitter after its last cell */
PRIVATE void
_prefetch_publish(struct prefetch_state *s, uint64_t pos, const struct prefetch_request *req)
{
    struct prefetch_cell *cell = &s->queue[pos & s->queue_mask];
    cell->request = *req;
    /* seq_cst, not release: the store half of the Dekker pair in _prefetch_wake */
    atomic_store_explicit(&cell->sequence, pos + 1, memory_order_seq_cst);
}

PRIVATE bool
_prefetch_valid_ticket(struct prefetch_state *s, int64_t ticket)
{
    int32_t slot = _ticket_slot(ticket);
    return s != NULL && ticket != -1 && slot >= 0 && slot < s->max_files;
}

/* WILLNEED for the range, in requests of request_size; returns 1 when all were queued, 0 when any was dropped (queue full or
   file not registered) */
PRIVATE int32_t
rvn_prefetch_enqueue(void *ring, int64_t ticket, int64_t offset, int64_t length, int64_t request_size)
{
    struct prefetch_state *s = ring;
    if (_prefetch_valid_ticket(s, ticket) == false || length <= 0 || request_size <= 0)
        return 0;
    struct prefetch_request req = {0};
    req.ticket = ticket;
    int32_t all = 1;
    for (int64_t end = offset + length; offset < end; offset += request_size)
    {
        int64_t pos = _prefetch_claim(s, 1);
        if (pos == -1)
        {
            all = 0;
            break; /* a full queue: the rest of the range would be dropped too */
        }
        req.offsets[0] = offset;
        req.length = (uint32_t)rvn_min(rvn_min(request_size, end - offset), (int64_t)UINT32_MAX);
        _prefetch_publish(s, (uint64_t)pos, &req);
    }
    _prefetch_wake(s);
    return all;
}

/* Async read of the 8 KB pages at offsets; calls on_reads_done with group when done. Never waits: returns 1 when queued,
   0 when the queue has no room for the whole group (dropped, on_reads_done is not called) */
EXPORT int32_t
rvn_prefetch_ring_read_pages(void *ring, int64_t ticket, const int64_t *offsets, int32_t count, uint32_t group)
{
    struct prefetch_state *s = ring;
    if (_prefetch_valid_ticket(s, ticket) == false || offsets == NULL || count <= 0 || group >= s->max_groups)
        return 0;
    uint32_t cells = ((uint32_t)count + PREFETCH_CELL_PAGES - 1) / PREFETCH_CELL_PAGES;
    int64_t pos = _prefetch_claim(s, cells); /* all or none: a group missing a cell would never complete */
    if (pos == -1)
        return 0;
    struct prefetch_request req = {0};
    req.ticket = ticket;
    req.group = group;
    req.total = (uint32_t)count;
    for (int32_t done = 0; done < count; done += PREFETCH_CELL_PAGES, pos++)
    {
        req.first = done == 0;
        req.count = (uint16_t)rvn_min(PREFETCH_CELL_PAGES, count - done);
        memcpy(req.offsets, offsets + done, req.count * sizeof(int64_t));
        _prefetch_publish(s, (uint64_t)pos, &req);
    }
    _prefetch_wake(s);
    return 1;
}

/* Moves the pages not in the page cache to the front of offsets. Kernel 6.12 and later also start their reads; earlier kernels do not */
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

/* For tests: the next count submits fail with -error; transient errors for give_up_ms fail the ring */
EXPORT int32_t
rvn_prefetch_ring_fail_submits(void *ring, int32_t count, int32_t error, int32_t give_up_ms)
{
    struct prefetch_state *s = ring;
    if (s == NULL)
        return FAIL_INVALID_HANDLE;
    atomic_store_explicit(&s->give_up_ms, give_up_ms, memory_order_relaxed);
    atomic_store_explicit(&s->failing_submit_error, -error, memory_order_relaxed);
    atomic_store_explicit(&s->failing_submits, count, memory_order_relaxed);
    return SUCCESS;
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
