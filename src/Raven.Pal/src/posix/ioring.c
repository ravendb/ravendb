#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <sys/utsname.h>
#include <unistd.h>
#include <sys/statfs.h>
#include <linux/magic.h>
#include <linux/ioprio.h>
#include <sys/syscall.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <libgen.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/eventfd.h>

#include "rvn.h"
#include "rvn_internal.h"
#include "status_codes.h"
#include "internal_posix.h"

typedef enum workitem_type
{
    workitem_none,
    workitem_write,
    workitem_fsync,
    workitem_open_folders,
    workitem_fsync_folders,
} workitem_type;

struct submittion
{
    int filefd;
    int notifyfd;
    int32_t count;
    int32_t result;
    bool error;
};

struct workitem
{
    struct submittion *submittion;
    struct workitem *next;
    workitem_type type;

    union
    {
        struct
        {
            off_t offset;
            int iovecs_count;
            struct iovec *iovecs;
        } write;
        struct
        {
            char *path;
            int folderid;
        } folder_sync;
    } op;
};

_Static_assert(_Alignof(struct workitem) == _Alignof(struct iovec), "workitem must have same alignment as iovec");
_Static_assert(sizeof(struct workitem) >= sizeof(struct iovec), "workitem must bet equal to or greater than iovec");

struct worker
{
    struct io_uring ring;
    int eventfd;
    _Atomic bool errored;
    int result;
    pthread_t thread;
    struct workitem *_Atomic head;
};

struct worker g_worker = {.eventfd = -1, .ring = {.ring_fd = -1}};

uint64_t done = 1;

void notify_work_completed(struct io_uring *ring, struct workitem *work)
{
    if (--work->submittion->count > 0)
    {
        return; // not yet completed
    }
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) // if we can't notify via the io_uring because it is full, let's use direct syscall
    {
        // explicitly ignoring the return value, we can't do anything about it
        eventfd_write(work->submittion->notifyfd, done);
        return;
    }
    io_uring_prep_write(sqe, work->submittion->notifyfd, &done, sizeof(done), 0);
    io_uring_sqe_set_data(sqe, 0);
}

void queue_work(struct workitem *head, struct workitem *last)
{
    struct workitem *cur_head = atomic_load(&g_worker.head);
    do
    {
        last->next = cur_head;
    } while (!atomic_compare_exchange_weak(&g_worker.head, &cur_head, head));
}

void mark_all_cqes_as_errors(struct io_uring *ring, int rc)
{
    struct io_uring_cqe *cqe;
    struct __kernel_timespec timeout = {.tv_sec = 0, .tv_nsec = 100 * 1000000}; // 100ms
    while (!io_uring_wait_cqe_timeout(ring, &cqe, &timeout))
    {
        struct workitem *work = io_uring_cqe_get_data(cqe);
        io_uring_cqe_seen(ring, cqe);
        if (--work->submittion->count > 0)
        {
            continue;
        }
        work->submittion->error = true;
        work->submittion->result = rc;
        eventfd_write(work->submittion->notifyfd, 1);
    }
}

void close_ring_with_error(struct io_uring *ring, int rc)
{
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (sqe)
    {
        io_uring_prep_cancel(sqe, 0, 0); // Cancel all, if possible
        io_uring_submit_and_get_events(ring);
    }
    mark_all_cqes_as_errors(ring, rc);
    io_uring_queue_exit(ring);
}

void *do_ring_work(void *arg)
{
    // Set I/O priority to a low value (Best Effort class with lowest priority within class)
    // since we want to use this thread for running flush & sync operation in the background
    // and those are not urgent. We'll let the OS schedule them as needed.
    int rc;
    if (g_cfg.low_priority_io)
    {
        int ioprio = IOPRIO_PRIO_VALUE(IOPRIO_CLASS_BE, 7);                  // 7 is the lowest priority in Best Effort class
        int result = syscall(SYS_ioprio_set, IOPRIO_WHO_PROCESS, 0, ioprio); // 0 means current thread
        (void)result;                                                        // explicitly ignoring this error, it doesn't matter if we cannot do that
    }
    pthread_setname_np(pthread_self(), "Rvn.Ring.Wrkr");

    struct io_uring *ring = &g_worker.ring;
    struct workitem *work = NULL;
    while (true)
    {
        {
            // wait for any writes on the eventfd / completion on the ring (associated with the eventfd)
            eventfd_t v;
            if (eventfd_read(g_worker.eventfd, &v))
            {
                rc = errno;
                goto error;
            }
        }
        bool has_work = true;
        while (has_work)
        {
            int must_wait = 0;
            has_work = false;
            if (!work) // we may have _previous_ work to run through
            {
                work = atomic_exchange(&g_worker.head, 0);
            }
            while (work)
            {
                has_work = true;

                struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
                if (sqe == NULL)
                {
                    must_wait = 1;
                    goto sumbit_and_wait; // will retry
                }
                io_uring_sqe_set_data(sqe, work);
                switch (work->type)
                {
                case workitem_open_folders:
                    io_uring_prep_open(sqe, work->op.folder_sync.path, O_RDONLY, 0);
                    break;
                case workitem_fsync_folders:
                {
                    struct io_uring_sqe *sqe2 = io_uring_get_sqe(ring);
                    if (sqe2 == NULL)
                    {
                        io_uring_prep_nop(sqe);
                        must_wait = 2;
                        goto sumbit_and_wait; // will retry
                    }
                    io_uring_prep_fsync(sqe, work->op.folder_sync.folderid, 0);
                    io_uring_sqe_set_data64(sqe, 0);
                    sqe->flags |= IOSQE_IO_LINK;
                    io_uring_prep_close(sqe2, work->op.folder_sync.folderid);
                    io_uring_sqe_set_data(sqe2, work);
                    break;
                }
                case workitem_fsync:
                    io_uring_prep_fsync(sqe, work->submittion->filefd, IORING_FSYNC_DATASYNC);
                    break;
                case workitem_write:
                    io_uring_prep_writev(sqe, work->submittion->filefd, work->op.write.iovecs, work->op.write.iovecs_count, work->op.write.offset);
                    break;
                default:
                    break;
                }
                work = work->next;
            }
        sumbit_and_wait:
            rc = must_wait ? io_uring_submit_and_wait(ring, must_wait) : io_uring_submit(ring);
            if (rc < 0)
            {
                rc = -rc;
                goto error;
            }
            struct io_uring_cqe *cqe;
            uint32_t head = 0;
            uint32_t i = 0;

            io_uring_for_each_cqe(ring, head, cqe)
            {
                i++;
                has_work = true; // force another run of the inner loop, to ensure that we call io_uring_submit again
                struct workitem *cur = io_uring_cqe_get_data(cqe);

                if (!cur)
                {
                    // can be null if it is:
                    // *  a notification about eventfd write
                    // *  multi stage process that completed a non final part
                    continue;
                }

                int result = cqe->res;
                switch (cur->type)
                {
                case workitem_open_folders:
                    cur->op.folder_sync.folderid = result;
                    notify_work_completed(ring, cur);
                    break;
                case workitem_fsync_folders:
                case workitem_fsync:
                    if (result != 0)
                    {
                        cur->submittion->error = true;
                        cur->submittion->result = result;
                    }
                    notify_work_completed(ring, cur);
                    break;
                case workitem_write:
                    if (result > 0)
                    {
                        cur->op.write.offset += result;
                        while (result)
                        {
                            if (result >= cur->op.write.iovecs->iov_len)
                            {
                                result -= cur->op.write.iovecs->iov_len;
                                cur->op.write.iovecs++;
                                cur->op.write.iovecs_count--;
                            }
                            else
                            {
                                cur->op.write.iovecs->iov_len -= result;
                                cur->op.write.iovecs->iov_base += result;
                                break;
                            }
                        }
                        if (result < 0)
                        {
                            // I'm *never* supposed to get to this line of code
                            // this is here as a safety net, to ensure that we if
                            // we messed up, we'll know about it rather than get an
                            // infinite loop or something like that
                            cur->submittion->error = true;
                            cur->submittion->result = ERANGE;
                            result = 0; // will force a completion of the current write
                        }
                        if (result)
                        {
                            queue_work(cur, cur);
                            continue;
                        }
                    }
                    else
                    {
                        cur->submittion->error = true;
                        if (result == 0)
                        {
                            // this usually happens if we a disk full, or some
                            cur->submittion->result = ENOSPC;
                        }
                    }
                    notify_work_completed(ring, cur);
                    break;
                default:
                    rc = FAIL_INVALID_CONFIGURATION;
                    goto error;
                }
            }
            io_uring_cq_advance(ring, i);
        }
    }

error:
    atomic_store(&g_worker.errored, rc);
    close(g_worker.eventfd);
    g_worker.eventfd = -1;
    close_ring_with_error(ring, rc);
    g_worker.ring = (struct io_uring){.ring_fd = -1};
    while (true)
    {
        if (!work) // fill from the global list if needed
        {
            work = atomic_exchange(&g_worker.head, 0);
            if (!work)
                break; // nothing remains to be done
        }
        while (work)
        {
            work->submittion->error = true;
            work->submittion->result = rc;
            if (--work->submittion->count > 0)
                continue;
            eventfd_write(work->submittion->notifyfd, 1);
            work = work->next;
        }
    }
    return 0;
}

bool _io_ring_supported()
{
    if (sizeof(void *) != 8)
        return 0; // not supported on 32 bits

    struct utsname buffer;
    if (uname(&buffer) != 0)
        return 0;

    int curr_major = 0, curr_minor = 0, curr_patch = 0;
    sscanf(buffer.release, "%d.%d.%d", &curr_major, &curr_minor, &curr_patch);

    // we require at least 5.10, because we need IOSQE_FIXED_FILE | IOSQE_IO_LINK & IORING_FEAT_NODROP
    return curr_major > 5 || (curr_major == 5 && curr_minor >= 10);
}

bool wake_ring_worker(int32_t *detailed_error_code)
{
    if (eventfd_write(g_worker.eventfd, 1))
    {
        *detailed_error_code = errno;
        // this means that the ring is probably dead, which is a catastrophic error
        // need to wait for the relevant thread to complete, to ensure we aren't
        // using the values we submitted to the ring
        pthread_join(g_worker.thread, NULL);
        return false;
    }
    return true;
}

int32_t
wait_for_work_completion(struct handle *handle_ptr, struct submittion *submittion, int32_t *detailed_error_code)
{
    if (!wake_ring_worker(detailed_error_code))
    {
        return FAIL_IO_RING_WRITE;
    }
    eventfd_t v;
    eventfd_read(submittion->notifyfd, &v);
    if (submittion->error)
    {
        *detailed_error_code = submittion->result;
    }
    return SUCCESS;
}

EXPORT int32_t
rvn_sync_pager(void *handle,
               int32_t *detailed_error_code)
{
    struct handle *handle_ptr = handle;
    if (!io_ring_setup_successful())
    {
        if (_flush_file(handle_ptr->file_fd))
        {
            *detailed_error_code = errno;
            return FAIL_SYNC_FILE;
        }
        return SUCCESS;
    }
    int eventfd = handle_ptr->global_state->fsync_dir_arena.eventfd;
    struct submittion submittion = {
        .filefd = handle_ptr->file_fd,
        .notifyfd = eventfd,
        .count = 1,
        .result = 0,
        .error = false,
    };
    struct workitem work = {
        .type = workitem_fsync,
        .submittion = &submittion,
    };
    queue_work(&work, &work);
    *detailed_error_code = 0;
    int32_t rc = wait_for_work_completion(handle_ptr, &submittion, detailed_error_code);
    if (*detailed_error_code)
    {
        rc = FAIL_SYNC_FILE;
    }
    return rc;
}

int32_t rvn_write_io_ring(
    void *handle,
    struct page_to_write *buffers,
    int32_t count,
    int32_t *detailed_error_code)
{
    *detailed_error_code = 0;
    int32_t rc = SUCCESS;
    struct handle *handle_ptr = handle;
    if (count == 0)
        return SUCCESS;

    if (g_worker.errored)
    {
        *detailed_error_code = g_worker.result;
        return FAIL_IO_RING_SUBMIT;
    }

    if (pthread_mutex_lock(&handle_ptr->global_state->writes_arena.lock))
    {
        *detailed_error_code = errno;
        return FAIL_MUTEX_LOCK;
    }
    // The worst case is that we have separate work item for each buffer, we assume that we
    // can usually do better, though..
    size_t max_req_size = (size_t)count * (sizeof(struct iovec) + sizeof(struct workitem));
    if (handle_ptr->global_state->writes_arena.arena_size < max_req_size)
    {
        size_t size = nextPowerOf2(max_req_size);
        void *ptr = realloc(handle_ptr->global_state->writes_arena.arena, size);
        if (!ptr)
        {
            *detailed_error_code = errno;
            pthread_mutex_unlock(&handle_ptr->global_state->writes_arena.lock);
            // ignoring errors in mutex unlock, since we already in error path
            return FAIL_NOMEM;
        }
        handle_ptr->global_state->writes_arena.arena = ptr;
        handle_ptr->global_state->writes_arena.arena_size = size;
    }
    struct submittion submittion = {
        .filefd = handle_ptr->file_fd,
        .notifyfd = handle_ptr->global_state->writes_arena.eventfd,
        .count = 0, // we merge work items for vector writes, so we update this below
        .result = 0,
        .error = false,
    };

    void *buf = handle_ptr->global_state->writes_arena.arena;
    struct workitem *head = NULL;
    struct workitem *last = buf;
    for (int32_t curIdx = count - 1; curIdx >= 0;)
    {
        int32_t startIdx = curIdx;
        while (startIdx > 0)
        {
            int32_t prevIdx = startIdx - 1;
            if (buffers[startIdx].page_num !=
                buffers[prevIdx].page_num + buffers[prevIdx].count_of_pages)
                break;

            int32_t iovecs = (curIdx - startIdx) + 1;
            if (iovecs >= IOV_MAX)
                break;

            startIdx--;
        }
        int64_t offset = buffers[startIdx].page_num * VORON_PAGE_SIZE;

        struct workitem *work = buf;
        struct iovec *iovecs = buf + sizeof(struct workitem);
        int32_t vec_count = curIdx - startIdx + 1;
        submittion.count++;
        *work = (struct workitem){
            .submittion = &submittion,
            .op.write = {
                .iovecs_count = vec_count,
                .iovecs = iovecs,
                .offset = offset,
            },
            .type = workitem_write,
            .next = head,
        };
        head = work;

        int32_t i = 0;
        for (size_t idx = startIdx; idx <= curIdx; idx++)
        {
            iovecs[i++] = (struct iovec){
                .iov_len = (int64_t)buffers[idx].count_of_pages * VORON_PAGE_SIZE,
                .iov_base = buffers[idx].ptr,
            };
        }
        buf += sizeof(struct workitem) + (sizeof(struct iovec) * vec_count);
        curIdx = startIdx - 1;
    }
    queue_work(head, last);
    rc = wait_for_work_completion(handle_ptr, &submittion, detailed_error_code);
    if (*detailed_error_code)
    {
        rc = FAIL_IO_RING_WRITE_RESULT;
    }

    if (pthread_mutex_unlock(&handle_ptr->global_state->writes_arena.lock) &&
        rc != SUCCESS)
    {
        *detailed_error_code = errno;
        return FAIL_MUTEX_UNLOCK;
    }
    return rc;
}

PRIVATE int32_t
rvn_one_time_init(int32_t* detailed_error_code)
{
    switch (g_cfg.write_mode)
    {
        case rvn_write_mode_io_ring:
            return rvn_io_ring_init(detailed_error_code);

        case rvn_mode_default:
            if (rvn_io_ring_init(detailed_error_code) == SUCCESS)
            {
                g_cfg.write_mode = rvn_write_mode_io_ring;
                return SUCCESS;
            }
            // no break here, we want to fall through
        case rvn_write_mode_vectored_file_io:
            g_cfg.write_mode = rvn_write_mode_vectored_file_io;
            return SUCCESS;

        case rvn_write_mode_file_io:
            g_cfg.write_mode = rvn_write_mode_file_io;
            return SUCCESS;

        default:
            *detailed_error_code = ENOTSUP;
            return FAIL_INVALID_CONFIGURATION;
    }
}


PRIVATE int32_t
rvn_io_ring_init(int32_t *detailed_error_code)
{
    if (g_cfg.io_ring_queue_size < 0 ||
        !_io_ring_supported())
    {
        *detailed_error_code = ENOTSUP;
        return FAIL_CREATE_IO_RING; // not supported or disabled, that is fine...
    }

    if (g_cfg.io_ring_queue_size < 3)
    {
        // we require at least 3 to allow efficient dir fsync
        *detailed_error_code = ENOSPC;
        return FAIL_CREATE_IO_RING;
    }

    struct io_uring_params params = {.flags = 0};

    int rc = io_uring_queue_init_params(g_cfg.io_ring_queue_size, &g_worker.ring, &params);
    if (rc)
    {
        *detailed_error_code = -rc;
        rc = FAIL_CREATE_IO_RING;
        goto error;
    }
    g_worker.eventfd = eventfd(0, EFD_CLOEXEC);
    if (g_worker.eventfd == -1)
    {
        *detailed_error_code = errno;
        rc = FAIL_CREATE_EVENTFD;
        goto error;
    }
    rc = io_uring_register_eventfd(&g_worker.ring, g_worker.eventfd);
    if (rc)
    {
        *detailed_error_code = -rc;
        rc = FAIL_IO_RING_REG_EVENTFD;
        goto error;
    }

    rc = pthread_create(&g_worker.thread, NULL, do_ring_work, NULL);
    if (rc != 0)
    {
        *detailed_error_code = rc;
        rc = FAIL_CREATE_THREAD;
        goto error;
    }
    return SUCCESS;
error:
    io_uring_queue_exit(&g_worker.ring);
    if (g_worker.eventfd != -1)
        close(g_worker.eventfd);
    g_worker = (struct worker){
        .eventfd = -1,
        .errored = true,
        .result = rc,
        .ring = {.ring_fd = -1},
    };
    return rc;
}

int io_ring_setup_successful()
{
    return g_worker.ring.ring_fd != -1;
}

PRIVATE int32_t
rvn_sync_directories_ioring(void *handle, char **folders, int32_t count, int32_t *detailed_error_code)
{
    int32_t rc = SUCCESS;
    struct handle *handle_ptr = handle;

    size_t req_size = count * sizeof(struct workitem);
    if (count > INT32_MAX / sizeof(struct workitem))
    {
        *detailed_error_code = EOVERFLOW;
        return FAIL_MATH_OVERFLOW;
    }

    if (pthread_mutex_lock(&handle_ptr->global_state->fsync_dir_arena.lock))
    {
        *detailed_error_code = errno;
        return FAIL_MUTEX_LOCK;
    }

    if (handle_ptr->global_state->fsync_dir_arena.arena_size < req_size)
    {
        size_t size = nextPowerOf2(req_size);
        void *ptr = realloc(handle_ptr->global_state->fsync_dir_arena.arena, size);
        if (!ptr)
        {
            *detailed_error_code = errno;
            pthread_mutex_unlock(&handle_ptr->global_state->fsync_dir_arena.lock);
            // ignoring errors in mutex unlock, since we are on an error path
            return FAIL_NOMEM;
        }
        handle_ptr->global_state->fsync_dir_arena.arena = ptr;
        handle_ptr->global_state->fsync_dir_arena.arena_size = size;
    }
    struct workitem *arr = handle_ptr->global_state->fsync_dir_arena.arena;
    int eventfd = handle_ptr->global_state->fsync_dir_arena.eventfd;

    // we need to do:
    // foreach folder:
    //      open folder
    //      fsync folder
    //      close folder
    //
    // we cannot open & fsync at the same time, because we have no way
    // to get the file descriptor for the folder, so we need to do it in two steps.
    // We still can do that in a batched manner, though...

    struct submittion submittion = {
        .notifyfd = eventfd,
        .count = count,
        .result = 0,
        .error = false,
    };

    for (size_t i = 0; i < count; i++)
    {
        arr[i] = (struct workitem){
            .type = workitem_open_folders,
            .submittion = &submittion,
            .next = &arr[i + 1], // fine to also set on the last, queue_work will override it
            .op = {.folder_sync = {.path = folders[i]}}};
    }
    *detailed_error_code = 0;
    queue_work(&arr[0], &arr[count - 1]);
    rc = wait_for_work_completion(handle_ptr, &submittion, detailed_error_code);
    if (rc != SUCCESS)
    {
        pthread_mutex_unlock(&handle_ptr->global_state->fsync_dir_arena.lock);
        // ignoring possible error code because we are in error path
        return rc;
    }
    // here we are *explicitly* ignoring wait_for_work_completion() detailed_error_code
    // since it is _fine_ to have errors opening the folder, such as when we deleted an
    // index before we had a chance to flush it
    *detailed_error_code = 0;
    submittion.count = 0;
    for (size_t i = 0; i < count; i++)
    {
        int folderid = arr[i].op.folder_sync.folderid;
        if (folderid < 0)
            continue;

        struct workitem *previous = NULL;
        if (submittion.count)
        {
            previous = &arr[submittion.count - 1];
        }
        arr[submittion.count++] = (struct workitem){
            .type = workitem_fsync_folders,
            .op = {.folder_sync = {.folderid = folderid}},
            .submittion = &submittion,
            .next = previous};
    }

    if (!submittion.count)
    {
        return SUCCESS;
    }
    queue_work(&arr[submittion.count - 1], &arr[0]);
    *detailed_error_code = 0;
    rc = wait_for_work_completion(handle_ptr, &submittion, detailed_error_code);
    if (*detailed_error_code)
        rc = FAIL_IO_RING_WRITE_RESULT;

    if (pthread_mutex_unlock(&handle_ptr->global_state->fsync_dir_arena.lock) &&
        rc != SUCCESS)
    {
        *detailed_error_code = errno;
        return FAIL_MUTEX_UNLOCK;
    }
    return rc;
}
