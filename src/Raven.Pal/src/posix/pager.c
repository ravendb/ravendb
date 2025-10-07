#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <unistd.h>
#include <libgen.h>
#include <limits.h>
#include <string.h>
#if !__APPLE__
#include <sys/eventfd.h>
#endif
#include "rvn.h"
#include "rvn_internal.h"
#include "status_codes.h"
#include "internal_posix.h"

extern struct rvn_configuration g_cfg;

int32_t rvn_lock_memory(struct handle *handle, void *mem, int64_t size, int32_t *detailed_error_code)
{
    int32_t rc = SUCCESS;
    if (sizeof(size_t) == 4)
    {
        if (size >= INT32_MAX) /* we won't uspport large values on 32 bits */
        {
            rc = FAIL_SIZE_INVALID_32_BITS;
            goto Exit;
        }
    }
    if (size <= 0)
    {
        rc = FAIL_SIZE_NEGATIVE_OR_ZERO;
        goto Exit;
    }

    if (!mlock(mem, size) ||
        handle->global_state->open_flags & OPEN_FILE_DO_NOT_CONSIDER_MEMORY_LOCK_FAILURE_AS_CATASTROPHIC_ERROR)
    {
        // note that we *explicitly* account for locked memory even if we failed to do so
        // if we don't consider this as catastrophic error, because otherwise accounting
        // for its removal is really complicated
        g_cfg.memoryLockCallback(size, handle->global_state->file_path);
        return SUCCESS;
    }
    rc = FAIL_LOCK_MEMORY;
Exit:
    *detailed_error_code = errno;
    return rc;
}

int32_t _open_pager_file(int fd,
                         struct handle_global_state *global_state,
                         int64_t req_file_size,
                         void **handle,
                         void **memory,
                         void **writable_memory,
                         int64_t *memory_size,
                         int32_t *detailed_error_code)
{
    int32_t rc = SUCCESS;
    struct handle *handle_ptr = NULL;
    void *mem = NULL;
    void *wmem = NULL;

    handle_ptr = calloc(1, sizeof(struct handle));
    if (handle_ptr == NULL)
    {
        rc = FAIL_NOMEM;
        goto Error;
    }
    handle_ptr->global_state = global_state;

    int64_t min_file_size = rvn_max(
        (req_file_size + ALLOCATION_GRANULARITY - 1) & ~(ALLOCATION_GRANULARITY - 1),
        ALLOCATION_GRANULARITY);

    struct stat st;
    if (fstat(fd, &st) == -1)
    {
        rc = FAIL_STAT_FILE;
        goto Error;
    }

    if (min_file_size > st.st_size && !(global_state->open_flags & OPEN_FILE_READ_ONLY))
    {
        st.st_size = min_file_size;
        rc = _allocate_file_space(fd, st.st_size, detailed_error_code);
        if (rc != SUCCESS)
            goto Error;
        if (_sync_directory_allowed(fd) == SYNC_DIR_ALLOWED)
        {
            rc = _sync_directory_for(global_state->file_path, detailed_error_code);
            if (rc != SUCCESS)
                goto Error;
        }
    }
    else if (st.st_size == 0 && (global_state->open_flags & OPEN_FILE_READ_ONLY))
    {
        handle_ptr->global_state->open_flags |= OPEN_FILE_DO_NOT_MAP;
        // we allow opening zero len files with read only mode, but don't try to map them
        handle_ptr->file_fd = fd;
        *memory_size = 0;
        *handle = handle_ptr;
        return SUCCESS;
    }

    if ((global_state->open_flags & OPEN_FILE_DO_NOT_MAP))
    {
        handle_ptr->file_fd = fd;
        handle_ptr->allocation_size = st.st_size;
        *memory_size = st.st_size;
        *handle = handle_ptr;
        return SUCCESS;
    }

    int32_t mmap_flags = (global_state->open_flags & OPEN_FILE_COPY_ON_WRITE) ? MAP_PRIVATE : MAP_SHARED;
    mem = rvn_mmap(NULL, st.st_size, PROT_READ, mmap_flags, fd, 0L);
    if (mem == NULL)
    {
        rc = FAIL_MAP_VIEW_OF_FILE;
        goto Error;
    }

    if (global_state->open_flags & OPEN_FILE_WRITABLE_MAP)
    {
        wmem = rvn_mmap(NULL, st.st_size, PROT_READ | PROT_WRITE, mmap_flags, fd, 0L);
        if (wmem == NULL)
        {
            rc = FAIL_MAP_VIEW_OF_FILE;
            goto Error;
        }
    }

    if (global_state->open_flags & OPEN_FILE_LOCK_MEMORY &&
        // We map the memory twice if we have WRITE access, but in phsyical memory, it ends up being the same
        // thing, so we only lock it once.
        rvn_lock_memory(handle_ptr, mem, st.st_size, detailed_error_code) != SUCCESS)
    {
        rc = FAIL_LOCK_MEMORY;
        goto Error;
    }

    handle_ptr->file_fd = fd;
    handle_ptr->read_address = mem;
    handle_ptr->write_address = wmem;
    handle_ptr->allocation_size = st.st_size;
    *handle = handle_ptr;
    *memory = mem;
    *writable_memory = wmem;
    *memory_size = st.st_size;
    return SUCCESS;

Error:
    *detailed_error_code = errno;
    if (mem != NULL)
    {
        munmap(mem, st.st_size);
    }
    if (wmem != NULL)
    {
        munmap(wmem, st.st_size);
    }
    close(fd);
    free(handle_ptr);
    return rc;
}

void delete_global_state(struct handle_global_state *global_state)
{
    if (global_state == NULL)
        return;
    if (global_state->writes_arena.eventfd != -1)
        close(global_state->writes_arena.eventfd);
    if (global_state->fsync_dir_arena.eventfd != -1)
        close(global_state->fsync_dir_arena.eventfd);
    free(global_state->writes_arena.arena);
    free(global_state->fsync_dir_arena.arena);
    free(global_state->file_path);
    pthread_mutex_destroy(&global_state->writes_arena.lock);
    pthread_mutex_destroy(&global_state->fsync_dir_arena.lock);
    free(global_state);
}

EXPORT int32_t
rvn_init_pager(const char *filename,
               int64_t initial_file_size,
               int32_t open_flags,
               void **handle,
               void **memory,
               void **writable_memory,
               int64_t *memory_size,
               int32_t *detailed_error_code)
{
    *memory_size = 0;
    *memory = NULL;
    *handle = NULL;
    int32_t rc = SUCCESS;
    int fd = -1;

    assert(filename);
    assert(filename[0] != '\0');

    struct handle_global_state *global_state = calloc(1, sizeof(struct handle_global_state));
    if (global_state == NULL)
    {
        *detailed_error_code = ENOMEM;
        return FAIL_NOMEM;
    }

    rvn_write_mode write_mode = g_cfg.write_mode;
    if (write_mode == rvn_write_mode_mmap && ((open_flags & OPEN_FILE_READ_ONLY) != OPEN_FILE_READ_ONLY))
    {
        open_flags |= OPEN_FILE_WRITABLE_MAP;
    }

    global_state->open_flags = open_flags;
    global_state->ref_count = 1;
    global_state->writes_arena = (struct arena){.eventfd = -1};
    global_state->fsync_dir_arena = (struct arena){.eventfd = -1};
    global_state->writes_arena.lock = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
    global_state->fsync_dir_arena.lock = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
#if !__APPLE__
    global_state->writes_arena.eventfd = eventfd(0, EFD_CLOEXEC);
    if (global_state->writes_arena.eventfd == -1)
    {
        *detailed_error_code = errno;
        rc = FAIL_CREATE_EVENTFD;
        goto Error;
    }
    global_state->fsync_dir_arena.eventfd = eventfd(0, EFD_CLOEXEC);
    if (global_state->fsync_dir_arena.eventfd == -1)
    {
        *detailed_error_code = errno;
        rc = FAIL_CREATE_EVENTFD;
        goto Error;
    }
#endif
    if (g_cfg.write_mode == rvn_write_mode_io_ring &&
        !io_ring_setup_successful())
    {
        *detailed_error_code = ENOTSUP;
        rc = FAIL_CREATE_IO_RING;
        goto Error;
    }

    // have to copy, dirname is mutating the buffer
    char *dup_path = strdup(filename);
    if (dup_path == NULL)
    {
        *detailed_error_code = errno;
        rc = FAIL_NOMEM;
        goto Error;
    }
    char *directory = dirname(dup_path);
    rc = _ensure_path_exists(directory, detailed_error_code);
    free(dup_path);
    if (rc != SUCCESS)
        goto Error;

    global_state->file_path = strdup(filename);
    if (global_state->file_path == NULL)
    {
        *detailed_error_code = errno;
        rc = FAIL_NOMEM;
        goto Error;
    }

    int flags = ((open_flags & OPEN_FILE_READ_ONLY) | (open_flags & OPEN_FILE_COPY_ON_WRITE)) ? O_RDONLY : O_RDWR | O_CREAT;
    fd = open(filename, flags, S_IWUSR | S_IRUSR);
    if (fd == -1)
    {
        rc = FAIL_OPEN_FILE;
        *detailed_error_code = errno;
        goto Error;
    }

    rc = _open_pager_file(fd, global_state, initial_file_size, handle, memory, writable_memory, memory_size, detailed_error_code);

    if (rc == SUCCESS)
        return SUCCESS;

Error:
    if (fd != -1)
    {
        close(fd);
    }
    delete_global_state(global_state);
    return rc;
}

int32_t
rvn_increase_pager_size(void *handle,
                        int64_t new_length,
                        void **new_handle,
                        void **memory,
                        void **writable_memory,
                        int64_t *memory_size,
                        int32_t *detailed_error_code)
{
    struct handle *handle_ptr = handle;
    *memory = NULL;
    *memory_size = 0;

    int new_fd = dup(handle_ptr->file_fd);
    if (new_fd == -1)
    {
        *new_handle = NULL;
        *detailed_error_code = errno;
        return FAIL_DUPLICATE_HANDLE;
    }
    int32_t rc = SUCCESS;
    if (pthread_mutex_lock(&handle_ptr->global_state->writes_arena.lock))
    {
        *detailed_error_code = errno;
        rc = FAIL_MUTEX_LOCK;
        goto error;
    }
    rc = _open_pager_file(new_fd, handle_ptr->global_state, new_length, new_handle, memory, writable_memory, memory_size, detailed_error_code);
    if (rc == SUCCESS)
    {
        handle_ptr->global_state->ref_count++;
    }
    if (pthread_mutex_unlock(&handle_ptr->global_state->writes_arena.lock))
    {
        *detailed_error_code = errno;
        rc = FAIL_MUTEX_UNLOCK;
        goto error;
    }
error:
    if (rc != SUCCESS)
    {
        close(new_fd);
    }
    return rc;
}

EXPORT int32_t
rvn_close_pager(
    void *handle,
    int32_t *detailed_error_code)
{
    if (handle == NULL)
    {
        return FAIL_INVALID_HANDLE;
    }
    struct handle *handle_ptr = handle;
    *detailed_error_code = 0;
    int rc = SUCCESS;
    if (!(handle_ptr->global_state->open_flags & OPEN_FILE_DO_NOT_MAP))
    {
        if (handle_ptr->global_state->open_flags & OPEN_FILE_LOCK_MEMORY)
        {
            g_cfg.memoryLockCallback(-handle_ptr->allocation_size, handle_ptr->global_state->file_path);
        }

        if (munmap(handle_ptr->read_address, handle_ptr->allocation_size))
        {
            *detailed_error_code = errno;
            rc = FAIL_MAP_VIEW_OF_FILE;
        }
        if (handle_ptr->global_state->open_flags & OPEN_FILE_WRITABLE_MAP)
        {
            if (munmap(handle_ptr->write_address, handle_ptr->allocation_size))
            {
                *detailed_error_code = errno;
                rc = FAIL_MAP_VIEW_OF_FILE;
            }
        }
    }

    if (close(handle_ptr->file_fd))
    {
        if (*detailed_error_code == 0)
            *detailed_error_code = errno;
        if (rc == SUCCESS)
            rc = FAIL_CLOSE;
    }

    if (pthread_mutex_lock(&handle_ptr->global_state->writes_arena.lock))
    {
        *detailed_error_code = errno;
        rc = FAIL_MUTEX_LOCK;
    }
    uint32_t refs = --handle_ptr->global_state->ref_count;
    if (pthread_mutex_unlock(&handle_ptr->global_state->writes_arena.lock))
    {
        *detailed_error_code = errno;
        rc = FAIL_MUTEX_UNLOCK;
    }
    if (refs == 0)
    {
        delete_global_state(handle_ptr->global_state);
    }
    free(handle_ptr);
    return rc;
}

EXPORT
int32_t rvn_pager_get_file_handle(
    void *handle,
    void **file_handle,
    int32_t *detailed_error_code)
{
    struct handle *handle_ptr = handle;
    int new_fd = dup(handle_ptr->file_fd);
    if (new_fd == -1)
    {
        *file_handle = NULL;
        *detailed_error_code = errno;
        return FAIL_DUPLICATE_HANDLE;
    }
    /* intentionally passing the fd this way */
    *file_handle = (void *)(intptr_t)new_fd;
    return SUCCESS;
}

EXPORT int32_t
rvn_pager_get_file_size(void *handle,
                        int64_t *total_size,
                        int64_t *phyiscal_size,
                        int32_t *detailed_error_code)
{
    struct handle *handle_ptr = handle;
    struct stat st;
    if (fstat(handle_ptr->file_fd, &st) == -1)
    {
        *detailed_error_code = errno;
        return FAIL_GET_FILE_SIZE;
    }
    *total_size = st.st_size;
    *phyiscal_size = st.st_blocks * 512;
    return SUCCESS;
}

EXPORT int32_t
rvn_pager_set_sparse_region(void *handle,
                            int64_t offset,
                            int64_t size,
                            int32_t *detailed_error_code)
{
    struct handle *handle_ptr = handle;
    if (handle_ptr->global_state->status_flags & PAGER_STATUS_SPARSE_NOT_SUPPORTED)
    {
        return FAIL_SPARSE_NOT_SUPPORTED;
    }
    int rc;

#if __APPLE__
    fpunchhole_t punchhole = {0};
    punchhole.fp_offset = offset;
    punchhole.fp_length = size;
    rc = fcntl(handle_ptr->file_fd, F_PUNCHHOLE, &punchhole);
#else
    rc = fallocate(handle_ptr->file_fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, size);
#endif
    if (rc == 0)
        return SUCCESS;

    if (errno == ENOTSUP)
    {
        handle_ptr->global_state->status_flags |= PAGER_STATUS_SPARSE_NOT_SUPPORTED;
        return FAIL_SPARSE_NOT_SUPPORTED;
    }

    *detailed_error_code = errno;
    return FAIL_SET_SPARSE_RANGE;
}

EXPORT
int32_t rvn_unmap_memory(
    void *handle,
    void *mem,
    int64_t size,
    int32_t *detailed_error_code)
{
    struct handle *handle_ptr = handle;
    *detailed_error_code = 0;
    if (munmap(mem, size))
    {
        *detailed_error_code = errno;
        return FAIL_UNMAP_VIEW_OF_FILE;
    }
    if (handle_ptr->global_state->open_flags & OPEN_FILE_LOCK_MEMORY)
    {
        g_cfg.memoryLockCallback(-size, handle_ptr->global_state->file_path);
    }
    return SUCCESS;
}
EXPORT
int32_t rvn_map_memory(void *handle,
                       int64_t offset,
                       int64_t size,
                       void **mem,
                       int32_t *detailed_error_code)
{
    int32_t rc = SUCCESS;
    if (sizeof(void *) == 4)
    {
        if (size > INT32_MAX)
        {
            rc = FAIL_SIZE_INVALID_32_BITS;
            goto Error;
        }
    }
    if (size <= 0)
    {
        rc = FAIL_SIZE_NEGATIVE_OR_ZERO;
        goto Error;
    }

    struct handle *handle_ptr = handle;

    int32_t mmap_flags = (handle_ptr->global_state->open_flags & OPEN_FILE_COPY_ON_WRITE) ? MAP_PRIVATE : MAP_SHARED;
    int32_t prot = (handle_ptr->global_state->open_flags & OPEN_FILE_WRITABLE_MAP) ? PROT_READ | PROT_WRITE : PROT_READ;
    *mem = rvn_mmap(NULL, size, prot, mmap_flags, handle_ptr->file_fd, offset);
    if (*mem == NULL)
    {
        rc = FAIL_MAP_VIEW_OF_FILE;
        goto Error;
    }

    if (handle_ptr->global_state->open_flags & OPEN_FILE_LOCK_MEMORY)
    {
        // intentionally returning the error code & rc from the lock_memory call
        rc = rvn_lock_memory(handle_ptr, *mem, size, detailed_error_code);
        if (rc != SUCCESS)
        {
            goto Error;
        }
    }

    return SUCCESS;

Error:
    *detailed_error_code = errno;
    if (*mem)
    {
        munmap(*mem, size);
        *mem = NULL;
    }
    return rc;
}

int32_t rvn_write_file_io(
    void *handle,
    struct page_to_write *buffers,
    int32_t count,
    int32_t *detailed_error_code)
{
    struct handle *handle_ptr = handle;
    for (int32_t i = 0; i < count; i++)
    {
        int64_t offset = buffers[i].page_num * VORON_PAGE_SIZE;
        int64_t size = (int64_t)buffers[i].count_of_pages * VORON_PAGE_SIZE;
        int32_t rc = _pwrite(handle_ptr->file_fd, buffers[i].ptr, size, offset, detailed_error_code);
        if (rc != SUCCESS)
            return rc;
    }
    return SUCCESS;
}

int32_t rvn_write_mmap(
    void *handle,
    struct page_to_write *buffers,
    int32_t count,
    int32_t *detailed_error_code)
{
    struct handle *handle_ptr = handle;
    for (int32_t i = 0; i < count; i++)
    {
        int64_t offset = buffers[i].page_num * VORON_PAGE_SIZE;
        int64_t size = (int64_t)buffers[i].count_of_pages * VORON_PAGE_SIZE;
        memcpy((char *)handle_ptr->write_address + offset, buffers[i].ptr, (size_t)size);
    }
    if (msync(handle_ptr->write_address, handle_ptr->allocation_size, MS_SYNC))
    {
        *detailed_error_code = errno;
        return FAIL_SYNC_FILE;
    }
    return SUCCESS;
}

int32_t rvn_write_invalid_setup(
    void *handle,
    struct page_to_write *buffers,
    int32_t count,
    int32_t *detailed_error_code)
{
    *detailed_error_code = ENOTSUP;
    return FAIL_INVALID_HANDLE;
}