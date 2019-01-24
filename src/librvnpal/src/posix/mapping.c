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

#include "rvn.h"
#include "status_codes.h"
#include "internal_posix.h"

struct map_file_handle
{
    int fd;
    const char* path;
    int flags;
};

PRIVATE void free_map_file_handle(struct map_file_handle *handle)
{
    if (handle->path != NULL)
    {
        free((void *)(handle->path));
        (handle)->path = NULL;
    }

    free((void *)handle);
}

EXPORT int32_t
rvn_create_and_mmap64_file(
    const char *path,
    int64_t initial_file_size,
    int32_t flags,
    void **handle,
    void **base_addr,
    int64_t *actual_file_size,
    int32_t *detailed_error_code)
{
    int32_t rc = SUCCESS;

    assert(initial_file_size > 0);

    _ensure_path_exists(path);
      
    struct map_file_handle* mfh = calloc(1, sizeof(struct map_file_handle));
    *handle = mfh;
    if(mfh == NULL)
    {
        rc = FAIL_CALLOC;
        goto error_cleanup;
    }
    mfh->path = strdup(path);
    if (mfh->path == NULL)
    {
        rc = FAIL_CALLOC;
        goto error_cleanup;
    }
    mfh->flags = flags;
    mfh->fd = open(path, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    if (mfh->fd == -1)
    {
        rc = FAIL_OPEN_FILE;
        goto error_cleanup;
    }

    int64_t sz = lseek(mfh->fd, 0, SEEK_END);
    if (sz == -1)
    {
        rc = FAIL_SEEK_FILE;
        goto error_cleanup;
    }

    int64_t sys_page_size = sysconf(_SC_PAGE_SIZE);
    if (sys_page_size == -1)
    {
        rc = FAIL_SYSCONF;
        goto error_cleanup;
    }

    int32_t allocation_granularity = ALLOCATION_GRANULARITY;
    if (initial_file_size < allocation_granularity)
        initial_file_size = allocation_granularity;

    if (sz <= initial_file_size || sz % sys_page_size != 0)
    {
        sz = _nearest_size_to_page_size(rvn_max(initial_file_size, sz), allocation_granularity);
    }

    rc = _allocate_file_space(mfh->fd, sz, detailed_error_code);
    if (rc != SUCCESS)
        goto error_clean_With_error;

    *actual_file_size = sz;

    if (_sync_directory_allowed(mfh->fd) == SYNC_DIR_ALLOWED)
    {
        rc = _sync_directory_for(path, detailed_error_code);
        if (rc != SUCCESS)
            goto error_clean_With_error;
    }

    int32_t mmap_flags = 0;
    if (flags & MMOPTIONS_COPY_ON_WRITE)
        mmap_flags |= MAP_PRIVATE;
    else
        mmap_flags |= MAP_SHARED;

    void *address = rvn_mmap(NULL, sz, PROT_READ | PROT_WRITE, mmap_flags, mfh->fd, 0L);

    if (address == MAP_FAILED)
    {
        rc = FAIL_MMAP64;
        goto error_cleanup;
    }

    *base_addr = address;

    return rc; /* SUCCESS */

error_cleanup:
    *detailed_error_code = errno;
error_clean_With_error:
    if (mfh != NULL)
    {
        if (mfh->fd != -1)
            close(mfh->fd);
        free_map_file_handle(mfh);
        *handle = NULL;
    }

    return rc;
}

EXPORT int32_t
rvn_allocate_more_space(
    int64_t new_length_after_adjustment, 
    void *handle,
    void **new_address, 
    int32_t *detailed_error_code)
{
    struct map_file_handle* mfh = (struct map_file_handle *)handle;
    int32_t rc = SUCCESS;

    rc = _allocate_file_space(mfh->fd, new_length_after_adjustment, detailed_error_code);
    if (rc != SUCCESS)
        return rc;

    if (_sync_directory_allowed(mfh->fd) == SYNC_DIR_ALLOWED)
    {
        rc = _sync_directory_for(mfh->path, detailed_error_code);
        if (rc != SUCCESS)
            return rc;
    }

    int32_t mmap_flags = 0;
    if (mfh->flags & MMOPTIONS_COPY_ON_WRITE)
        mmap_flags |= MAP_PRIVATE;
    else
        mmap_flags |= MAP_SHARED;

    void *address = rvn_mmap(NULL, new_length_after_adjustment, PROT_READ | PROT_WRITE, mmap_flags, mfh->fd, 0L);
    if (address == MAP_FAILED)
    {
        rc = FAIL_MMAP64;
        goto error_cleanup;
    }

    *new_address = address;

    return SUCCESS;

error_cleanup:
    *detailed_error_code = errno;
    return rc;
}

EXPORT int32_t
rvn_unmap(void *handle, void *address, int64_t size, int32_t *detailed_error_code)
{
    struct map_file_handle *mfh = (struct map_file_handle *)handle;
    int32_t _;
    if (mfh->flags & MMOPTIONS_DELETE_ON_CLOSE)
        rvn_discard_virtual_memory(address, size, &_); /* ignore error */

    int32_t rc = munmap(address, size);
    if (rc != 0)
        *detailed_error_code = errno;

    return rc;
}

EXPORT int32_t
rvn_mmap_dispose_handle(void *handle, int32_t *detailed_error_code)
{
    struct map_file_handle *mfh = (struct map_file_handle *)handle;
    int32_t rc = SUCCESS;

    if (mfh->fd == -1)
    {
        rc = FAIL_INVALID_HANDLE;
        goto error_cleanup;
    }

    if (mfh->flags & MMOPTIONS_DELETE_ON_CLOSE)
    {
        int32_t unlink_rc = unlink(mfh->path);
        if (unlink_rc != 0)
        {
            /* record the error and continue to close */
            rc = FAIL_UNLINK;
            *detailed_error_code = errno;
        }
    }

    int32_t close_rc = close(mfh->fd);
    if (close_rc != 0)
    {
        if (rc == 0) /* if unlink failed - return unlink's error */
        {
            rc = FAIL_CLOSE;
            *detailed_error_code = errno;
            goto error_cleanup;
        }
    }
    goto cleanup;

error_cleanup:
    *detailed_error_code = errno;
cleanup:
    free_map_file_handle(mfh);
    return rc;
}

EXPORT int32_t
rvn_discard_virtual_memory(void* address, int64_t size, int32_t* detailed_error_code)
{
    int32_t rc = madvise(address, size, MADV_DONTNEED);
    if (rc != 0)
        *detailed_error_code = errno;
    return rc;
}
