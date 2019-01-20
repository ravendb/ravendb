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
#include <assert.h>

#include "rvn.h"
#include "status_codes.h"
#include "internal_posix.h"

EXPORT int32_t
rvn_create_and_mmap64_file(const char *path,
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

    int32_t fd = open(path, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    *(int32_t*)handle = fd;
    if (fd == -1)
    {
        rc = FAIL_OPEN_FILE;
        goto error_cleanup;
    }

    int64_t sz = lseek(fd, 0, SEEK_END);
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
        sz = _nearest_size_to_page_size(max(initial_file_size, sz), allocation_granularity);
    }

    rc = _allocate_file_space(fd, sz, detailed_error_code);
    if (rc != SUCCESS)
        goto error_clean_With_error;

    *actual_file_size = sz;

    if (_sync_directory_allowed(fd) == SYNC_DIR_ALLOWED)
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

    void *address = rvn_mmap(NULL, sz, PROT_READ | PROT_WRITE, mmap_flags, fd, 0L);

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
    if (fd != -1)
        close(fd);

    return rc;
}

EXPORT int32_t
rvn_allocate_more_space(const char *filename, int64_t new_length_after_adjustment, void *handle, int32_t flags,
                        void **new_address, int32_t *detailed_error_code)
{
    int32_t fd = (int32_t)(int64_t)handle;
    int32_t rc = SUCCESS;
    
    rc = _allocate_file_space(fd, new_length_after_adjustment, detailed_error_code);
    if (rc != SUCCESS)
        goto error_clean_With_error;

    if (_sync_directory_allowed(fd) == SYNC_DIR_ALLOWED)
    {
        rc = _sync_directory_for(filename, detailed_error_code);
        if (rc != SUCCESS)
            goto error_clean_With_error;
    }

    int32_t mmap_flags = 0;
    if (flags & MMOPTIONS_COPY_ON_WRITE)
        mmap_flags |= MAP_PRIVATE;
    else
        mmap_flags |= MAP_SHARED;

    void *address = rvn_mmap(NULL, new_length_after_adjustment, PROT_READ | PROT_WRITE, mmap_flags, fd, 0L);
    if (address == MAP_FAILED)
    {
        rc = FAIL_MMAP64;
        goto error_cleanup;
    }

    *new_address = address;

    return SUCCESS;

error_cleanup:
    *detailed_error_code = errno;
error_clean_With_error:
    if (fd != -1)
        close(fd);

    return rc;
}

EXPORT int32_t
rvn_unmap(void *address, int64_t size, int32_t delete_on_close, int32_t *detailed_error_code)
{    
    if (delete_on_close == DELETE_ON_CLOSE_YES)
        madvise(address, size, MADV_DONTNEED); /* ignore error */        

    int32_t rc = munmap(address, size);
    if (rc != 0)
        *detailed_error_code = errno;

    return rc;
}

EXPORT int32_t
rvn_mmap_dispose_handle(const char *filepath, void *handle, int32_t delete_on_close, int32_t *detailed_error_code)
{
    int32_t rc = SUCCESS;

    /* the following in two lines to avoid compilation warning */
    int32_t fd = (int32_t)(int64_t)handle;

    if (fd != -1)
    {
        if (delete_on_close == DELETE_ON_CLOSE_YES)
        {
            int32_t unlink_rc = unlink(filepath);
            if (unlink_rc != 0)
            {
                /* record the error and continue to close */
                rc = FAIL_UNLINK;
                *detailed_error_code = errno;
            }
        }

        int32_t close_rc = close(fd);
        if (close_rc != 0)
        {
            if (rc == 0) /* if unlink failed - return unlink's error */
            {
                rc = FAIL_CLOSE;
                *detailed_error_code = errno;
            }
        }
        return rc;
    }

    return FAIL_INVALID_HANDLE;
}
