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

#include "rvn.h"
#include "status_codes.h"
#include "internal_posix.h"

struct map_file_handle
{
    int fd;
    const char* path;
    int flags;
};

EXPORT int32_t
rvn_file_sync(void *handle, int32_t *detailed_error_code)
{
    struct map_file_handle* mfh = (struct map_file_handle *)handle;
    int32_t rc = _flush_file(mfh->fd);
    if (rc != SUCCESS)
        *detailed_error_code = errno;
    return rc;
}


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
rvn_create_file(const char *path,
    int64_t initial_file_size,
    int32_t flags,
    void **handle,    
    int64_t *actual_file_size,
    int32_t *detailed_error_code)
{
    int32_t rc = SUCCESS;

    assert(path);
    assert(path[0] != '\0');
    assert(initial_file_size > 0);

    struct map_file_handle *mfh = calloc(1, sizeof(struct map_file_handle));
    *handle = mfh;
    if(mfh == NULL)
    {
        rc = FAIL_CALLOC;
        goto error_clean_with_error;
    }

    char* dup_path = strdup(path);
    if (dup_path == NULL)
    {
        rc = FAIL_NOMEM;
        goto error_clean_with_error;
    }
    char *directory = dirname(dup_path);
    rc = _ensure_path_exists(directory, detailed_error_code);
    free(dup_path);
    if (rc != SUCCESS) 
        goto error_clean_with_error;

    mfh->path = strdup(path);
    if (mfh->path == NULL)
    {
        rc = FAIL_CALLOC;
        goto error_clean_with_error;
    }
    mfh->flags = flags;
    int32_t largefile = 0;
     if (sizeof(int) == 4) /* 32 bits */
        largefile = O_LARGEFILE;

    mfh->fd = open(path, O_RDWR | O_CREAT | largefile, S_IWUSR | S_IRUSR);
    if (mfh->fd == -1)
    {
        rc = FAIL_OPEN_FILE;
        goto error_clean_with_error;
    }

    struct stat st;
    if (fstat(mfh->fd, &st) == -1)
    {
        rc = FAIL_STAT_FILE;
        goto error_clean_with_error;
    }

    int64_t sz = st.st_size;

    int64_t sys_page_size = sysconf(_SC_PAGE_SIZE);
    if (sys_page_size == -1)
    {
        rc = FAIL_SYSCONF;
        goto error_clean_with_error;
    }

    int32_t allocation_granularity = ALLOCATION_GRANULARITY;
    if (initial_file_size < allocation_granularity)
        initial_file_size = allocation_granularity;

    if (sz <= initial_file_size || sz % sys_page_size != 0)
    {
        sz = _nearest_size_to_page_size(rvn_max(initial_file_size, sz), allocation_granularity);
    }

    if(sz != st.st_size)
    {
        rc = _allocate_file_space(mfh->fd, sz, detailed_error_code);
        if (rc != SUCCESS)
            goto error_clean;
        if (_sync_directory_allowed(mfh->fd) == SYNC_DIR_ALLOWED)
        {
            rc = _sync_directory_for(path, detailed_error_code);
            if (rc != SUCCESS)
                goto error_clean;
        }
    }

    *actual_file_size = sz;

    return rc; /* SUCCESS */

error_clean_with_error:
    *detailed_error_code=errno;
error_clean:
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
rvn_create_and_mmap64_file(
    const char *path,
    int64_t initial_file_size,
    int32_t flags,
    void **handle,
    void **base_addr,
    int64_t *actual_file_size,
    int32_t *detailed_error_code)
{
    int32_t rc = rvn_create_file(path, initial_file_size, flags, handle, actual_file_size, detailed_error_code);
    if (rc == SUCCESS)
        rc = rvn_mmap_file(*actual_file_size, flags, *handle, 0L, base_addr, detailed_error_code);

    return rc;
}

EXPORT int32_t
rvn_allocate_more_space(
    int32_t map_after_allocation_flag,
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

    if (map_after_allocation_flag != 0)
    {
        int32_t mmap_flags = 0;
        if (mfh->flags & MMOPTIONS_COPY_ON_WRITE)
            mmap_flags |= MAP_PRIVATE;
        else
            mmap_flags |= MAP_SHARED;
        
        rc = rvn_mmap_file(new_length_after_adjustment, mfh->flags, handle, 0L, new_address, detailed_error_code);
        if (rc == FAIL_MMAP64)
            goto error_cleanup_without_errno;
    }
    return SUCCESS;

error_cleanup_without_errno:
    return rc;
}

EXPORT int32_t
rvn_unmap(int32_t flags, void *address, int64_t size, int32_t *detailed_error_code)
{    
    int32_t _;
    if (flags & MMOPTIONS_DELETE_ON_CLOSE)
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

EXPORT int32_t
rvn_remap(void *base_address, void **new_address, void *handle, int64_t size, int32_t flags, int64_t offset, int32_t *detailed_error_code)
{
    int32_t rc = rvn_unmap(MMOPTIONS_NONE, base_address, ALLOCATION_GRANULARITY, detailed_error_code);
    if (rc != SUCCESS)    
        return rc;

    int64_t sz = _nearest_size_to_page_size(size, ALLOCATION_GRANULARITY);
    rc = rvn_mmap_file(sz, flags, handle, offset, new_address, detailed_error_code);    
    return rc;
}


/* ADIADI:: The mfh is illegal (probably passing void ** instead of void*? why fail only on the pi itself? is happening from remap?) */
EXPORT int32_t
rvn_mmap_file(int64_t sz, int32_t flags, void *handle, int64_t offset, void **addr, int32_t *detailed_error_code)
{
    struct map_file_handle* mfh = (struct map_file_handle *)handle;
    int32_t mmap_flags = 0;
    if (flags & MMOPTIONS_COPY_ON_WRITE)
        mmap_flags |= MAP_PRIVATE;
    else
        mmap_flags |= MAP_SHARED;

    *addr = rvn_mmap(NULL, sz, PROT_READ | PROT_WRITE, mmap_flags, mfh->fd, offset);
    if (*addr == MAP_FAILED)
    {
        *detailed_error_code = errno;
        return FAIL_MMAP64;
    }

    return SUCCESS;
}

EXPORT int32_t
rvn_flush_file(void *handle, int32_t *detailed_error_code)
{
    struct map_file_handle* mfh = (struct map_file_handle *)handle;
    int32_t rc = _flush_file(mfh->fd);
    if (rc != SUCCESS)
        *detailed_error_code = errno;
    return rc;
}
