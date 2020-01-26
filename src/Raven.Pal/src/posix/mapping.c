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
    {
        *detailed_error_code = errno;
#ifdef PRINTF_DEBUG
        printf("rvn_file_sync() failed, error code = %d\n", *detailed_error_code);
#endif
    }

#ifdef PRINTF_DEBUG
        printf("rvn_file_sync(), rc = %d\n", rc);
#endif
    return rc;
}


PRIVATE void free_map_file_handle(struct map_file_handle *handle)
{
    if (handle->path != NULL)
    {
        #ifdef PRINTF_DEBUG
        printf("free_map_file_handle(), invoke free() on path = %s\n", handle->path);
        #endif

        free((void *)(handle->path));
        (handle)->path = NULL;
    }

    #ifdef PRINTF_DEBUG
    printf("free_map_file_handle(), invoke on fd = %d\n", handle->fd);
    #endif
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
#ifdef PRINTF_DEBUG
    printf("rvn_create_file(), path = %s\n", path);
#endif
    assert(path);
    assert(path[0] != '\0');
    assert(initial_file_size > 0);

    struct map_file_handle *mfh = calloc(1, sizeof(struct map_file_handle));
    *handle = mfh;
    if(mfh == NULL)
    {
        rc = FAIL_CALLOC;
#ifdef PRINTF_DEBUG
    printf("rvn_create_file(), path = %s, failed to allocate map_file_handle (FAIL_CALLOC)\n", path);
#endif
        goto error_clean_with_error;
    }

    char* dup_path = strdup(path);
    if (dup_path == NULL)
    {
        rc = FAIL_NOMEM;
#ifdef PRINTF_DEBUG
    printf("rvn_create_file(), path = %s, failed to duplicate path (FAIL_NOMEM)\n", path);
#endif
        goto error_clean_with_error;
    }

    char *directory = dirname(dup_path);
    rc = _ensure_path_exists(directory, detailed_error_code);
    free(dup_path);
    if (rc != SUCCESS)
    {
#ifdef PRINTF_DEBUG
    printf("rvn_create_file(), path = %s, _ensure_path_exists() failed, rc = %d\n", path, rc);
#endif
        goto error_clean_with_error;
    }

    mfh->path = strdup(path);
    if (mfh->path == NULL)
    {
        rc = FAIL_CALLOC;
#ifdef PRINTF_DEBUG
    printf("rvn_create_file(), path = %s, failed to duplicate mfh->path (FAIL_CALLOC)\n", path);
#endif
        goto error_clean_with_error;
    }
    mfh->flags = flags;
    int32_t largefile = 0;
     if (sizeof(void*) == 4) /* 32 bits */
     {
        largefile = O_LARGEFILE;
#ifdef PRINTF_DEBUG
    printf("rvn_create_file(), path = %s, 32-bit process detected, setting O_LARGEFILE flag on flags\n", path);
#endif
     }

    mfh->fd = open(path, O_RDWR | O_CREAT | largefile, S_IWUSR | S_IRUSR);
    if (mfh->fd == -1)
    {
        rc = FAIL_OPEN_FILE;
#ifdef PRINTF_DEBUG
    printf("rvn_create_file(), path = %s, failed to open file (FAIL_OPEN_FILE)\n", path);
#endif
        goto error_clean_with_error;
    }

    struct stat st;
    if (fstat(mfh->fd, &st) == -1)
    {
        rc = FAIL_STAT_FILE;
#ifdef PRINTF_DEBUG
    printf("rvn_create_file(), path = %s, fstat() failed (FAIL_STAT_FILE)\n", path);
#endif
        goto error_clean_with_error;
    }

    int64_t sz = st.st_size;

    int64_t sys_page_size = sysconf(_SC_PAGE_SIZE);
    if (sys_page_size == -1)
    {
        rc = FAIL_SYSCONF;
#ifdef PRINTF_DEBUG
    printf("rvn_create_file(), path = %s, sysconf(_SC_PAGE_SIZE) failed (FAIL_SYSCONF)\n", path);
#endif
        goto error_clean_with_error;
    }

    if (initial_file_size < ALLOCATION_GRANULARITY)
        initial_file_size = ALLOCATION_GRANULARITY;

    if (sz <= initial_file_size || sz % (ALLOCATION_GRANULARITY) != 0)
    {
        sz = _nearest_size_to_page_size(rvn_max(initial_file_size, sz), ALLOCATION_GRANULARITY);
    }
#ifdef PRINTF_DEBUG
    printf("rvn_create_file(), path = %s, calculated desired size for the file is %lld\n", path, sz);
#endif

    if(sz != st.st_size)
    {        
#ifdef PRINTF_DEBUG
    printf("rvn_create_file(), path = %s, desired size is not its actual size (%lld != %ld), will attempt to resize the file\n", path, sz, st.st_size);
#endif

        rc = _allocate_file_space(mfh->fd, sz, detailed_error_code);
        if (rc != SUCCESS)
        {
#ifdef PRINTF_DEBUG
    printf("rvn_create_file(), path = %s, _allocate_file_space() failed, rc = %d\n", path, rc);
#endif

            goto error_clean;
        }
        if (_sync_directory_allowed(mfh->fd) == SYNC_DIR_ALLOWED)
        {
            rc = _sync_directory_for(path, detailed_error_code);
            if (rc != SUCCESS)
            {
#ifdef PRINTF_DEBUG
    printf("rvn_create_file(), path = %s, _sync_directory_for() failed, rc = %d\n", path, rc);
#endif
                goto error_clean;
            }
        }
    }

    *actual_file_size = sz;
#ifdef PRINTF_DEBUG
    printf("rvn_create_file(), path = %s, finished successfully, actual_file_size = %lld\n", path, sz);
#endif

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
    {
#ifdef PRINTF_DEBUG
    printf("rvn_allocate_more_space(), path = %s, _allocate_file_space() failed, detailed_error_code = %d\n", mfh->path, *detailed_error_code);
#endif
        return rc;
    }

    if (_sync_directory_allowed(mfh->fd) == SYNC_DIR_ALLOWED)
    {
        rc = _sync_directory_for(mfh->path, detailed_error_code);
        if (rc != SUCCESS)
        {
#ifdef PRINTF_DEBUG
    printf("rvn_allocate_more_space(), path = %s, _sync_directory_for() failed, detailed_error_code = %d\n", mfh->path, *detailed_error_code);
#endif

            return rc;
        }
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
        {
#ifdef PRINTF_DEBUG
    printf("rvn_allocate_more_space(), path = %s, rvn_mmap_file() failed, detailed_error_code = %d\n", mfh->path, *detailed_error_code);
#endif
            goto error_cleanup_without_errno;
        }
    }
#ifdef PRINTF_DEBUG
    printf("rvn_allocate_more_space(), path = %s, finished successfully.", mfh->path);
#endif

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
    {
        *detailed_error_code = errno;
#ifdef PRINTF_DEBUG
    printf("rvn_unmap(), address = %p, size = %lld, execution failed, detailed_error_code = %d\n", address, size, errno);
#endif
    }
    return rc;
}

EXPORT int32_t
rvn_mmap_dispose_handle(void *handle, int32_t *detailed_error_code, bool shouldFree)
{
    struct map_file_handle *mfh = (struct map_file_handle *)handle;
    int32_t rc = SUCCESS;
    
    if (mfh->fd == -1)
    {
#ifdef PRINTF_DEBUG
    printf("rvn_mmap_dispose_handle() failed, passed an invalid handle (FAIL_INVALID_HANDLE)\n");
#endif
        rc = FAIL_INVALID_HANDLE;
        goto error_cleanup;
    }

    if (mfh->flags & MMOPTIONS_DELETE_ON_CLOSE)
    {
        int32_t unlink_rc = unlink(mfh->path);
        if (unlink_rc != 0)
        {
            /* record the error and continue to close */
#ifdef PRINTF_DEBUG
    printf("rvn_mmap_dispose_handle(), path = %s, unlink() failed, detailed_error_code = %d (FAIL_UNLINK)\n", mfh->path, errno);
#endif

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
    if(shouldFree == true)
    {
        free_map_file_handle(mfh);
    }
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
#ifdef PRINTF_DEBUG
    struct map_file_handle *mfh = (struct map_file_handle *)handle;
    printf("rvn_remap() started, path = %s, starting execution\n", mfh->path);
#endif

    int32_t rc = rvn_unmap(MMOPTIONS_NONE, base_address, ALLOCATION_GRANULARITY, detailed_error_code);
    if (rc != SUCCESS)
    {
#ifdef PRINTF_DEBUG
    printf("rvn_remap(), path = %s, rvn_unmap() failed, rc = %d\n", mfh->path, rc);
#endif
        return rc;
    }

    int64_t sz = _nearest_size_to_page_size(size, ALLOCATION_GRANULARITY);
    rc = rvn_mmap_file(sz, flags, handle, offset, new_address, detailed_error_code);
    if(rc != SUCCESS)
    {
#ifdef PRINTF_DEBUG
    printf("rvn_remap(), path = %s, rvn_mmap_file() failed, rc = %d\n", mfh->path, rc);
#endif       
    }

    return rc;
}

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
#ifdef PRINTF_DEBUG
    printf("rvn_mmap_file(), path = %s, rvn_mmap() failed, detailed_error_code = %d\n", mfh->path, errno);
#endif

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
    {
#ifdef PRINTF_DEBUG
    printf("rvn_flush_file(), path = %s, _flush_file() failed, detailed_error_code = %d\n", mfh->path, errno);
#endif
        *detailed_error_code = errno;
    }
    return rc;
}
