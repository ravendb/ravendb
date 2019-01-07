#if defined(__unix__) || defined(__APPLE__)

#ifdef __APPLE__
#define MMAP mmap
#else
#define MMAP mmap64
#endif

#define _GNU_SOURCE
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

int32_t
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

    /* TODO: rnv_ensure_file_exists(path); */

    int32_t fd = open(path, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);

    *handle = NULL;

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

    if (sz <= initial_file_size || sz % sys_page_size != 0)
    {
        sz = rvn_nearest_size_to_page_size(max(initial_file_size, sz), sys_page_size);
    }

    rc = rvn_allocate_file_space(fd, sz, detailed_error_code);
    if (rc != SUCCESS)
        goto error_cleanup;

    *actual_file_size = sz;

    if (rvn_sync_directory_allowed(fd))
    {
        rc = rvn_sync_directory_for(path, detailed_error_code);
        if (rc != SUCCESS)
            goto error_cleanup;
    }

    int32_t mmap_flags = 0;
    if (flags & MMOPTIONS_COPY_ON_WRITE)
        mmap_flags |= MAP_PRIVATE;
    else
        mmap_flags |= MAP_SHARED;

    void *address = MMAP(NULL, sz, PROT_READ | PROT_WRITE, mmap_flags, fd, 0L);

    if (address == MAP_FAILED)
    {
        rc = FAIL_MMAP64;
        goto error_cleanup;
    }

    *base_addr = address;
    *((int32_t *)handle) = fd;

    return rc; /* SUCCESS */

error_cleanup:
    *detailed_error_code = errno;
    if (fd != -1)
        close(fd);

    return rc;
}

int32_t
rvn_allocate_more_space(int64_t new_length, int64_t total_allocation_size, const char *filename, void *handle, int32_t flags,
                        void **new_address, int64_t *new_length_after_adjustment, int32_t *detailed_error_code)
{
    int32_t fd = rvn_pointer_to_int(handle);
    int32_t rc = SUCCESS;

    int64_t sys_page_size = sysconf(_SC_PAGE_SIZE);
    if (sys_page_size == -1)
    {
        rc = FAIL_SYSCONF;
        goto error_cleanup;
    }

    *new_length_after_adjustment = rvn_nearest_size_to_page_size(new_length, sys_page_size);

    if (*new_length_after_adjustment <= total_allocation_size)
        return FAIL_ALLOCATION_NO_RESIZE;

    int64_t allocation_size = *new_length_after_adjustment - total_allocation_size;

    /* TODO :: ADIADI : verify why there's an offset in fallocate and is it better to use it? */
    rc = rvn_allocate_file_space(fd, total_allocation_size + allocation_size, detailed_error_code);
    if (rc != SUCCESS)
        goto error_cleanup;

    if (rvn_sync_directory_allowed(fd))
    {
        rc = rvn_sync_directory_for(filename, detailed_error_code);
        if (rc != SUCCESS)
            goto error_cleanup;
    }

    int32_t mmap_flags = 0;
    if (flags & MMOPTIONS_COPY_ON_WRITE)
        mmap_flags |= MAP_PRIVATE;
    else
        mmap_flags |= MAP_SHARED;

    void *address = MMAP(NULL, total_allocation_size + allocation_size, PROT_READ | PROT_WRITE, mmap_flags, fd, 0L);
    if (address == MAP_FAILED)
    {
        rc = FAIL_MMAP64;
        goto error_cleanup;
    }

    *new_address = address;

    return SUCCESS;

error_cleanup:
    *detailed_error_code = errno;
    if (fd != -1)
        close(fd);

    return rc;
}

int32_t
rvn_unmap(void *address, int64_t size, int32_t delete_on_close, int32_t *unmap_error_code, int32_t *madvise_error_code)
{
    int32_t rc = SUCCESS;
    if (delete_on_close == true)
    {
        rc |= madvise(address, size, MADV_DONTNEED);
        *madvise_error_code = errno;
    }

    rc |= munmap(address, size);
    *unmap_error_code = errno;

    return rc;
}

#endif