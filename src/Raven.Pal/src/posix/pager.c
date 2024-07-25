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
#include <string.h>

#include "rvn.h"
#include "status_codes.h"
#include "internal_posix.h"

struct handle
{
    char *file_path;
    void *base_address;
    uint64_t allocation_size;
    int file_fd;
    int32_t open_flags;
};

int32_t rvn_lock_memory(int32_t open_flags, void *mem, int64_t size, int32_t *detailed_error_code)
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

    if (mlock(mem, size) ||
        open_flags & OPEN_FILE_DO_NOT_CONSIDER_MEMORY_LOCK_FAILURE_AS_CATASTROPHIC_ERROR)
    {
        return SUCCESS;
    }
    rc = FAIL_LOCK_MEMORY;
Exit:
    *detailed_error_code = errno;
    return rc;
}

int32_t _open_pager_file(int fd,
                         char *owned_file_path,
                         int32_t open_flags,
                         int64_t req_file_size,
                         void **handle,
                         void **memory,
                         int64_t *memory_size,
                         int32_t *detailed_error_code)
{
    int32_t rc = SUCCESS;
    struct handle *handle_ptr = NULL;
    void *mem = NULL;

    handle_ptr = calloc(1, sizeof(struct handle));
    if (handle_ptr == NULL)
    {
        rc = FAIL_NOMEM;
        goto Error;
    }

    int64_t min_file_size = rvn_max(
        (req_file_size + ALLOCATION_GRANULARITY - 1) & ~(ALLOCATION_GRANULARITY - 1),
        ALLOCATION_GRANULARITY);

    struct stat st;
    if (fstat(fd, &st) == -1)
    {
        rc = FAIL_STAT_FILE;
        goto Error;
    }

    if (min_file_size > st.st_size && !(open_flags & OPEN_FILE_READ_ONLY))
    {
        st.st_size = min_file_size;
        rc = _allocate_file_space(fd, st.st_size, detailed_error_code);
        if (rc != SUCCESS)
            goto Error;
        if (_sync_directory_allowed(fd) == SYNC_DIR_ALLOWED)
        {
            rc = _sync_directory_for(owned_file_path, detailed_error_code);
            if (rc != SUCCESS)
                goto Error;
        }
    }
    else if(st.st_size == 0 && (open_flags & OPEN_FILE_READ_ONLY))
    {
        // we allow opening zero len files with read only mode, but don't try to map them
        handle_ptr->file_fd = fd;
        handle_ptr->open_flags = open_flags | OPEN_FILE_DO_NOT_MAP;
        *memory_size = 0;
        *handle = handle_ptr;
        return SUCCESS;
    }

    if ((open_flags & OPEN_FILE_DO_NOT_MAP))
    {
        handle_ptr->file_fd = fd;
        handle_ptr->open_flags = open_flags;
        *memory_size = st.st_size;
        *handle = handle_ptr;
        return SUCCESS;
    }

    int32_t mmap_flags = (open_flags & OPEN_FILE_COPY_ON_WRITE) ? MAP_PRIVATE : MAP_SHARED;
    int32_t prot = (open_flags & OPEN_FILE_WRITABLE_MAP) ? PROT_READ | PROT_WRITE : PROT_READ;
    mem = rvn_mmap(NULL, st.st_size, prot, mmap_flags, fd, 0L);
    if (mem == NULL)
    {
        rc = FAIL_MAP_VIEW_OF_FILE;
        goto Error;
    }

    if (open_flags & OPEN_FILE_LOCK_MEMORY &&
        !rvn_lock_memory(open_flags, mem, st.st_size, detailed_error_code))
    {
        rc = FAIL_LOCK_MEMORY;
        goto Error;
    }

    handle_ptr->file_fd = fd;
    handle_ptr->base_address = mem;
    handle_ptr->allocation_size = st.st_size;
    handle_ptr->open_flags = open_flags;
    handle_ptr->file_path = owned_file_path;
    *handle = handle_ptr;
    *memory = mem;
    *memory_size = st.st_size;
    return SUCCESS;

Error:
    *detailed_error_code = errno;
    if (mem != NULL)
    {
        munmap(mem, st.st_size);
    }
    close(fd);
    free(owned_file_path);
    free(handle_ptr);
    return rc;
}

EXPORT int32_t
rvn_init_pager(const char *filename,
               int64_t initial_file_size,
               int32_t open_flags,
               void **handle,
               void **memory,
               int64_t *memory_size,
               int32_t *detailed_error_code)
{
    *memory_size = 0;
    *memory = NULL;
    *handle = NULL;

    char *owned_file_path = NULL;

    assert(filename);
    assert(filename[0] != '\0');

    int32_t rc = SUCCESS;
    char *dup_path = strdup(filename);
    if (dup_path == NULL)
    {
        rc = FAIL_NOMEM;
        goto Error;
    }
    char *directory = dirname(dup_path);
    rc = _ensure_path_exists(directory, detailed_error_code);
    free(dup_path);
    if (rc != SUCCESS)
        goto Error;

    owned_file_path = strdup(filename);

    int flags = ((open_flags & OPEN_FILE_READ_ONLY) | (open_flags & OPEN_FILE_COPY_ON_WRITE)) ? O_RDONLY : O_RDWR | O_CREAT;
    int fd = open(filename, flags, S_IWUSR | S_IRUSR);
    if (fd == -1)
    {
        rc = FAIL_OPEN_FILE;
    }

    return _open_pager_file(fd, owned_file_path, open_flags, initial_file_size, handle, memory, memory_size, detailed_error_code);

Error:
    *detailed_error_code = errno;
    free(owned_file_path);
    return rc;
}

int32_t
rvn_increase_pager_size(void *handle,
                        int64_t new_length,
                        void **new_handle,
                        void **memory,
                        int64_t *memory_size,
                        int32_t *detailed_error_code)
{
    struct handle *handle_ptr = handle;
    *memory = NULL;
    *memory_size = 0;

    char *owned_file_path = strdup(handle_ptr->file_path);
    if (owned_file_path == NULL)
    {
        *detailed_error_code = 0;
        return FAIL_NOMEM;
    }

    int new_fd = dup(handle_ptr->file_fd);
    if (new_fd == -1)
    {
        *new_handle = NULL;
        *detailed_error_code = errno;
        free(owned_file_path);
        return FAIL_DUPLICATE_HANDLE;
    }
    int32_t rc = _open_pager_file(new_fd, owned_file_path, handle_ptr->open_flags, new_length, new_handle, memory, memory_size, detailed_error_code);
    return rc;
}

EXPORT int32_t
rvn_close_pager(
    void *handle,
    int32_t *detailed_error_code)
{
    if(handle == NULL)
    {
        return FAIL_INVALID_HANDLE;
    }
    struct handle *handle_ptr = handle;
    *detailed_error_code = 0;
    int rc = SUCCESS;
    if (!(handle_ptr->open_flags & OPEN_FILE_DO_NOT_MAP))
    {
        if (munmap(handle_ptr->base_address, handle_ptr->allocation_size))
        {
            *detailed_error_code = errno;
            rc = FAIL_MAP_VIEW_OF_FILE;
        }
    }
  
    if (close(handle_ptr->file_fd))
    {
        if (*detailed_error_code == 0)
            *detailed_error_code = errno;
        if (rc == SUCCESS)
            rc = FAIL_CLOSE;
    }
    free(handle_ptr->file_path);
    free(handle_ptr);
    return rc;
}

EXPORT int32_t
rvn_sync_pager(void *handle,
               int32_t *detailed_error_code)
{
    struct handle *handle_ptr = handle;
    if (_flush_file(handle_ptr->file_fd))
    {
        *detailed_error_code = errno;
        return FAIL_SYNC_FILE;
    }
    return SUCCESS;
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


EXPORT
int32_t rvn_unmap_memory(
    void *mem,
    int64_t size,
    int32_t *detailed_error_code)
{
    *detailed_error_code = 0;
    if (munmap(mem, size))
    {
        *detailed_error_code = errno;
        return FAIL_UNMAP_VIEW_OF_FILE;
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
    
    int32_t mmap_flags = (handle_ptr->open_flags & OPEN_FILE_COPY_ON_WRITE) ? MAP_PRIVATE : MAP_SHARED;
    int32_t prot = (handle_ptr->open_flags & OPEN_FILE_WRITABLE_MAP) ? PROT_READ | PROT_WRITE : PROT_READ;
    *mem = rvn_mmap(NULL, size, prot, mmap_flags, handle_ptr->file_fd, offset);
    if (*mem == NULL)
    {
        rc = FAIL_MAP_VIEW_OF_FILE;
        goto Error;
    }

    if (handle_ptr->open_flags & OPEN_FILE_LOCK_MEMORY)
    {
        // intentionally returning the error code & rc from the lock_memory call
        rc = rvn_lock_memory(handle_ptr->open_flags, *mem, size, detailed_error_code);
        if (rc != SUCCESS)
        {
            goto Error;
        }
    }

    return SUCCESS;

Error:
    *detailed_error_code = errno;
    if(*mem)
    {
        munmap(*mem, size);
        *mem = NULL;
    }
    return rc;
}