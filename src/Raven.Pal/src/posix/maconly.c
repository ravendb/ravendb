#if !defined(__unix__) || defined(__APPLE__)

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <sys/param.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <pthread.h>
#include "rvn.h"
#include "rvn_internal.h"
#include "internal_posix.h"
#include "status_codes.h"

bool _io_ring_supported()
{
    return false;
}
int32_t _setup_io_ring(struct handle_global_state *global_state, int32_t *detailed_error_code)
{
    *detailed_error_code = ENOTSUP;
    return FAIL_CREATE_IO_RING;
}

void _close_io_ring(struct handle_global_state *global_state)
{
}

EXPORT uint64_t
rvn_get_current_thread_id()
{
    uint64_t id;
    pthread_threadid_np(NULL, &id);

    return id;
}

PRIVATE int32_t
_flush_file(int32_t fd)
{
    return fcntl(fd, F_FULLFSYNC);
}

PRIVATE int32_t
_sync_directory_allowed(int32_t dir_fd)
{
    return 1;
}

PRIVATE int32_t
_finish_open_file_with_odirect(int32_t fd)
{
    /* mac doesn't support O_DIRECT, we fcntl instead: */
    return fcntl(fd, F_NOCACHE, 1);
}

PRIVATE int32_t
_rvn_fallocate(int32_t fd, int64_t offset, int64_t size)
{
    /* mac doesn't support fallocate */
    return EINVAL;
}

PRIVATE char *
_get_strerror_r(int32_t error, char *tmp_buff, int32_t buf_size)
{
    int32_t non_gnu_compliant_rc = strerror_r(error, tmp_buff, buf_size);
    if (non_gnu_compliant_rc != 0)
        return tmp_buff;
    return NULL;
}

EXPORT int32_t
rvn_test_storage_durability(
    const char *temp_file_name,
    int32_t *detailed_error_code)
{
    *detailed_error_code = 0;
    return SUCCESS; /* windows and mac are always true */
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

int32_t
rvn_one_time_init(int32_t *detailed_error_code)
{
    return SUCCESS;
}

int io_ring_setup_successful(void)
{
    return 0;
}

EXPORT
rvn_writer rvn_get_writer(void *handle)
{
    struct handle *handle_ptr = handle;
    if (handle_ptr->write_address)
        return rvn_write_mmap;
    return rvn_write_file_io;
}

EXPORT int32_t
rvn_write_journal(void *handle, struct journal_entry *buffer, int64_t count_of_entries, int64_t offset, int32_t *detailed_error_code)
{
    struct journal_handle *jfh = (struct journal_handle *)handle;
    for (size_t i = 0; i < count_of_entries; i++)
    {
        int32_t size = buffer[i].number_of_4kbs * SYS_PAGE_SIZE;
        if (size / SYS_PAGE_SIZE != buffer[i].number_of_4kbs)
        {
            *detailed_error_code = EOVERFLOW;
            return FAIL_MATH_OVERFLOW;
        }
        int32_t rc = _pwrite(jfh->fd, buffer[i].base, size, offset, detailed_error_code);
        if (rc != SUCCESS)
            return rc;
        offset += size;
    }

    return SUCCESS;
}

EXPORT int32_t
rvn_sync_directories(void* handle, char** folders, int32_t count, int32_t *detailed_error_code)
{
    return rvn_sync_directories_sync(handle, folders, count, detailed_error_code);
}

EXPORT int32_t
rvn_pager_get_next_sparse_region(void* handle,
    int64_t offset,
    int64_t* start,
    int64_t* size,
    int32_t* detailed_error_code)
{
    struct handle *handle_ptr = handle;
    if (handle_ptr->global_state->status_flags & PAGER_STATUS_SPARSE_NOT_SUPPORTED)
    {
        *start = -1;
        *size = -1;
        return SUCCESS;
    }
    int fd = handle_ptr->file_fd;
    // macOS: use F_LOG2PHYS_EXT to find holes
    struct stat st;
    if (fstat(fd, &st) == -1)
    {
        *detailed_error_code = errno;
        return FAIL_STAT_FILE;
    }

    off_t current = offset;
    off_t file_size = st.st_size;

    // Search for the start of a hole
    while (current < file_size)
    {
        struct log2phys l2p = {0};
        l2p.l2p_contigbytes = file_size - current;
        l2p.l2p_devoffset = current;

        if (fcntl(fd, F_LOG2PHYS_EXT, &l2p) == -1)
        {
            if (errno == ENOTSUP)
            {
                *start = -1;
                *size = -1;
                return SUCCESS;
            }
            *detailed_error_code = errno;
            return FAIL_SEEK_FILE;
        }

        // l2p_devoffset == -1 means the region is a hole (not allocated)
        if (l2p.l2p_devoffset == -1)
        {
            // Found start of a hole, now find its extent
            off_t hole_start = current;
            off_t hole_end = current + l2p.l2p_contigbytes;

            // Clamp to file size
            if (hole_end > file_size)
                hole_end = file_size;

            *start = hole_start;
            *size = hole_end - hole_start;
            return SUCCESS;
        }

        // Skip over data region
        current += l2p.l2p_contigbytes;
        if (l2p.l2p_contigbytes == 0)
            current++; // Avoid infinite loop
    }

    // No hole found
    *start = -1;
    *size = -1;
    return SUCCESS;
}

#endif
