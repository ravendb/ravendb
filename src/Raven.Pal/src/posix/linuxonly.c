#if defined(__unix__) && !defined(__APPLE__)

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <sys/utsname.h>
#include <unistd.h>
#include <sys/statfs.h>
#include <linux/magic.h>
#include <sys/syscall.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <libgen.h>
#include <unistd.h>
#include <linux/fs.h>
#include <linux/fiemap.h>
#include <sys/ioctl.h>

#include "rvn.h"
#include "rvn_internal.h"
#include "status_codes.h"
#include "internal_posix.h"

EXPORT uint64_t
rvn_get_current_thread_id(void)
{
    return syscall(SYS_gettid);
}

PRIVATE int32_t
_flush_file(int32_t fd)
{
    return fsync(fd);
}

#define SMB2_MAGIC_NUMBER 0xfe534d42

PRIVATE int32_t
_sync_directory_allowed(int32_t dir_fd)
{
    struct statfs buf;
    if (fstatfs(dir_fd, &buf) == -1)
        return FAIL;

    switch (buf.f_type)
    {
    case NFS_SUPER_MAGIC:
    case CIFS_MAGIC_NUMBER:
    case SMB_SUPER_MAGIC:
    case SMB2_MAGIC_NUMBER:
        return SYNC_DIR_NOT_ALLOWED;
    default:
        return SYNC_DIR_ALLOWED;
    }
}

PRIVATE int32_t
_finish_open_file_with_odirect(int32_t fd)
{
    /* nothing to do in posix, O_DIRECT is supported */
    return 0;
}

PRIVATE int32_t
_rvn_fallocate(int32_t fd, int64_t offset, int64_t size)
{
    return posix_fallocate64(fd, offset, size);
}

PRIVATE char *
_get_strerror_r(int32_t error, char *tmp_buff, int32_t buf_size)
{
    return strerror_r(error, tmp_buff, buf_size);
}

EXPORT int32_t
rvn_test_storage_durability(
    const char *temp_file_name,
    int32_t *detailed_error_code)
{
    int fd = open(temp_file_name, O_WRONLY | O_DSYNC | O_DIRECT | O_CREAT, S_IWUSR | S_IRUSR);
    int rc = SUCCESS;
    if (fd == -1)
    {
        rc = FAIL_OPEN_FILE;
        goto error_cleanup;
    }

    rc = _allocate_file_space(fd, 64 * 1024, detailed_error_code);
    if (rc != SUCCESS)
    {
        rc = FAIL_ALLOC_FILE;
        if (errno == EINVAL)
            rc = FAIL_TEST_DURABILITY;
        goto error_cleanup;
    }

error_cleanup:
    if (rc != SUCCESS)
        *detailed_error_code = errno;
    if (fd != -1)
    {
        close(fd);
        unlink(temp_file_name);
    }

    return rc;
}

PRIVATE
int32_t rvn_write_vectored_file_io(
    void *handle,
    struct page_to_write *buffers,
    int32_t count,
    int32_t *detailed_error_code)
{
    struct iovec iovs[IOV_MAX];
    int used = 0;

    struct handle *handle_ptr = handle;
    for (int32_t curIdx = 0; curIdx < count; curIdx++)
    {
        int64_t offset = buffers[curIdx].page_num * VORON_PAGE_SIZE;
        int64_t size = (int64_t)buffers[curIdx].count_of_pages * VORON_PAGE_SIZE;
        int64_t after = offset + size;
        iovs[0].iov_base = buffers[curIdx].ptr;
        iovs[0].iov_len = size;
        used = 1;

        for (size_t nextIndex = curIdx + 1; nextIndex < count && used < IOV_MAX; nextIndex++)
        {
            int64_t dest = buffers[nextIndex].page_num * VORON_PAGE_SIZE;
            if (after != dest)
                break;

            size = (int64_t)buffers[nextIndex].count_of_pages * VORON_PAGE_SIZE;
            after = dest + size;
            iovs[used].iov_base = buffers[nextIndex].ptr;
            iovs[used].iov_len = size;
            used++;
            curIdx++;
        }

        int32_t rc = _pwritev(handle_ptr->file_fd, iovs, used, offset, detailed_error_code);
        if (rc != SUCCESS)
            return rc;
    }
    _mark_dirty_pages(handle, buffers, count);
    return SUCCESS;
}

PRIVATE int32_t
_writeback_supported(struct handle *handle_ptr)
{
    (void)handle_ptr;
    return true;
}

PRIVATE int32_t
_writeback_range_start(struct handle *handle_ptr, int64_t offset, int64_t length, int32_t *detailed_error_code)
{
    int rc;
    do
    {
        rc = sync_file_range(handle_ptr->file_fd, offset, length, SYNC_FILE_RANGE_WRITE);
    } while (rc != 0 && errno == EINTR);
    if (rc != 0)
    {
        *detailed_error_code = errno;
        return FAIL_SYNC_FILE;
    }
    return SUCCESS;
}

PRIVATE int32_t
_writeback_range_complete(struct handle *handle_ptr, int64_t offset, int64_t length, int32_t *detailed_error_code)
{
    int rc;
    do
    {
        rc = sync_file_range(handle_ptr->file_fd, offset, length,
                             SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER);
    } while (rc != 0 && errno == EINTR);
    if (rc != 0)
    {
        *detailed_error_code = errno;
        return FAIL_SYNC_FILE;
    }
    return SUCCESS;
}

EXPORT
rvn_writer rvn_get_writer(void *handle)
{
    struct handle *handle_ptr = handle;

    if (handle_ptr->write_address)
        return rvn_write_mmap;

    switch (g_cfg.write_mode)
    {
    case rvn_write_mode_vectored_file_io:
        return rvn_write_vectored_file_io;
    case rvn_write_mode_file_io:
        return rvn_write_file_io;
    case rvn_write_mode_io_ring:
        return rvn_write_io_ring;
    case rvn_write_mode_mmap:
        if (handle_ptr->global_state->open_flags & OPEN_FILE_DO_NOT_MAP)
            return rvn_write_mmap32;
        return rvn_write_invalid_setup;
    default:
        return io_ring_setup_successful() ? rvn_write_io_ring : rvn_write_vectored_file_io;
    }
}

EXPORT int32_t
rvn_write_journal(void *handle, void *context, struct journal_entry *buffer, int64_t count_of_entries, int64_t offset, int32_t *detailed_error_code)
{
    (void)context; // context is unused in posix
    struct journal_handle *jfh = handle;
    struct iovec elements[IOV_MAX];
    int32_t index = 0;
    int64_t bytes_in_batch = 0;
    for (size_t i = 0; i < count_of_entries; i++)
    {
        elements[index].iov_base = buffer[i].base;
        elements[index].iov_len = buffer[i].number_of_4kbs * SYS_PAGE_SIZE;
        if (elements[index].iov_len / SYS_PAGE_SIZE != buffer[i].number_of_4kbs)
        {
            *detailed_error_code = EOVERFLOW;
            return FAIL_MATH_OVERFLOW;
        }
        bytes_in_batch += (int64_t)elements[index].iov_len;
        index++;
        if (index == IOV_MAX)
        {
            int32_t rc = _pwritev(jfh->fd, elements, index, offset, detailed_error_code);
            if (rc != SUCCESS)
                return rc;
            offset += bytes_in_batch;
            bytes_in_batch = 0;
            index = 0;
        }
    }
    if (index > 0)
    {
        int32_t rc = _pwritev(jfh->fd, elements, index, offset, detailed_error_code);
        if (rc != SUCCESS)
            return rc;
    }
    return SUCCESS;
}


EXPORT int32_t
rvn_sync_directories(void* handle, char** folders, int32_t count, int32_t *detailed_error_code)
{
    if(io_ring_setup_successful())
        return rvn_sync_directories_ioring(handle, folders, count, detailed_error_code);
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
    
    // Get file size
    struct stat st;
    if (fstat(fd, &st) == -1)
    {
        *detailed_error_code = errno;
        return FAIL_STAT_FILE;
    }
    
    int64_t file_size = st.st_size;
    if (offset >= file_size)
    {
        *start = -1;
        *size = -1;
        return SUCCESS;
    }
    
    // Allocate space for FIEMAP request (1 extent at a time for simplicity)
    char buf[sizeof(struct fiemap) + sizeof(struct fiemap_extent)];
    struct fiemap *fiemap = (struct fiemap *)buf;
    
    int64_t current_pos = offset;
    int64_t hole_start = -1;
    
    while (current_pos < file_size)
    {
        memset(fiemap, 0, sizeof(buf));
        fiemap->fm_start = current_pos;
        fiemap->fm_length = file_size - current_pos;
        fiemap->fm_extent_count = 1;
        fiemap->fm_flags = 0;
        
        if (ioctl(fd, FS_IOC_FIEMAP, fiemap) == -1)
        {
            if (errno == ENOTTY || errno == EOPNOTSUPP)
            {
                *start = -1;
                *size = -1;
                return SUCCESS;
            }
            *detailed_error_code = errno;
            return FAIL_SEEK_FILE;
        }
        
        if (fiemap->fm_mapped_extents == 0)
        {
            // No more extents from current_pos to EOF - rest is a hole
            if (hole_start == -1)
                hole_start = current_pos;
            
            *start = hole_start;
            *size = file_size - hole_start;
            return SUCCESS;
        }
        
        struct fiemap_extent *extent = &fiemap->fm_extents[0];
        if (extent->fe_length == 0)
        {
            // found zero length range, let's skip that and keep searching...
            current_pos = rvn_max(current_pos + 1, extent->fe_logical + 1);
            continue;
        }
        int64_t extent_start = extent->fe_logical;
        int64_t extent_end = extent_start + extent->fe_length;
        
        // Check if there's a gap (true hole) before this extent
        if (extent_start > current_pos)
        {
            if (hole_start == -1)
                hole_start = current_pos;
            
            *start = hole_start;
            *size = extent_start - hole_start;
            return SUCCESS;
        }
        
        // Check if this extent is unwritten (preallocated)
        if (extent->fe_flags & FIEMAP_EXTENT_UNWRITTEN)
        {
            // This is an unwritten extent - end of file, but not a
            // sparse region we care about
            current_pos = extent_end;
            continue;
        }
        
        // This is a data extent, move past it
        hole_start = -1; // Reset hole tracking
        current_pos = extent_end;
        
        // Check if this was the last extent
        if (extent->fe_flags & FIEMAP_EXTENT_LAST)
        {
            // Check if there's space after the last extent
            if (current_pos < file_size)
            {
                *start = current_pos;
                *size = file_size - current_pos;
                return SUCCESS;
            }
            break;
        }
    }
    
    // No holes found
    *start = -1;
    *size = -1;
    return SUCCESS;
}

#endif