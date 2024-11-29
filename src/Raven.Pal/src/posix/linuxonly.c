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

#include "rvn.h"
#include "rvn_internal.h"
#include "status_codes.h"
#include "internal_posix.h"

#define IO_RING_SIZE (256)


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

PRIVATE char*
_get_strerror_r(int32_t error, char* tmp_buff, int32_t buf_size)
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
    if ( rc != SUCCESS )
    {
        rc = FAIL_ALLOC_FILE;
        if ( errno == EINVAL)
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

bool _io_ring_supported()
{
  if(sizeof(void*) != 8)
    return false;

   struct utsname buffer; 
   if (uname(&buffer) != 0)
        return false;

  int curr_major = 0, curr_minor = 0, curr_patch = 0;
  sscanf(buffer.release, "%d.%d.%d", &curr_major, &curr_minor, &curr_patch); 
  
  return curr_major > 5 || (curr_major==5 && curr_minor>=1);
}

int32_t _setup_io_ring(struct handle_global_state *global_state, int32_t *detailed_error_code)
{
    int rc = io_uring_queue_init(IO_RING_SIZE, &global_state->ring, 0);
    if(rc)
    {
        *detailed_error_code = -rc;
        return FAIL_CREATE_IO_RING;
    }
    return SUCCESS;
}

void _close_io_ring(struct handle_global_state *global_state)
{
    io_uring_queue_exit(&global_state->ring);
}


int32_t _submit_and_wait(
    struct io_uring* ring,
    int32_t count,
    int32_t* detailed_error_code)
{
    int32_t rc = io_uring_submit_and_wait(ring, count);
    if(rc < 0)
    {
        *detailed_error_code = -rc;
        return FAIL_IO_RING_SUBMIT;
    }

    struct io_uring_cqe* cqe;
    for(int i = 0; i < count; i++)
    {
        rc = io_uring_wait_cqe(ring, &cqe);
        if (rc < 0)
        {
            *detailed_error_code = -rc;
            return FAIL_IO_RING_NO_RESULT;
        }
        if(cqe->res < 0)
        {
            *detailed_error_code = -cqe->res;
            return FAIL_IO_RING_WRITE_RESULT;
        }
        io_uring_cqe_seen(ring, cqe);
    }
    return SUCCESS;
}

int32_t rvn_write_io_ring(
    void *handle,
    int32_t count,
    struct page_to_write *buffers,
    int32_t *detailed_error_code)
{
    int32_t rc = SUCCESS;
    struct handle *handle_ptr = handle;
    int32_t submitted = 0;
    for (size_t i = 0; i < count; i++)
    {
        struct io_uring_sqe *sqe = io_uring_get_sqe(&handle_ptr->global_state->ring);
        if (sqe == NULL)
        {
            rc = _submit_and_wait(&handle_ptr->global_state->ring, submitted, detailed_error_code);
            if (rc != SUCCESS)
            {
                return rc;
            }
            submitted = 0;
            sqe = io_uring_get_sqe(&handle_ptr->global_state->ring);
            if (sqe == NULL)
            {
                // *after* we submitted, we have no entry? No recovery from this...
                *detailed_error_code = ENOENT;
                return FAIL_IO_RING_WRITE;
            }
        }
        io_uring_prep_write(sqe,
            handle_ptr->file_fd, 
            buffers[i].ptr, 
            buffers[i].count_of_pages * VORON_PAGE_SIZE, 
            buffers[i].page_num * VORON_PAGE_SIZE
        );

        submitted++;
    }

    return _submit_and_wait(&handle_ptr->global_state->ring, submitted, detailed_error_code);
}


PRIVATE
int32_t rvn_write_vectored_file_io(
    void* handle,
    int32_t count,
    struct page_to_write *buffers,
    int32_t *detailed_error_code
)
{
    struct iovec iovs[IOV_MAX];
    int used = 0;

    struct handle *handle_ptr = handle;
    for(int32_t curIdx = 0; curIdx < count; curIdx++)
    {
        int64_t offset = buffers[curIdx].page_num * VORON_PAGE_SIZE;
        int64_t size = (int64_t)buffers[curIdx].count_of_pages * VORON_PAGE_SIZE;
        int64_t after = offset+size;
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

        if(rvn_pwritev(handle_ptr->file_fd, iovs, used, offset) <= 0)
        {
            *detailed_error_code = errno;
            return FAIL_WRITE_FILE;
        }
    }
    return SUCCESS;
}

EXPORT
rvn_writer rvn_get_writer(void* handle)
{
    struct handle *handle_ptr = handle;

    if(handle_ptr->write_address)
        return rvn_write_mmap;
    if(handle_ptr->global_state->ring.ring_fd != -1)
        return rvn_write_io_ring;

    rvn_write_mode mode = _get_writer_mode();
    switch (mode)
    {
        case rvn_write_mode_vectored_file_io:
            return rvn_write_vectored_file_io;
        case rvn_write_mode_file_io:
            return rvn_write_file_io;
        case rvn_write_mode_io_ring:
           return rvn_write_invalid_setup;
        case rvn_write_mode_mmap:
            return rvn_write_invalid_setup;
        default:
            return rvn_write_vectored_file_io;
    }
}

#endif