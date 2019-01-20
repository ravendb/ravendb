#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <assert.h>
#include <time.h>

#include "rvn.h"
#include "status_codes.h"
#include "internal_posix.h"

PRIVATE int64_t
_nearest_size_to_page_size(int64_t orig_size, int64_t sys_page_size)
{
    int64_t mod = orig_size % sys_page_size;
    if (mod == 0)
    {
        return orig_size;
    }
    return ((orig_size / sys_page_size) + 1) * sys_page_size;
}

PRIVATE int64_t
_pwrite(int32_t fd, void *buffer, uint64_t count, uint64_t offset, int32_t *detailed_error_code)
{
    uint64_t actually_written = 0;
    int64_t cifs_retries = 3;
    do
    {
        int64_t result = pwrite(fd, buffer, count - actually_written, offset + actually_written);
        if (result < 0) /* we assume zero cannot be returned at any case as defined in POSIX */
        {
            if (errno == EINVAL && _sync_directory_allowed(fd) == SYNC_DIR_NOT_ALLOWED && --cifs_retries > 0)
            {
                /* cifs/nfs mount can sometimes fail on EINVAL after file creation
                lets give it few retries with short pauses between them - RavenDB-11954 */
                struct timespec ts;
                ts.tv_sec = 0;
                ts.tv_nsec = 100000000L * cifs_retries; /* 100mSec * retries..*/
                nanosleep(&ts, NULL);
                continue; /* retry cifs */
            }
            *detailed_error_code = errno;
            if (cifs_retries != 3)
                return FAIL_PWRITE_WITH_RETRIES;
            return FAIL_PWRITE;
        }
        actually_written += result;
    } while (actually_written < (int64_t)count);

    return SUCCESS;
}

PRIVATE int32_t
_allocate_file_space(int32_t fd, int64_t size, int32_t *detailed_error_code)
{
    int32_t result;
    int32_t retries;
    for (retries = 0; retries < 1024; retries++)
    {
        result = _rvn_fallocate(fd, 0, size);

        switch (result)
        {
        case EINVAL:
        case EFBIG: /* can occure on >4GB allocation on fs such as ntfs-3g, W95 FAT32, etc.*/
            /* fallocate is not supported, we'll use lseek instead */
            {
                char b = 0;
                int64_t rc = _pwrite(fd, &b, 1UL, (uint64_t)size - 1UL, detailed_error_code);
                if (rc != SUCCESS)
                    *detailed_error_code = errno;
                return rc;
            }
            break;
        case EINTR:
            *detailed_error_code = errno;
            continue; /* retry */

        case SUCCESS:
            return SUCCESS;

        default:
            *detailed_error_code = result;
            return FAIL_ALLOC_FILE;
        }
    }
    return result; /* return EINTR */
}

PRIVATE int32_t
_ensure_path_exists(const char* path)
{
    /* TODO: implement */
    return SUCCESS;   
}

PRIVATE int32_t 
_open_file_to_read(const char *file_name, void **handle, int32_t *detailed_error_code)
{
    int32_t fd = open(file_name, O_RDONLY, S_IRUSR);
    if (fd != -1)
    {
        *handle = (void*)(int64_t)fd;
        return SUCCESS;
    }

    *detailed_error_code = errno;
    return FAIL_OPEN_FILE;
}

PRIVATE int32_t
_read_file(void *handle, void *buffer, int64_t required_size, int64_t offset, int64_t *actual_size, int32_t *detailed_error_code)
{
    int32_t rc;
    int32_t fd = (int32_t)(int64_t)handle;
    int64_t remain_size = required_size;
    int64_t already_read;
    *actual_size = 0;
     while (remain_size > 0)
    {
        already_read = pread64(fd, buffer, remain_size, offset);
        if (already_read == -1)
        {
            rc = FAIL_READ_FILE;
            goto error_cleanup;
        }
        if (already_read == 0)
        {
            rc = FAIL_EOF;
            goto error_cleanup;
        }

        remain_size -= already_read;
        buffer += already_read;
        offset += already_read;
    }

    *actual_size = required_size;
    return SUCCESS;

error_cleanup:
    *detailed_error_code = errno;
    *actual_size = required_size - remain_size; 
    return rc;
}

PRIVATE int32_t
_resize_file(void *handle, int64_t size, int32_t *detailed_error_code)
{
    int32_t fd = (int32_t)(int64_t)handle;

    int32_t rc;
    struct stat st;
    if (fstat(fd, &st) == -1)
    {
        rc = FAIL_STAT_FILE;
        goto error_cleanup;
    }

    if(size > st.st_size)
    {
        int32_t rc = _allocate_file_space(fd, size, detailed_error_code);
        if(rc != SUCCESS)
            return rc;
    }
    else
    {
        if(ftruncate64(fd, size) == -1)
        {
            rc = FAIL_TRUNCATE_FILE;
            goto error_cleanup;
        }
    }

    return SUCCESS;

error_cleanup:
    *detailed_error_code = errno;
    return rc;
}
